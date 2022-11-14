---
title: Spark存储原理
date: "2021-07-21"
description: 
tags: 存储级别, 读数据过程, 写数据过程, Shuffle Read, Shuffle Write, 数据缓存机制
---

Spark的存储模块负责管理计算过程中产生的各种数据，并将内存、磁盘、本地、远程各种场景下数据的读写功能进行封装，解耦了RDD与底层物理存储，并提供了RDD的持久化功能。

Spark的存储采取了主从模式，即Master/Slave模式，使用了RPC的消息通信方式。Master负责整个应用程序运行期间的数据块元数据的管理和维护，Slave一方面负责将本地数据块的状态信息上报给Master，另一方面接收从Master传过来的执行命令，如获取数据块状态、删除RDD、数据块等命令。每个Slave存在数据传输通道，根据需要在Slave之间进行远程数据的读取和写入。

Spark存储过程分为读数据和写数据两个过程，读数据分为本地读取和远程节点读取两种方式。

RDD包含多个Partition，每个Partition对应一个数据块（Block），每个Block拥有唯一的编号BlockId，Block编号规则为："rdd_" + rddId + "_" + splitIndex，splitIndex为该数据块对应Partition序列号。

```bob-svg
                           ,------------------------,
                           |Driver                  |
                           | .~~~~~~~~~~~~~~~~~~~~. |
                           | !BlockManagerMaster  ! |
  1. RegisterBlockManager  | ! ,----------------, ! |       4. RemoveBlock
               ,-----------+-->| BlockManager   +-+-+-----,    RemoveRdd
               | ,---------+-->| MasterEndPoint | ! |     |    GetBlockStatus
               | |         | ! `----------------` ! |     |
               | |         | `~~~~~~~ ^ ~~~~~~~~~~` |     |
               | |         `----------|-------------`     |
               | | 2.UpdateBlockInfo  | 3. GetLocations   |
               | |                    |    GetMemoryStatus|
               | |                    |    ...            |
               | |          ,---------+-------------------`     网络方式读取数据
               | |          |         |     ,-------------+---+--------------------------------,
        ,------+-+----------|---------+-----|---------,   |   |   ,----------------------------+----------------,
        | ,----+-+----------|---------+-----|-------, |   |   |   | ,--------------------------+--------------, |
        | | .~~+~+~~~~~~~~~~|~~~~~~~~~+~~~~~|~~~~~. | |   |   |   | | .~~~~~~~~~~~~~~~~~~~~~~~~+~~~~~~~~~~~~. | |
        | | ! BlockManager  v               v     ! | |   v   v   | | ! BlockManager           v            ! | |
        | | ! ,--------------,   ,--------------, ! | |    ....   | | ! ,--------------,   ,--------------, ! | |
        | | ! |BlockManager  |   |BlockTransfer | ! | |           | | ! |BlockManager  |   |BlockTransfer | ! | |
        | | ! |SlaveEndPoint |   |   Service    | ! | |           | | ! |SlaveEndPoint |   |   Service    | ! | |
        | | ! `------+-------`   `-------+------` ! | |           | | ! `------+-------`   `-------+------` ! | |
        | | !        +---------+---------+        ! | |           | | !        +---------+---------+        ! | |
        | | !                  v                  ! | |           | | !                  v                  ! | |
        | | !            ,------------,           ! | |           | | !            ,------------,           ! | |
        | | !            | BlockStore |           ! | |           | | !            | BlockStore |           ! | |
        | | !            `-----+------`           ! | |           | | !            `-----+------`           ! | |
        | | `~~~~~~~~~~~~~~~~~~|~~~~~~~~~~~~~~~~~~` | |           | | `~~~~~~~~~~~~~~~~~~|~~~~~~~~~~~~~~~~~~` | |
        | | Executor           |                    | |           | | Executor           |                    | |
        | `--------------------|--------------------` |           | `--------------------|--------------------` |
        |           +----------+-----------+          |           |           +----------+-----------+          |
        |           v                      v          |           |           v                      v          |
        |        ,------,              ,--------,     |           |        ,------,              ,--------,     |
        |        | Disk |              | Memory |     |           |        | Disk |              | Memory |     |
        |        `------`              `--------`     |           |        `------`              `--------`     |
        | Worker1                                     |           | Worker1                                     |
        `---------------------------------------------`           `---------------------------------------------`

```

1. 在应用程序启动时，SparkContext会创建Driver端的SparkEnv，在该SparkEnv中实例化BlockManager和BlockManagerMaster，在BlockManagerMaster内部创建消息通信的终端点BlockManagerMasterEndpoint。
  在Executor启动时也会创建其SparkEnv，在该SparkEnv中实例化BlockManager和BlockTransferService（负责数据传输服务）。在BlockManager初始化过程中，一方面会加入BlockManagerMasterEndPoint终端点的引用，另一方面会创建Executor消息通信的BlockManagerSlaveEndpoint终端点的引用，另一方面会创建Executor消息通信的BlockManagerSlaveEndpoint终端点，并把该终端点的引用注册到Driver中，这样Driver和Executor相互持有通信终端点的引用，可以在应用程序执行过程中进行消息通信。

2. 当写入、更新或删除数据完毕后，发送数据块的最新状态消息UpdateBlockInfo给BlockManagerMasterEndpoint终端点，由其更新数据块的元数据。该终端点的元数据存放在BlockManagerMasterEndpoint的3个HashMap（blockManagerInfo、blockManagerIdByExecutor、blockLocation）中。

   blockManagerInfo（new mutable.HashMap[BlockManagerId, BlockManagerInfo]）中存放了BlockManagerId到BlockManagerInfo的映射，BlockManagerInfo包含了Executor内存使用情况、数据块的使用情况、已被缓存的数据块和Executor终端点的引用，通过该引用可以向该Executor发送消息；blockManagerIdByExecutor（new mutable.HashMap[String, BlockManagerId]）中存放了ExecutorId到BlockManagerId的映射。blockLocation（new JHashMap[BlockId, mutable.HashSet[BlockManagerId]]）中存放了BlockId到BlockManagerId列表的映射。

   在更新数据块的元数据时，更新blockManagerInfo和blockLocations两个列表。
   + 在处理blockManagerInfo时，传入的blockManagerId、blockId和storageLevel等参数，通过这些参数判断数据的操作是插入、更新还是删除操作。当插入或删除数据块时，会增加或更新BlockManagerInfo该数据块信息。如果是删除数据块，则会在BlockManagerInfo移除该数据块信息，这些操作结束后还会对该Executor的内存使用信息进行更新
   + 在处理blockLocations时，根据blockId判断blockLocations中是否包含该数据块。如果包含该数据块，则根据数据块的操作，当进行数据更新时，更新该数据块所在的BlockManagerId信息，当进行数据删除时，则移除该BlockManagerId信息，在删除的过程中判断数据块对应的Executor是否为空，如果为空表示在集群中删除了该数据块，则在blockLocations删除该数据块信息。如果不包含该数据块，则进行的是数据添加操作，把该数据块对应的信息加入到blockLocations列表中

3. 应用程序数据存储后，在获取远程节点数据、获取RDD执行的首选位置等操作时需要根据数据块的编号查询数据块所处的位置，此时发送GetLocations或GetLocationsMultipleBlockIds等消息给BlockManagerMasterEndpoint终端点，通过对元数据的查询获取数据块的位置信息。

   BlockManagerMasterEndpoint中的getLocations方法用于获取blockLocations中键值为数据块编号BlockId的所有BlockManagerId序列

   ```scala
   private def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
       // 根据blockId判断是否包含该数据块，如果包含，则返回其所对应的BlockManagerId序列
       if (blockLocations.containsKey(blockId)) blockLocations.get(blockId).toSeq
       else Seq.empty
   }
   ```

4. Spark删除RDD、数据块和广播变量的方式。当数据需要删除时，提交删除消息给BlockManagerMasterEndpoint终端点，在该终端点发起删除操作，删除操作一方面需要删除Driver端元数据信息，另一方面需要发送消息通知Executor，删除对应的物理数据。
   以RDD的unpersistRDD方法描述其删除过程：

   首先，在SparkContext中调用unpersistRDD方法，在该方法中发送removeRdd消息给BlockManagerMasterEndpoint终端点；然后，该终端点接收到消息时，从blockLocations列表中找出该RDD对应的数据存在BlockManagerId列表，查询完毕后，更新blockLocations和blockManagerInfo两个数据块元数据列表；最后，把获取的BlockManagerId列表，发送消息给所在BlockManagerSlaveEndpoint终端点，通知其删除该Executor上的RDD，删除时调用BlockManager的removeRdd方法，删除在Executor上RDD所对应的数据块。

   BlockManagerMasterEndpoint终端点的removeRdd代码如下：

   ```scala
   private def removeRdd(rddId: Int): Future[Seq[Int]] = {
       // 在blockLocations和blockManagerInfo中删除该RDD的数据元信息
       // 首先，根据RDD编号获取该RDD存储的数据块信息
       val blocks = blockLocations.keys.flatMap(_.asRDDId).filter(_.rddId == rddId)
       blocks.foreach { blockId =>
         // 然后，根据数据块的信息找出这些数据块所在BlockManagerId列表，遍历这些列表并删除BlockManager包含该数据块的元数据，同时删除blockLocations对应该数据块的元数据
         val bms: mutable.HashSet[BlockManagerId] = blockLocations.get(blockId)
         bms.foreach(bm => blockManagerInfo.get(bm).foreach(_.removeBlock(blockId)))
         blockLocations.remove(blockId)
       }
       // 最后，发送RemoveRdd消息给Executor，通知其删除RDD
       val removeMsg = RemoveRdd(rddId)
       Future.sequence(blockManagerInfo.values.map { bm => bm.slaveEndpoint.ask[Int](removeMsg)}.toSeq)
   }
   ```

BlockManagerMasterEndpoint和BlockManagerSlaveEndpoint之间还进行数据的状态信息、获取BlockManager副本等信息的交互，可以查看这两个类的receiveAndReply方法中的处理消息

Spark存储模块类图：

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml

hide circle

class BlockManager {
    initialize()
    doGetLocal()
    putBlockData()
}

class BlockManagerMaster {
    getLocations()
    removeRdd()
}

class BlockManagerMasterEndpoint {
    receiveAndReply()
    getLocations()
}

class BlockManagerInfo {
    _blocks: JHashMap
    _cachedBlocks: HashSet
}

class BlockManagerSlaveEndpoint {
    receiveAndReply()
    doAsync()
}

together {
    class MapOutputTracker
    class ShuffleManager
    class BlockTransferService
}

class DiskBlockManager {
    getFile()
}

class DiskStore {
    putIterator()
    getBytes()
}

class MemoryStore {
    putIterator()
    getBytes()
}

BlockManager .u.> BlockManagerMaster
BlockManager .u.> BlockManagerSlaveEndpoint
BlockManager .r.> MapOutputTracker
BlockManager .r.> ShuffleManager
BlockManager .r.> BlockTransferService
BlockManager .d.> DiskBlockManager
BlockManager .d.> DiskStore
BlockManager .d.> MemoryStore

BlockManagerMaster .r.> BlockManagerMasterEndpoint
BlockManagerMasterEndpoint .r.> BlockManagerInfo
BlockManagerInfo .r.> BlockManagerSlaveEndpoint

DiskStore .l.> DiskBlockManager

BlockManagerSlaveEndpoint .[hidden].> MapOutputTracker
MapOutputTracker .[hidden].> ShuffleManager
ShuffleManager .[hidden].> BlockTransferService
DiskStore .[hidden]> MemoryStore

@enduml
```

在整个模块中BlockManager是其核心，它不仅提供存储模块处理各种存储方式的读写方法，而且为Shuffle模块提供数据处理等操作接口。BlockManager存在于Driver端和每个Executor中，在Driver端的BlockManager中保存了数据的元数据信息，而在Executor的BlockManager根据接收到的消息进行操作：

+ 当Executor的BlockManager接收到读取数据的消息时，根据数据块所在的节点是否本地使用BlockManager不同的方法进行处理。如果在本地，则直接调用MemoryStore和DiskStore中的方法getValues/getBytes进行读取；如果是在远程，则调用BlockTransferService服务获取远程节点上的数据
+ 当Executor的BlockManager接收到写入数据的消息时，如果不需要创建副本，则调用BlockStore的接口方法进行处理，根据数据写入的存储类型，决定调用对应的写入方法

## 存储级别

Spark存储介质包括内存^[分为堆内内存和堆外内存]和磁盘，RDD的数据集不仅可以存储在内存中，还可以使用`persist()`或`cache()` ^[cache方法只是persist参数为MEMORY_ONLY的情况]方法显式地将RDD的数据集缓存到内存或者磁盘中。

`persist()`方法实现：

```scala
private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // 如果RDD指定了非NONE的存储级别，该存储级别将不能修改
    if (storageLevel != StorageLevel.None && newLevel != storageLevel && !allowOverride) {
        throw new UnsupportedOperationException("Cannot change storage level of an RDD after it was already assigned a level")
    }

    // 当RDD原来的存储级别为NONE时，可以对RDD进行持久化处理，在处理前需要先清除SparkContext中原来RDD相关存储元数据，然后加入该RDD持久化信息
    if (storageLevel == StorageLevel.NONE) {
        sc.cleaner.foreach(_.registerRDDForCleanup(this))
        sc.persistRDD(this)
    }

    // 当RDD原来的存储级别为NONE时，把RDD存储级别修改为传入新值
    storageLevel = newLevel
    this
}
```

当RDD第一次被计算时，persist方法会根据参数StorageLevel的设置采取特定的缓存策略，当RDD原本存储级别为NONE或者新传递进来的存储级别值与原来的存储级别相等时才进行操作，由于persist操作是控制操作的一种，只是改变了原RDD的元数据信息，并没有进行数据的存储操作，真正进行是在RDD的iterator方法中。

StorageLevel类中，根据useDisk、useMemory、useOffHeap、deserialized、replication 5个参数的组合，Spark提供了12中存储级别的缓存策略，可以将RDD持久化到内存、磁盘和外部存储系统，或者是以序列化的方式持久化到内存中，甚至在集群的不同节点之间存储多份副本。

```scala
class StorageLevel private(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useOffHeap: Boolean,
    private var _deserialized: Boolean,
    private var _replication: Int = 1
)

object StorageLevel {
    val NONE = new StorageLevel(false, false, false)
    val DISK_ONLY = new StorageLevel(true, false, false, false)
    val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
    val MEMORY_ONLY = new StorageLevel(false, true, false, true)
    val MEMORY_ONLY_2 = new StorageLevel(false, true, flase, true, 2)
    val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
    val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
    val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
    val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
    val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
    val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
    val OFF_HEAP = new StorageLevel(false, false, true, false)
}
```

|存储级别|描述|
|---|---|
|NONE|不进行数据存储|
|MEMORY_ONLY|将RDD作为反序列化的对象存储在JVM中，如果RDD不能被内存装下，一些分区将不会被缓存，并且在需要的时候重新计算，是默认级别|
|MEMORY_AND_DISK|将RDD作为反序列化的对象存储在JVM中。如果RDD不能被内存装下，超出的分区将保存在硬盘上，并且在需要时被读取|
|MEMORY_ONLY_SER|将RDD作为序列化的对象进行存储（每一分区占用一个字节数组）|
|MEMORY_AND_DISK_SER|与MEMORY_ONLY_SER相似，但是把超出内存的分区存储在硬盘上，而不是在每次需要的时候重新计算|
|DISK_ONLY|只将RDD分区存储在硬盘上|
|XXX_2|与上述存储级别一样，但是将每一个分区都复制到两个集群节点上|
|OFF_HEAP|将RDD存储到分布式内存文件系统中|

## 存储逻辑

存储逻辑位于RDD的iterator方法中。先根据BlockId判断是否已经按照指定的存储级别进行存储，如果存在该Block，则从本地或远程节点读取数据；如果不存在，则调用RDD的计算方法得出结果，并把结果按照指定的存储级别进行存储。

RDD的iterator方法如下：

```scala
final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
        // 如果存在存储级别，尝试读取内存的数据进行迭代计算
        getOrCompute(split, context)
    } else {
        // 如果不存在存储级别，则直接读取数据进行迭代计算或读取检查点结果进行迭代计算
        computeOrReadCheckpoint(split, context)
    }
}
```

getOrCompute方法是存储逻辑的核心：

```scala
private[spark] def getOrCompute(partition: Partition, context: TaskContext): Iterator[T] = {
    // 通过RDD的编号和Partition序号获取BlockId
    val blockId = RDDBlockId(id, partition.index)
    var readCachedBlock = true

    // 由于该方法由Executor调用，可使用SparkEnv代替sc.env
    // 根据BlockId先读取数据，然后再更新数据，这里是读写数据的入口点
    SparkEnv.get.blockManager.getOrElseUpdate(blockId, storageLevel, elementClassTag, () => {
        // 如果Block不再内存，则会尝试读取检查点结果进行迭代计算
        readCachedBlock = false
        computeOrReadCheckpoint(partition, context)
    }) match {
        // 对getOrElseUpdate返回结果进行处理，该结果表示处理成功，记录结果度量信息
        case Left(blockResult) =>
            if (readCachedBlock) {
                val existingMetrics = context.taskMetrics.registerInputMetrics(blockResult.readMethod)
                existingMetrics.incBytesReadInternal(blockResult.bytes)
                new InterruptibleIterator[T](context, blockResult.data.asInstanceOf[Iterator[T]]) {
                    override def next(): T = {
                        existingMetrics.incRecordsReadInternal(1)
                        delegate.next()
                    }
                }
            } else {
                new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])
            }
        // 对getOrElseUpdate返回结果进行处理，该结果表示保存失败，如数据过大无法放到内存中，并且也无法保存到磁盘中，把该结果返回给调用者，由其决定如何处理
        case Right(iter) =>
            new InterruptibleIterator(context, iter.asInstanceOf[Iterator[T]])
    }
}
```

getOrElseUpdate方法是读写数据的入口点：

```scala
def getOrElseUpdate[T](blockId: BlockId, level: StorageLevel, classTag: ClassTag[T], makeIterator: ()=>Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // 读取数据块入口，尝试从本地或者远程读取数据
    get(blockId) match {
        case Some(block) => return Left(block)
        case _ =>
    }

    // 写数据入口
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
        case None =>
            val blockResult = getLocalValues(blockId).getOrElse {
                releaseLock(blockId)
                throw new SparkException(s"get() failed for block $blockId even though we held a lock")
            }
            releaseLock(blockId)
            Left(blockResult)
        case Some(iter) => Right(iter)
    }
}
```

## 读数据过程

BlockManager的get方法是读数据的入口点，在读取时分为本地读取和远程节点读取两个步骤。本地读取使用getLocalValues方法，在该方法中根据不同的存储级别直接调用不同存储实现的方法；而远程节点读取使用getRemoteValues方法，在getRemoteValues方法中调用了getRemoteBytes方法，在getRemoteBytes方法中调用远程数据传输服务类BlockTransferService的fetchBlockSync进行处理，使用Netty的fetchBlocks方法获取数据

```bob-svg
,----------------,   ,-----------------------------------------------,  ,-------------,
|RDD             |   |BlockManager                                   |  |MemoryStore  |
| ,--------,     |   | ,---------------,   ,---,                     |  | ,---------, |
| |iterator|     | +-+>|getOrElseUpdate|-->|get|                  +--+--+>|getValues| |
| `---+----`     | | | `---------------`   `-+-`                  |  |  | `---------` |
|     v          | | |         +-------------+----------+         |  |  | ,---------, |
| ,------------, | | |         v                        v         |--+--+>|getBytes | |
| |getOrCompute|-+-+ | ,---------------,        ,--------------,  |  |  | `---------` |
| `------------` |   | |getRemoteValues|        |getLocalValues|--+  |  `-------------`
`----------------`   | `-------+-------`        `--------------`  |  |  ,-------------,
                     |         v                                  |  |  |DiskStore    |
                     | ,---------------,        ,-------------,   |  |  | ,--------,  |
                     | |getRemoteBytes |        |getBlockData |   +--+--+>|getBytes|  |
                     | `-------+-------`        `-------------`      |  | `--------`  |
                     `---------+---------------------- ^ ------------`  `-------------`
                               v                       |                 
                   ,-----------------------------------+-------------,
                   | ,--------------------,            |              |
                   | |BlockTransferService|            +------------+ |
                   | |  .fetchBlockSync   |                         | |
                   | `---------+----------`                         | |
                   |   ,-------+--------,     ,-------------------, | |
                   |   |       v        |     | ,---------------, | | |
                   |   | ,-----------,  |  +--+>|TransportClient| | | |
                   |   | |fetchBlocks|  |  |  | | .sendRpc      | | | |
                   |   | `-----+-----`  |  |  | `-------+-------` | | |
                   |   |       v        |  |  |         v         | | |
                   |   | ,------------, |  |  | ,---------------, | | |
                   |   | |OneForOne   | |  |  | |receive        | | | |
                   |   | |BlockFetcher| |  |  | |  .~~~~~~~~~~. | | | |
                   |   | `------------` |  |  | |  !OpenBlocks! | | | |
                   |   |NettyB |lock    |  |  | |  `~~~~~~~~~~` | | | |
                   |   |Transfe|rService|  |  | `-------+-------` | | |
                   |   `-------+--------`  |  |         v         | | |
                   |           v           |  |   ,-----------,   | | |
                   |   ,----------------,  |  |   |OpenBlocks +---+-+ |
                   |   |OneForOneBlock  +--+  |   `-----------`   |   |
                   |   |Fetcher.start   |     |    NettyBlock     |   |
                   |   `----------------`     |    RpcServer      |   |
                   |                          `-------------------`   |
                   `--------------------------------------------------`
```

### 内存读取

在getLocalValues方法中，读取内存中的数据根据返回的是BlockResult类型还是数据流分别调用MemoryStore的getValues方法和getBytes两种方法:

```scala
// 使用内存存储级别，并且数据在内存中
if (level.useMemory && memoryStore.contains(blockId)) {
    val iter: Iterator[Any] = if (level.deserialized) {
        // 如果存储时未序列化，则直接读取内存中的数据
        memoryStore.getValues(blockId).get
    } else {
        // 序列化时需要进行反序列化
        serializerManager.dataDeserializeStream(blockId, memoryStore.getBytes(blockId).get.toInputStream())(info.classTag)
    }

    // 数据读取完毕后，返回数据及数据块大小、读取方法等信息
    val ci = CompletionIterator[Any, Iterator[Any]](iter, releaseLock(blockid))
    Some(new BlockResult(ci, DataReadMethod.Memory, info.size))
}
```

在MemoryStore的getValues和getBytes方法中，最终都是通过数据块编号获取内存中的数据，其代码为：

```scala
val entry = entries.synchronized { entries.get(blockId) }
```

Spark内存存储使用了一个大的LinkedHashMap来保存数据，其定义如下：

```scala
private val entries = new LinkedHashMap[BlockId, MemoryEntry[_]](32, 0.75f, true)
```

向对于HashMap，LinkedHashMap保存了记录的插入顺序，在遍历LinkedHashMap元素时，先得到的记录时先插入的，由于内存大小的限制和LinkedHashMap特性，先保存的数据会先被清除。

### 磁盘读取

**磁盘目录的组织形式**：spark.local.dir设置磁盘存储的一级目录，默认情况下设置1个一级目录，在每个一级目录下最多创建64个二级目录。一级目录命名为spark-UUID.randomUUID ^[UUID.randomUUID为16位的UUID]，二级目录以数字命名，范围是00～63，目录中文件的名字是数据块的名称blockId.name。二级子目录在启动时并没有创建，而是当进行数据操作时才创建。

磁盘读取在getLocalValues方法中，调用的是DiskStore的getBytes方法，读取磁盘中的数据后需要把这些数据缓存到内存中。

BlockManager#getLocalValues方法如下：

```scala
else if (level.useDisk && diskStore.contains(blockId)) {
    val iterToReturn: Iterator[Any] = {
        // 从磁盘中获取数据，由于保存到磁盘的数据是序列化的，读取得到的数据也是序列化
        val diskBytes = diskStore.getBytes(blockId)
        if (level.deserialized) {
            // 如果存储级别需要反序列化，则把读取的数据反序列化，然后存储到内存中
            val diskValues = serializerManager.dataDeserializeStream(blockId, diskBytes.toInputStream(dispose = true))(info.classTag)
            maybeCacheDiskValuesInMemory(info, blockId, level, diskValues)
        } else {
            // 如果存储级别不需要反序列化，则直接把这些序列化数据存储到内存中
            val stream = maybeCacheDiskBytesInMemory(info, blockId, level, diskBytes)
              .map {_.toInputStream(dispose = false)}
              .getOrElse { diskBytes.toInputStream(dispose = true)}
            // 返回的数据需进行反序列化处理
            serializerManager.dataDeserializeStream(blockId, stream)(info.classTag)
        }
    }
    // 数据读取完毕后，返回数据及数据块大小、读取方法等信息
    val ci = CompletionIterator[Any, Iterator[Any]](iterToReturn, releaseLock(blockId))
    Some(new BlockResult(ci, DataReadMethod.Disk, info.size))
}
```

DiskStore#getBytes方法中，调用DiskBlockManager的getFile方法获取数据块所在文件的句柄。文件名为数据块名称，文件所在的一级目录和二级目录索引值通过文件名的哈希值取模获取:

```scala
def getFile(filename: String): File = {
    // 根据文件名的哈希值获取一级目录和二级子目录索引值
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % localDirs.length
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // 先通过一级目录和二级子目录索引值获取该目录，然后判断该子目录是否存在
    val subDir = subDir(dirId).synchronized {
        val old = subDirs(dirId)(subDirId)
        if (old != null) {
            old
        } else {
            // 如果不存在该目录创建该目录，范围为00~63
            val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
            if (!newDir.exists() && !newDir.mkdir()) {
                throw new IOException(s"Failed to create local dir in $newDir.")
            }
            // 判断该文件是否存在，如果不存在，则创建
            subDirs(dirId)(subDirId) = newDir
            newDir
        }
    }

    // 通过文件的路径获取文件的句柄并返回
    new File(subDir, filename)
}
```

获取文件句柄后，读取整个文件内容，以RandomAccessFile的只读方式打开该文件。这种打开方式支持从偏移位置开始读取指定大小，如果该文件足够小则直接读取，而较大文件则指定通道的文件区域直接映射到内存中进行读取：

```scala
def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    // 获取数据块所在文件的句柄
    val file = diskManager.getFile(blockId.name)

    // 以只读的方式打开文件，并创建读取文件的通道
    val channel = new RandomAccessFile(file, "r").getChannel
    Utils.tryWithSafeFinally {
        // 对于小文件，直接读取
        if (file.length < minMemoryMapBytes) {
            val buf = ByteBuffer.allocate(file.length.toInt)
            channel.position(0)
            while (buf.remaining() != 0) {
                if (channel.read(buf) == -1) {
                    throw new IOException("Reached EOF before filling buffer\n" +
                      s"offset=0\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
                }
            }
            buf.flip()
            new ChunkedByteBuffer(buf)
        } else {
            // 将指定通道的文件区域直接映射到内存中
            new ChunkedByteBuffer(channel.map(MapMode.READ_ONLY, 0, file.length))
        }
    } {
        channel.close()
    }
}
```

### 远程数据读取

Spark提供了Netty远程读取方式。在Spark中主要由NettyBlockTransferService和NettyBlockRpcServer两个类处理Netty远程数据读取

+ NettyBlockTransferService：该类向Shuffle、存储模块提供了数据存取的接口，接收到数据存取的命令时，通过Netty的RPC架构发送消息给指定节点，请求进行数据存取操作
+ NettyBlockRpcServer：当Executor启动时，同时会启动RPC监听器，当监听到消息时把消息传递到该类进行处理，消息内容包括读取数据OpenBlocks和写入数据UploadBlock两种

使用Netty处理远程数据读取流程如下：

1. Spark远程读取数据入口为getRemoteValues，然后调用getRemoteBytes方法，在该方法中调用getLocations方法先向BlockManagerMasterEndpoint终端点发送GetLocations消息，请求数据块所在的位置信息。当Driver的终端点接收到请求消息时，根据数据块的编号获取该数据块所在的位置列表，根据是否是本地节点数据对位置列表进行排序

   BlockManager#getLocalValues方法：
 
   ```scala
   private def getLocations(blockId: BlockId): Seq[BlockManagerid] = {
       // 向BlockManagerMasterEndpoint终端点发送消息，获取数据块所在节点消息
       val locs = Random.shuffle(master.getLocations(blockId))
 
       // 从获取的节点信息中，优先读取本地节点数据
       val (preferredLocs, otherLocs) = locs.partition { loc => blockManagerId.host == loc.host }
       preferredLocs ++ otherLocs
   }
   ```

   获取数据块的位置列表后，在BlockManager.getRemoteBytes方法中调用BlockTransferService提供的fetchBlockSync方法进行读取远程数据：

   ```scala
   def getRemoteBytes(blockId: BlockId): Option[ChunkedByteBuffer] = {
       var runningFailureCount = 0
       var totalFailureCount = 0
 
       // 获取数据块位置列表
       val locations = getLocations(blockId)
       val maxFetchFailures = locations.size
       var locationIterator = locations.iterator
       while (locationIterator.hasNext) {
           val loc = locationIterator.next()
 
           // 通过BlockTransferService提供的fetchBlockSync方法获取远程数据
           val data = try {
               blockTransferService.fetchBlockSync(
                   loc.host, loc.port, loc.executorId, blockId.toString).nioByteBuffer()
               )
           } catch {...}
           // 获取到数据后，返回该数据块
           if (data != null) {
               return Some(new ChunkedByteBuffer(data))
           }
           logDebug(s"The value of block $blockId is null")
       }
       logDebug(s"Block $blockId not found")
       None
   }
   ```

2. 调用远程数据传输服务BlockTransferService的fetchBlockSync方法后，在该方法中继续调用fetchBlocks方法。该方法是一个抽象方法，实际上调用的是Netty远程数据服务NettyBlockTransferService类中的fetchBlocks方法。在fetchBlocks方法中，根据远程节点的地址和端口创建通信客户端TransportClient，通过该RPC客户端向指定节点发送读取数据消息

   ```scala
   override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String], listener: BlockFetchingListener): Unit = {
       try {
           val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
               override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
                   // 根据远程节点的节点和端口创建客户端
                   val client = clientFactory.createClient(host, port)
                   // 通过该客户端向指定节点发送获取数据消息
                   new OneForOneBlockFetcher(client, appId, execId, blockIds.toArray, listener).start()
               }
           }
       }
       //...
   }
   ```

   发送读取消息是在OneForOneBlockFetcher类中实现的，该类的构造函数中定义了该消息`this.openMessage = new OpenBlocks(appId, execId, blockIds)`，然后在该类的start方法中向RPC客户端发送消息：

   ```scala
   public void start() {
       // ...
       // 通过客户端发送读取数据块消息
       client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {
           @Override
           public void onSuccess(ByteBuffer response) {...}

           @Override
           public void onFialure(Throwable e) {...}
       })
   }
   ```

3. 当远程节点的RPC服务端接收到客户端发送消息时，在NettyBlockRpcServer类中对消息进行匹配。如果是请求读取消息时，则调用BlockManager的getBlockData方法读取该节点上的数据，读取的数据块封装为ManagedBuffer序列缓存在内存中，然后使用Netty提供的传输通道，把数据传递到请求节点上，完成远程传输任务

   ```scala
   override def receive(client: TransportClient, messageBytes: Array[Byte], responseContext: RpcResponseCallback): Unit = {
       val message = BlockTransferMessage.Decoder.fromByteArray(messageBytes)

       message match {
           case openBlocks: OpenBlocks =>
               // 调用blockManager的getBlockData读取该节点上的数据，读取的数据块封装为ManagedBuffer序列缓存在内存中
               val blocks: Seq[ManagedBuffer] = openBlocks.blockIds.map(BlockId.apply).map(blockManager.getBlockData)
               // 注册ManagedBuffer序列，利用Netty传输通道进行传输数据
               val streamId = streamManager.registerStream(appId, blocks.iterator.asJava)
               responseContext.onSuccess(new StreamHandle(streamId, blocks.size).toByteArray)
               ......
       }
   }
   ```

## 序列化

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members

package java.io {
    interface Serializable
    interface Externalizable
}

package org.apache.spark.serializer {
    abstract class Serializer
    class JavaSerializer
    class KryoSerializer
    class SparkSqlSerializer
}

Serializer <|-- JavaSerializer
Serializer <|-- KryoSerializer
KryoSerializer <|-- SparkSqlSerializer
Serializable <|-- Externalizable
Externalizable <|-- JavaSerializer

@enduml
```

Spark中内置了两个数据序列化类：JavaSerializer和KryoSerializer，默认使用JavaSerializer。JavaSerializer使用的是Java的ObjectOutputStream序列化框架，继承自java.io.Serializable，灵活但是性能不佳，且生成序列结果较大。KryoSerializer性能更佳、压缩效率更高，但是不支持所有的序列化对象，而且要求用户注册类。还可以通过继承Serializer类自定义新的序列化方法。

SparkEnv中根据spark.serializer配置初始序列化实例，然后把该实例作为参数初始化SerializerManager实例，而SerializerManager作为参数初始化BlockManager：

```scala
val serializer = instantiateClassFromConf[Serializer]{"spark.serializer", "org.apache.spark.serializer.JavaSerializer"}
val serializerManager = new SerializerManager(serializer, conf)
val closureSerializer = new JavaSerializer(conf)
```

可配的序列化的对象是Shuffle数据以及RDD缓存等场合，对于Spark任务的序列化是通过spark.closure.serializer来配置，目前只支持JavaSerializer

## 压缩

Spark内置提供了三种压缩方法，分别是：LZ4、LZF和Snappy，均继承自CompressionCodec，并实现了其压缩和解压缩两个方法。Snappy提供了更高的压缩速度，LZF提供了更高的压缩比，LZ4提供了压缩速度和压缩比俱佳的性能。

|配置项|默认值|描述|
|---|---|---|
|spark.io.compression.codec|LZ4|RDD缓存和Shuffle数据压缩所采用的算法^[]|
|spark.rdd.compress|false|RDD缓存过程中，数据在序列化之后是否进一步进行压缩再储存到内存或磁盘上，主要考虑磁盘的IO带宽|
|spark.broadcast.compress|true|是否压缩广播变量|
|spark.io.compression.snappy.blockSize|设置使用Snappy压缩算法的块大小|
|spark.io.compression.lz4.blockSize|设置使用LZ4压缩算法的块大小|

## 共享变量

通常情况下，当一个函数传递给远程集群节点上运行的Spark操作时，该函数中所有的变量都会在各节点中创建副本，在各节点中的变量相互隔离并由所在节点的函数进行调用，并且这些变量的更新都不会传递回Driver程序。Spark提供了两种类型的共享变量：广播变量和累加器，用于支持在任务间进行通用、可读写的共享变量。

广播变量能够把数据高效地传递到集群的节点内存中，在节点中所运行的作业和任务能够进行调用。Spark能够在每个调度阶段自动广播任务所需通用的数据，这些数据在广播时需进行序列化缓存，并在任务运行前进行反序列化。当多个调度阶段的任务需要相同的数据时，显式地创建广播变量才有用。

通过调用SparkContext.broadcast(v)创建一个广播变量，使用该变量的value方法进行访问。

```scala
val broadcastVar = sc.broadcast(v)
broadcastVar.value
```

累加器有效地支持并行计算，能够用于计数和求和。Spark原生支持数值类型的累加器，也支持通过继承AccumulatorParam类来创建自定义累加类型。AccumulatorParam接口提供了两个方法：zero方法为自定义类型设置0值和addInPlace方法将两个变量进行求和。累加器只能由Spark内部进行更新，并保证每个任务在累加器的更新操作仅执行一次，重启任务也不应该更新。累加器同样具有Spark懒加载的求值模型，即如果在RDD的操作中进行更新，则它们的值只在RDD进行行动操作时才进行更新。如果在创建累加器时指定了名称，可以在Spark的UI监控界面中进行查看。

通过调用SparkContext.accumulator(v)初始化累加器变量，在集群中的任务能够是用加法或者`+=`操作符进行累加操作，但不能在应用程序中读取这些值，只能由Driver程序通过读方法获取这些累加器的值。

## 数据缓存机制

数据缓存机制的主要目的是加速计算，在应用执行过程中，对需要重用的数据进行缓存，当应用再次访问这些数据时，可以直接从缓存中读取，避免再次计算。

缓存机制是一种空间换时间的方法，需要权衡计算代价和存储代价。如果数据满足以下3条，就可以进行缓存：

+ 重复使用的数据
+ 数据不宜过大：过大会占用大量存储空间，导致内存不足，降低数据计算时可使用的空间。缓存数据过大存放到磁盘中时，磁盘的I/O代价比较高，有时不如重新计算
+ 非重复缓存的数据：如果缓存了某个RDD，那么其通过一对一依赖的父RDD就不需要缓存了

Spark中，RDD、广播数据、任务计算结果数据、Dataset、DataFrame都可以被缓存。

包含数据缓存操作的应用执行流程生成规则：首先假设应用没有数据缓存，按照正常规则生成逻辑处理流程（RDD之间的数据依赖关系），然后从第2个作业开始，将缓存RDD之前的RDD都去掉，得到消减后的逻辑处理流程，最后，按照正常规则将逻辑处理流程转化为物理执行计划。

**缓存数据的写入** `rdd.cache()`[^cache()、persist()、unpersist()都只能针对用户可见的RDD进行操作]只是对RDD进行缓存标记的，不是立即执行的，实际在作业计算过程中进行缓存，当需要缓存的RDD被计算出来时，及时进行缓存，再进行下一步操作。在实现中，Spark在每个Executor进程中分配一个区域，以进行数据缓存，该区域由BlockManager来管理，memoryStore包含了一个LinkedHashMap，用来存储RDD的分区，该LinkedHashMap中的Key是blockId，即rddId+partitionId，Value是分区中的数据，LinkedHashMap基于双向链表实现。

**缓存数据的读取** 当某个RDD被缓存后，该RDD的分区成为CachedPartitions[^可以使用`RDD#toDebugString()`来查看被缓存RDD的CachedPartitions的存储位置及占用空间大小]，当作业需要读取被缓存RDD中的分区时，首先去本地的BlockManager中查找该分区是否被缓存，如果对应的CachedPartition在本地，直接通过memoryStore读取即可，如果在其他节点，则需要通过远程访问，即通过`getRemote()`读取，远程读取需要对数据进行序列化和反序列化，远程读取时是一条条记录读取，并及时处理。

**缓存数据的替换与回收** 在内存空间有限的情况下，Spark需要缓存替换与回收机制。缓存替换是指当需要缓存的RDD大于当前可利用的空间时，使用新的RDD替换旧的RDD（由系统自动完成，用户无感知）。Spark采用LRU替换算法（优先替换掉当前最长时间没有被使用过的RDD）和动态替换策略（当可用内存空间不足时，每次通过LRU替换一个或多个RDD，然后开始存储新的RDD，如果中途存放不下，就暂停，继续使用LRU替换一个或多个RDD，依此类推，直到存放完新的RDD[^如果替换掉所有旧的RDD都存不下新的RDD，那么需要分两种情况处理：如果新的RDD的存储级别里包含磁盘，那么可以将新的RDD存放到磁盘中；如果新的RDD的存储级别只是内存，那么就不存储该RDD]）。Spark直接利用了LinkedHashMap自带的LRU功能实现了缓存替换，LinkedHashMap使用双向链表实现，每当Spark插入或读取其中的RDD分区数据时，LinkedHashMap自动调整链表结构，将最近插入或者最近被读取的分区数据放在表头，这样链表尾部的分区数据就是最近最久未被访问的分区数据，替换时直接将链表尾部的分区数据删除。在进行缓存替换时，RDD的分区数据不能被该RDD的其他分区数据替换

用户可以通过`RDD#unpersist()`方法自己设置进行回收的RDD和回收的时间，`unpersist()`操作是立即生效的，参数`blocking`用于指定同步阻塞（程序需要等待unpersist结束后再进行下一步操作）或异步执行（边执行unpersist边进行下一步操作）。

与Hadoop MapReduce的缓存机制对比，Hadoop MapReduce的DistributedCache缓存机制不是用于存放作业运行的中间结果的，而是用于缓存作业运行所需的文件的，DistributedCache将缓存文件放在每个worker的本地磁盘上，而不是内存中。

Spark当前的缓存机制存在一些缺陷。缓存的RDD数据是只读的，不能修改，也不能根据RDD的生命周期进行自动缓存替换等。另外，当前的缓存机制只能用在每个Spark应用内部，即缓存数据只能在作业之间共享，应用之间不能共享缓存数据。

### 缓存级别

Spark从3个方面考虑了缓存级别（Storage Level）：

+ 存储位置：可以将数据缓存到内存和磁盘中，内存空间小但读写速度快，磁盘空间大但读写速度慢
+ 是否序列化存储：如果对数据进行序列化，则可以减少存储空间，方便网络传输，但是在计算时需要对数据进行反序列化，会增加计算时延
+ 是否将缓存数据进行备份。将缓存数据复制多份并分配到多个节点，可以应对节点失效带来的缓存数据丢失问题，但需要更多存储空间

缓存界别针对的是RDD中的全部分区，即对RDD中每个分区中的数据都进行缓存。对于MEMORY_ONLY级别，只使用内存进行缓存，如果某个分区在内存中存放不下，就不对该分区进行缓存，当后续作业中的任务计算需要这个分区中的数据时，需要重新计算得到该分区。
