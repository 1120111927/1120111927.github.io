---
title: Spark内存管理
date: "2021-07-28"
description: 
tags: 内存管理, 静态内存管理, 统一内存管理
---

Spark应用内存消耗在微观上指的是task线程的内存消耗，在宏观上指的是Executor JVM的内存消耗。

内存消耗来源：
+ Shuffle数据（框架执行），指在Shuffle Write/Read过程中使用类似HashMap和Array的数据结构对数据进行聚合和排序。Shuffle Write阶段，首先对map task输出的Output records进行分区，在这个过程中只需要计算每个record的partitionId，内存消耗可以忽略不计；然后采用类似HashMap的数据结构对数据进行combine()聚合，这个过程中HashMap会占用大量内存空间；最后按照partitionId或者Key对record进行排序，这个过程中可能会使用数组保存record，也会消耗一定的内存空间。Shuffle Read阶段，首先分配一个缓冲区（buffer）暂存从不同map task获取的record，这个过程中buffer需要消耗一些内存；然后采用类似HashMap的数据结构对数据进行combine()进行聚合，这个过程中会占用大量内存空间；最后需要对Key进行排序，这个过程中可能会使用数组保存record，需要消耗一定的内存空间。Shuffle机制中内存消耗影响因素包括Shuffle方式（sortByKey()不需要聚合过程，而reduceByKey(func)需要）和中间数据大小（Shuffle Write/Read的数据量和用户自定义的聚合函数的空间复杂度影响中间结果的大小）。这些内存消耗难以在task运行前进行估计，Spark采用动态监测的方法，在Shuffle机制中动态监测HashMap等数据结构大小，动态调整数据结构长度，并在内存不足时将数据溢写（spill）到磁盘中
+ 用户代码运行，指用户采用的数据操作和其中的func（如map(func)、reduceByKey(func)等），这些操作的内存使用模式大致有两种：一是每读入一条数据，立即调用func进行处理并输出结果，产生的中间计算结果并不进行存储，内存消耗可以忽略不计；另一种是对中间计算结果进行一定程度的存储，会造成内存消耗。用户代码内存消耗影响因素包括数据操作的输入数据大小和func的空间复杂度。输入数据大小决定用户代码会产生多少中间计算结果，空间复杂度决定有多少中间计算结果会被保存在内存中。Spark在运行时可以获得数据操作的输入数据信息，但是中间计算结果占用内存大小由用户代码的空间复杂度决定，难以预先估计
+ 数据缓存，当前job在运行中产生的数据可能被后续job重用，Spark会将一些重要数据缓存到内存中，提升应用性能。缓存数据内存消耗影响因素包括缓存RDD的大小、缓存级别、是否序列化等。Spark无法体现预测缓存数据大小，只能在写入过程中动态监控当前缓存数据的大小。另外，缓存数据还存在替换和回收机制，因此缓存数据在运行过程中大小也是动态变化的

内存管理挑战与方法：
+ 内存消耗来源多种多样，难以统一管理。

  Spark将内存划分为3个区域，每个区域分别负责存储和管理一项内存消耗来源，统一内存管理模型通过硬界限+软界限的方法来限制每个区域的内存消耗，并通过内存共享平衡数据计算与缓存的内存消耗。硬界限是指Spark将内存分为固定大小的用户代码空间（User memory）和框架内存空间（Framework memory）。软界限指的是框架内存空间（Framework memory）由框架执行空间（Execution memory）和数据缓存空间（Storage momory）共享。框架执行空间或者数据缓存空间内存不足时可以向对方借用，如果还不够，则会采取溢写到磁盘上、缓存数据替换、丢弃等方法。
+ 内存消耗动态变化、难以预估，为内存分配和回收带来困难。

  Spark采用边监控边调整的方法，如通过监控Shuffle Write/Read过程中数据结构大小来观察是否达到框架执行空间界限、监控缓存数据大小观察是否达到数据缓存空间界限，如果达到界限，则进行数据结构扩容、空间借用或者将缓存数据移出内存。
+ task（ShuffleMapTask或ResultTask）之间共享内存，导致内存竞争。在Spark中，多个task以线程方式运行在同一个Executor JVM中，task之间还存在内存共享和内存竞争

  在Spark的统一内存管理模型中，框架执行空间和数据缓存空间都是由并行运行的task共享的，为了平衡task间的内存消耗，Spark采取均分的方法限制每个task的最大使用空间，同时保证task的最小使用空间

Spark内存管理不仅要平衡各种来源的内存消耗，也要解决task的内存共享与竞争。

Spark为数据存储和计算执行提供了统一的内存管理接口：MemoryManager。MemoryManager包括静态内存管理（StaticMemoryManager）和统一内存管理（UnifiedMemoryManager）。

Spark内存模型可以限制框架使用的空间大小，但无法控制用户代码的内存消耗量。用户代码运行时的实际内存消耗量可能超过用户代码空间的界限，侵占框架使用的空间，此时如果框架也使用了大量内存空间，则可能造成内存溢出。

## 内存模式

Spark内存模式（MemoryMode）分为堆内（ON_HEAP）内存和堆外（OFF_HEAP）内存。

堆内内存管理建立在JVM的内存管理之上，堆内内存的申请和释放在本质上都是由JVM完成的，Spark对堆内内存的管理只是在逻辑上进行记录和规划^[Spark在创建对象时，由JVM分配内存空间并返回该对象的引用，Spark负责保存该引用并记录该对象占用的内存。同样的，释放内存也由JVM的垃圾回收机制完成]，不能精准控制堆内内存的申请和释放。Spark并不能准确记录实际可用的堆内内存，无法完全避免内存溢出^[非序列化对象占用的内存是通过周期性地采样近似估算而得的，有可能误差较大，此外，被Spark标记为释放的对象实例，实际上很有可能没有被JVM回收，导致实际可用的内存大小小于Spark记录的可用内存大小]。堆内内存依赖于JVM，无法完全摆脱GC带来的开销。

堆外内存管理使得Spark可以直接在集群节点的系统内存中分配空间，存储经过序列化的二进制数据^[在Spark 2.0之前，堆外内存依赖第三方分布式内存文件系统Tachyon/Alluxio，从2.0版本开始，Spark基于JDK自带的Unsafe API实现了堆外内存的管理]。堆外内存可以被精确地申请和释放，而且二进制字节流形式的数据占用的内存空间也可以被精确计算。

堆外内存比堆内内存更加容易管理，能够进一步优化内存的使用，减少了不必要的内存开销（例如频繁的垃圾回收），提升了处理性能。但是序列化^[序列化的过程是将对象转换为二进制字节流，在本质上可以理解为将非连续空间的链式存储转换为连续空间的块存储]数据在访问时需要进行反序列化，增加了转换过程的计算开销。

## 静态内存管理

```bob-svg


```

静态内存管理模型（StaticMemoryManager）将内存空间划分为数据缓存空间、框架执行空间、用户代码空间三个分区，每个分区负责存储三种内存消耗来源中的一种，并根据经验确定三者的空间比例，数据缓存空间和框架执行空间的内存占比和界限是固定的，彼此不能互相占用内存。静态内存管理模型不支持堆外内存，Spark1.6之前的版本采用了这个方法。

+ 数据缓存空间（Storage memory）：空间大小由参数spark.storage.memoryFraction设置，默认为0.6。用于存储RDD缓存数据、广播数据、task的一些计算结果。Unroll内存也属于数据缓存空间的一部分
+ 框架执行空间（Execution memory）：空间大小由参数spark.shuffle.memoryFraction设置，默认为0.2。用于存储Shuffle机制中的中间数据
+ 用户代码空间（User memory）：占用剩余空间（默认为0.2），用于存储用户代码的中间计算结果、Spark框架本身产生的内部对象、Executor JVM自身的一些内存对象

为了防止内存溢出，数据缓存空间和框架执行空间都会按照一定比例设置安全的使用空间。框架执行空间的安全比例通过参数spark.shuffle.safetyFraction设置，默认为0.8；数据缓存空间的安全比例通过参数spark.storage.safetyFraction设置，默认为0.9，Unroll内存处于安全空间中，比例通过参数spark.storage.unrollFraction设置，默认为0.2。

静态内存管理方式的局限性：

+ 静态的方式往往无法高效地适用于所有的应用类型
+ 静态内存管理方式一般都需要调整相关参数
+ 静态的方式也容易造成内存资源的浪费

## 统一内存管理

```bob-svg



```

统一内存管理模型（UnifiedMemoryManager）将内存空间划分为系统保留空间、框架内存空间和用户代码空间，框架内存空间又划分为数据缓存空间和框架执行空间。当框架执行空间不足时，会将shuffle数据spill到磁盘上；当数据缓存空间不足时，Spark会进行缓存替换、移除缓存数据等操作。为了限制每个task的内存使用，也会对每个task的内存使用进行限额。

+ 系统保留内存（Reserved Memory）：空间大小由参数spark.testing.ReservedMemory设置，默认为300MB，用于存储Spark框架产生的内部对象（如Spark Executor对象，TaskMemoryManager对象等Spark内部对象）
+ 框架内存空间（FrameWork Memory）：空间大小为spark.memory.fraction * (heap - Reserved Memory)，spark.memory.fraction默认为0.6。又被划分为框架执行空间（Execution Memory）和数据缓存空间（Storage Memory），两者共享这个空间，其中一方空间不足时可以动态向另一方借用，当数据缓存空间不足时，可以向框架执行空间借用借用其空闲空间，当框架执行空间需要更多空间时，Spark将部分缓存数据移除内存来归还空间；当框架执行空间不足时，可以向数据缓存空间借用空间，但至少要保证数据缓存空间具有spark.memory.storageFraction * Framework momory大小的空间，spark.memory.storageFraction默认为0.5。在框架执行时借走的空间不会归还给数据缓存空间，因为难以代码实现。另外，当框架执行空间不足时，会将Shuffle数据spill到磁盘上；当数据缓存空间不足时，Spark会进行缓存替换、移除缓存数据等操作。
+ 用户代码空间（Use Memory）：占用堆内剩余空间（堆空间-系统保留空间-框架内存空间，默认约为40%的堆内内存空间），用于存储用户代码生成的对象，如map()中用户自定义的数据结构

**堆外内存空间**为了减少垃圾回收开销，Spark的统一内存管理机制允许使用堆外内存，该空间不受JVM垃圾回收机制管理，在结束使用时需要手动释放空间。堆外内存主要存储序列化对象数据，只用于框架执行空间和数据缓存空间[^用户代码处理的是普通Java对象，所以堆外内存不能用于用户代码空间]。空间大小由spark.memory.offHeap.size设置，Spark按照spark.memory.storageFraction比例将堆外内存分为框架执行空间和数据缓存空间，堆外内存的管理方式与堆内内存一样，在运行应用时，Spark会根据应用的Shuffle方式及用户设定的数据缓存级别来决定使用堆内内存还是堆外内存，如SerializedShuffle方式可以利用堆外内存来进行Shuffle Write，rdd.persist(OFF_HEAP)可以将rdd存储到堆外内存。

统一内存管理最大的特点在于动态占用机制：

+ 设定基本的数据存储内存和计算执行内存区域，该设定确定了双方各自拥有的空间范围
+ 双方的空间都不足时，则存储到硬盘，若己方空间不足^[存储空间不足是指不足以放下一个完整的数据块]而对方空间空余时，可借用对方的空间
+ 计算执行内存的空间被对方占用后，可让对方将占用的部分转存到硬盘，然后“归还”借用的空间
+ 数据存储内存的空间被对方占用后，无法让对方归还

MemoryManager通过内存池机制管理内存，内存池（MemoryPool）是内存空间中一段大小可以调节的区域，分为数据存储内存池（StorageMemoryPool）和计算执行内存池（ExecutionMemoryPool）^[由于存在不同的内存模式，数据存储内存池包含onHeapStorageMemoryPool和offHeapStorageMemoryPool，计算执行内存池包含onHeapExecutionMemoryPool和offHeapExecutionMemoryPool]。

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml

hide circle

class MemoryPool {
    _poolSize: Long
    poolSize(): Long
    memoryFree(): Long
    incrementPoolSize(delta: Long): Unit
    decrementPoolSize(delta: Long): Unit
    memoryUsed: Long
}

@enduml
```

MemoryManager提供了三类内存管理接口，包括内存申请接口、内存释放接口和获取内存使用情况的接口。在Spark 2.1版本中，内存申请接口又包含存储内存申请（acquireStorageMemory）、展开内存申请（acquireUnrollMemory）^[Spark将RDD中的数据分区由不连续的存储空间组织为连续存储空间的过程称为展开（Unroll），展开内存即用于这个操作]和执行内存申请（acquireExecutionMemory）。

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml

hide circle

class MemoryManager {
    -- 内存申请 --
    acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean
    acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean
    acquireExecutionMemory(numBytes: Long, taskAttemptId: Long, memoryMode: MemoryMode): Long
    -- 内存释放 --
    releaseExecutionMemory(numBytes: Long, taskAttemptId: Long, memoryMode: MemoryMode): Unit
    releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long
    releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit
    releaseAllStorage(): Unit
    releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit
    -- 使用情况 --
    executionMemoryUsed(): Long
    storageMemoryUsed(): Long
    getExecutionMemoryUsageForTask(taskAttemptId: Long): Long
}

@enduml
```


### 数据存储内存管理

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml

hide circle

class StorageMemoryPool {
    poolName: Strig
    _memoryUsed: Long
    memoryUsed: Long
    _memoryStore: MemoryStore
    memoryStore: MemoryStore
    setMemoryStore(store: MemoryStore): Unit
    acquireMemory(blockId: BlockId, numBytes: Long): Boolean
    acquireMemory(blockId: BlockId, numBytesToAcquire: Long, numBytesToFree: Long): Boolean
    releaseMemory(size: Long): Unit
    releaseAllMemory(): Unit
    freeSpaceToShrinkPool(spaceToFree: Long): Long
}

@enduml
```

StorageMemoryPool是数据存储内存管理的入口，内部包含poolName、\_memoryUsed和\_memoryStore三个变量，poolName是内存池的名称（on-heap execution或off-heap execution）；_memoryUsed用于记录当前数据存储内存池的内存使用量；_memoryStore是MemoryStore类型。

acquireMemory方法用来申请内存，最终调用的是MemoryStore中的evictBlocksToFreeSpace方法，将内存中的数据块落盘，回收相应的内存空间。如果MemoryStore回收了数据块的内存空间，则同步机制会动态更新StorageMemoryPool中的内存使用量，相应的变量也会自动完成更新，不需要调用程序显式的修改。

freeSpaceToShrinkPool方法用来收缩数据存储可用内存并借给计算执行内存空间，最终调用的是MemoryStore中的evictBlocksToFreeSpace方法。freeSpaceToShrinkPool方法调用后不会更新当前StorageMemoryPool的大小，需要显式应用decrementPoolSize方法来处理。

```scala
def freeSpaceToShrinkPool(spaceToFree: Long): Long = lock.synchronized {
    val spaceFreedByReasingUnusedMemory = math.min(spaceToFree, memoryFree)
    val remainingSpaceToFree = spaceToFree - spaceFreeByReasingUnusedMemory
    if (remainingSpaceToFree > 0) {
        val spacefreedByEviction = memoryStore.evictBlocksToFreeSpace(None, remainingSpaceToFree, memoryMode)
        spaceFreeByReleasingUnusedMemory + spaceFreeByEviction
    } else {
        spaceFreedByReleasingUnusedMemory
    }
}
```

数据存储内存的主要使用者是Spark的存储模块。RDD在缓存到数据存储内存之前，每条数据的对象实例都处于JVM堆内内存的Other部分，即便同一个分区内的数据在内存空间中也是不连续的，具体分布由JVM管理，上层通过迭代器来访问。当RDD持久化到数据存储内存之后，分区对应转换为数据块，此时数据在数据存储内存空间（堆内或堆外）中将连续的存储。这种分区由不连续的存储空间转换为连续的存储空间的过程就是展开（Unroll）操作。数据块有序列化和非序列化两种存储格式。非序列化的数据块定义在DeserializedMemoryEntry类中，用一个数组存储所有的对象实例；序列化的数据块定义在SerializedMemoryEntry类中，用字节缓冲区（ByteBuffer）存储二进制数据。每个Executor的存储模块采用LinkedHashMap数据结构来管理堆内和堆外数据存储内存中的所有数据块对象实例，对该LinkedHashMap新增和删除间接记录了内存的申请和释放。

因为不能保证数据存储内存空间可以一次容纳Iterator中所有的数据，所以当前的计算任务在执行展开操作时需要向MemoryManager申请足够的空间来临时占位，空间不足则展开失败，空间足够时可以继续进行。对于序列化的分区，其所需的展开空间可以直接累加计算，一次申请；而非序列化的分区则要在遍历数据的过程中依次申请，即每读取一条记录，采样估算其所需的展开空间并进行申请，空间不足时可以中断，释放已占用的展开空间。如果最终展开成功，则当前分区所占用的展开空间被转换为正常的缓存RDD的数据存储内存空间。

当有新的数据块需要缓存但是剩余空间不足且无法动态占用时，就要对LinkedHashMap中的旧数据块进行淘汰（Eviction）。对于被淘汰的数据块，如果其存储级别中同时包含存储到磁盘的要求，则要对其进行落盘^[落盘的流程：如果其存储级别符合useDisk为true的条件，就根据其deserialized判断是否非序列化的形式，若是则对其进行序列化，最后将数据存储到磁盘，在存储模块中更新其信息]，否则直接删除该数据块。数据存储内存的淘汰规则如下：

+ 被淘汰的旧数据块要与新数据块的内存模式相同
+ 新旧数据块不能属于同一个RDD，避免循环淘汰
+ 旧数据块所属RDD不能处于被读状态，避免引发一致性问题
+ 遍历LinkedHashMap中的数据块，按照最近最少使用（LRU）的顺序淘汰，直到满足新数据块所需的空间

### 计算执行内存管理

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml

hide circle

class ExecutionMemoryPool {
    poolName: Strig
    memoryUsed: Long
    memoryForTask: HashMap[Long, Long]
    setMemoryStore(store: MemoryStore): Unit
    acquireMemory(numBytes: Long, taskAttemptId: Long, maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long), computeMaxPoolSize:() => Long = () => poolSize): Long
    releaseMemory(numBytes: Long, taskAttemptId: Long): Unit
    releaseAllMemoryForTask(taskAttemptId: Long): Long
    getMemoryUsageForTask(taskAttemptId: Long): Long
}

@enduml
```

ExecutionMemoryPool是计算执行内存管理的入口，内部包含poolName和memoryForTask两个变量，poolName是内存池的名称（on-heap execution或off-heap execution），memoryForTask实际上是一个HashMap结构，记录了每个Task的内存使用量。

acquireMemory方法用来申请内存。numBytes和taskAttemptId分别表示申请的内存大小（字节数目）和发出内存申请的Task，maybeGrowPool是一个回调函数，用来控制增加当前ExecutionMemoryPool的大小（从数据存储内存中借用内存），computeMaxPoolSize同样是一个回调函数，用来获取当前ExecutionMemoryPool的大小。

计算执行内存主要用于满足Shuffle、Join、Sort、Aggregation等计算过程中对内存的需求。Executor内运行的任务共享计算执行内存，假设当前Executor中正在执行的任务数目为n，那么每个任务可占用的计算执行内存大小的范围是[1/2n, 1/n]。每个任务启动时，要向MemoryManager申请最少1/2n的执行内存，如果不能满足要求，则该任务被阻塞，直到有其他任务释放了足够的执行内存，该任务才能被唤醒。在执行期间，Executor中活跃的任务数目是不断变化的，Spark采用wait和notifyAll机制同步状态并重新计算n的值。

Shuffle的Write和Read两个阶段对执行内存的使用：

+ Shuffle Write：若在map端选择普通的排序方式，则会采用ExternalSorter进行外排，在内存中存储数据时主要占用堆内执行空间。若在map端选择Tungsten的排序方式，则采用ShuffleExternalSorter直接对以序列化形式存储的数据进行排序，在内存中存储数据时可以占用堆外或堆内执行空间，取决于用户是否开启了堆外内存，以及堆外执行内存是否足够
+ Shuffle Read：在对reduce端的数据进行聚合时，要将数据交给Aggregator处理，在内存中存储数据时占用堆内执行空间。如果需要进行最终结果排序，则要再次将数据交给ExternalSorter处理，占用堆内执行空间

## 框架执行内存消耗与管理

**内存共享与竞争** 框架执行空间由多个task（ShuffleMapTask、ResultTask）共享，在运行过程中，Executor中活跃的task数目在[0, ExecutorCores]内变化，每个task可使用的内存空间大小被控制在[1/2N，1/N]*ExecutorMemory内，N是当前活跃的task数目。

Shuffle Write/Read内存消耗变化及消耗位置：

**Shuffle Write阶段内存消耗及管理**：根据Spark应用是否需要map端聚合，是否需要按Key进行排序，将Shuffle Write方式分为4种

+ 无map端聚合、无排序且partition个数不超过200的情况：采用基于buffer的BypassMergeSortShuffleWriter方式
+ 无map端聚合、无排序且partition个数大于200的情况：采用SerializedShuffleWriter方式
+ 无map端聚合但需要排序的情况：采用基于数组的SortShuffleWriter(KeyOrdering=true)方式
+ 有map端聚合的情况：采用基于HashMap的SortShuffleWriter(mapSideCombine=true)方式


## 参数配置

|参数|描述|
|---|---|
|spark.driver.memory|设置Driver堆内内存|
|spark.executor.memory|设置Driver堆外内存|
|spark.memory.offHeap.enabled|开启堆外内存|
|spark.memory.offHeap.size|设置堆外内存空间的大小|
|spark.storage.memoryFraction|设置数据存储内存比例，默认为0.6|
|spark.storage.safetyFraction|设置数据存储内存安全空间比例，默认为0.9|
|spark.storage.unrollFraction|设置Unroll内存比例，默认为0.2|
|spark.shuffle.momoryFraction|设置计算执行内存比例，默认为0.2|
|spark.shuffle.safetyFraction|设置计算执行内存比例，默认为0.8|
|spark.memory.fraction|设置数据存储与计算执行的公用，默认为0.6|
|spark.memory.storageFraction|设置数据存储内存空间的初始比例，默认为0.5|
