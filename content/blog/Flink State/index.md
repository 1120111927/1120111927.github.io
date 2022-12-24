---
title: Flink原理之状态
date: "2022-12-22"
description: 对于流计算而言，事件持续不断地产生，如果每次计算都是相互独立的，不依赖于上下游的事件，则是无状态计算，如果计算需要依赖于之前或者后续的事件，则是有状态计算。状态（State）用来保存中间计算结果或者缓存数据，为有状态计算提供容错
tags: 状态后端、状态存储、状态重分布、状态过期
---

对于流计算而言，事件持续不断地产生，如果每次计算都是相互独立的，不依赖于上下游的事件，则是无状态计算，如果计算需要依赖于之前或者后续的事件，则是有状态计算。状态（State）用来保存中间计算结果或者缓存数据，为有状态计算提供容错。

按照数据结构的不同，Flink中定义了多种状态，应用于不同的场景：
+ `ValueState<T>`: 单值状态，可以通过`update()`方法更新状态值，通过`value()`方法获取状态值
+ `ListState<T>`: 列表状态，可以通过`add()`方法添加元素，通过`get()`方法返回一个`Iterable<T>`来遍历状态值
+ `MapState<K, V>`: Map状态，通过`put()`/`putAll()`方法添加键值对，通过`get()`方法获取元素
+ `ReducingState<T>`:
+ `AggregatingState<IN, OUT>`
+ `FoldingState<T, ACC>`

状态描述（StateDescriptor）用于描述状态信息（状态名称、类型信息、序列化/反序列化器、过期时间等），每一类State都有对应的StateDescriptor。运行时，在RichFunction和ProcessFunction中，通过RuntimeContext对象，使用StateDesctiptor从状态后端（StateBackend）中获取实际的状态实例。

状态按照是否有Key分为KeyedState和OperatorState两种。KeyedState在KeyedStream中使用，跟特定的Key绑定，即KeyedStream流上的每一个Key对应一个State对象，可以使用所有类型的状态，保存在StateBackend中。OperatorState跟特定算子的一个实例绑定，整个算子只对应一个状态，只支持ListState。

按照由Flink管理还是用户自行管理，状态分为原始状态（Raw State）和托管状态（Managed State）。原始状态是用户自定义的状态，Flink做快照时把整个状态当作一个整体，需要用户自己管理，使用byte数组读写状态内容。托管状态是由Flink框架管理的状态，其序列化和反序列化由FLink框架提供支持，用户无感知。

广播状态（BroadcastState）在广播状态模式中使用，必须是MapState。广播状态模式是指来自一个流的数据需要被广播到所有下游任务，在算子本地存储，在处理另一个流的时候依赖于广播的数据，广播状态模式需要使用广播函数进行处理^[广播函数提供了处理广播数据流和普通数据流的接口]。

状态管理需要考虑以下几点：
+ 状态数据的存储和访问：在任务内部，如何高效地保存状态数据和使用状态数据
+ 状态数据的备份和恢复：如何高效地将状态数据保存下来，避免状态备份降低集群地吞吐量，并且在Failover的时候恢复作业到失败前的状态
+ 状态数据的划分和动态扩容：如何对状态数据进行切分，在作业修改并行度导致任务数量改变的时候确保正确地恢复到任务
+ 状态数据的清理：如何及时清理过期的状态

## 状态数据的存储和访问

Flink中使用状态有两种典型场景：
+ 状态操作接口：使用状态对象本身存储、写入、更新数据，分为面向应用开发者的状态接口和内部状态接口（Flink框架使用）。面向开发者的状态接口只提供了对状态中数据的添加、更新、删除等基本的操作接口。内部状态接口除了对状态中数据的访问之外还提供了内部的运行时信息接口，如状态中数据的序列化器、命名空间、命名空间序列化器、命名空间合并的接口
+ 状态访问接口：从状态后端获取状态本身。Flink抽象了OperatorStateStore和KeyedStateStore两个状态访问接口，屏蔽状态后端。OperatorStateStore中使用内存以Map形式来保存数据，KeyedStateStore中使用RocksDBStateBackend或者HeapKeyedStateBackend来保存数据，获取/创建状态都交给了具体的状态后端处理

状态后端（StateBackend）用于存储状态，需要具备如下能力：
+ 在计算过程中提供访问状态的能力
+ 能够将状态持久化到外部存储，提供容错能力

Flink内置了3中StateBackend：
+ MemoryStateBackend（内存型状态后端）：纯内存，适用于验证、测试。运行时所需要的状态数据保存在TaskManager JVM堆上内存中，键值类型的状态、窗口算子的状态使用HashTable来保存数据、触发器等，执行检查点时，会把状态的快照数据保存到JobManager进程的内存中
+ FsStateBackend（文件型状态后端）：内存+文件，适用于长周期大规模的数据。运行时所需要的状态数据保存在TaskManager JVM堆上内存中，执行检查点时，会把状态的快照数据保存到配置的文件系统中（分布式文件系统或本地文件系统）
+ RocksDBStateBackend：RocksDB，适用于长周期大规模的数据。使用嵌入式的本地数据库RocksDB将流计算数据状态存储在本地磁盘中，执行检查点时，再将整个RocksDB中保存的状态数据全量或者增量持久化到配置的文件系统中，在JobManager内存中会存储少量的检查点数据。相比基于内存的状态后端，访问状态的成本高很多，可能导致数据流的吞吐量剧烈下降

MemoryStateBackend和FsStateBackend依赖于HeapKeyStateBackend，HeapKeyStateBackend使用StateTable存储数据，StateTable有NestedMapsStateTable和CopyOnWriteStateTable两个子类，NestedMapStateTabel使用两层嵌套的HashMap保存状态数据，支持同步快照。CopyOnWriteStateTable使用CopyOnWriteStateMap来保存数据，支持异步快照。

## 状态数据的备份和恢复

**全量持久化策略** 每次把全量的状态写入状态存储中，HeapKeyedStateBackend对应HeapSnapshotStrategy策略，RocksDBStateBackend对应RocksFullSnapshotStrategy策略。在执行持久化策略时，使用异步机制，每个算子独立启动1个独立的线程，将自身的状态写入分布式可靠存储中，在做持久化的过程中，状态可能会被持续修改，基于内存的状态后端使用CopyOnWriteStateTable来保证线程安全，RocksDBStateBackend则使用RocksDB的快照机制，使用快照来保证线程安全

**增量持久化策略** 每次持久化增量的状态，只有RocksDBStateBackend支持增量持久化。RocksDB是一个基于LSM-Tree的键值存储，新的数据保存在内存中，成为memtable，一旦memtable写满了，RocksDB就会将数据压缩并写入到磁盘，memtable的数据持久化到磁盘后就变成了不可变的sstable。RocksDB会在后台合并sstable并删除其中重复的数据，然后在RocksDB删除原来的sstable，替换成新合成的sstable，新的sstable包含了被删除的sstable中的信息，通过合并，历史sstable会合并成一个新的sstable，并删除这些历史sstable，可以减少检查点的历史文件，避免大量小文件的产生。因为sstable是不可变的，Flink对比前一个检查点创建和删除的RocksDB sstable文件就可以计算出状态有哪些改变。为了确保sstable是不可变的，Flink会在RocksDB上触发刷新操作，强制将memtable刷新到磁盘上，在Flink执行检查点时，会将新sstable持久化到存储中，同时保留引用。

## 状态数据的划分和动态扩容

OpeartorssState重分布：
+ ListState：并行度发生改变时，会将每个List都取出，然后把这些List合并到一个新的List，根据元素的个数均匀分配给新的Task
+ UnionListState：并行度发生改变时，会将原来的List合并，但不做划分，直接交给用户
+ BroadcastState：并行度发生改变时，直接把Broadcast数据分发到新的Task即可

KeyedState重分布：基于Key-Group，每个Key隶属于唯一的Key-Group，Key-Group分配给Task实例，每个Task至少有1个Key-Group，Key-Group数量等于最大并行度。KeyGroup分配算法：
```
假设KeyGroup的数量为numberOfKeyGroups
hash = hash(key)
KeyGroup = hash % numberOfKeyGroups   // key所属的KeyGroup是确定的
subtask = KeyGroup * parallelism / numberOfKeyGroups
```

## 状态数据的清理

DataStream作业中可以通过API对状态过期进行精细控制，对每一个状态可以设置清理的策略，`StateDescriptor#enableTimeToLive(stateTtlConfig)`，StateTtlConfig中可以设置的内容如下：
+ 过期时间：超过多长时间未访问，视为状态过期
+ 过期时间更新策略：创建和写时更新、读取和写时更新
+ 状态可见性：未清理可用，超期则不可用

Flink SQL是高层抽象，没有状态概念，可以通过Flink Table API和SQL的参数配置选项设置状态清理策略（`StreamQueryConfig#withIdleStateRetentionTime()`）。定时清理状态存在可能因为状态被清理而导致计算结果不完全准确的风险，过期时间一般为1.5天左右。

默认情况下，只有在明确读出过期值时才会删除过期值。做完整快照时清理后，在获取完整状态快照时激活清理，减少其大小，在当前实现下不清楚本地状态，但在从上一个快照恢复的情况下，不会包括已删除的过期状态，通过StateTtlConfig设置`cleanupFullSnapshot()`。另一种是通过增量触发器渐进清理状态，通过StateTtlConfig设置`cleanupIncrementally()`，当进行状态访问或者清理数据时，在回调函数中进行处理，当每次增量清理触发时，遍历状态后端中的状态，清理掉过期的。
