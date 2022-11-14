---
title: Spark错误容忍机制
date: "2022-07-05"
description: 
tags: 重新计算机制，checkpoint机制
---

错误容忍机制就是在应用执行失败时能够自动恢复应用执行，而且执行结果与正常执行时得到的结果一致。

对于Spark等大数据处理框架，错误容忍需要考虑以下两个问题：作业执行失败问题[^作业执行失败的原因既包括硬件问题，如节点宕机、网络阻塞、磁盘损坏等，也包括软件问题，如内存资源分配不足、分区配置过少导致任务处理的数据规模太大、分区划分不当引起的数据倾斜，以及用户和系统bug等]和数据丢失问题[^由于软硬件故障，如节点宕机，会导致某些输入/输出或中间数据丢失]。Spark错误容忍机制的核心方法有以下两种：

1. 通过重新计算任务来解决软硬件环境异常造成的作业失败问题，当作业抛出异常不能继续执行时，重新启动计算任务，再次执行

2. 通过采用checkpoint机制，对一些重要的输入/输出、中间数据进行持久化，在一定程度上解决数据丢失问题，能够提高任务重新计算时的效率

## 重新计算机制

重新计算机制有效（即得到与之前一样的结果）的前提条件：任务重新执行时能够读取与上次一致的数据[^`rand()`操作必须指定种子][^虽然Shuffle Read的数据不能保证顺序性，但一般应用并不要求结果有序，或者只要求Key有序]，并且计算逻辑具有确定性和幂等性。

Spark采用lineage来统一对RDD的数据和计算依赖进行建模，使用回溯方法解决重新计算时对数据和计算追根溯源的问题（从哪里重新计算，以及计算什么的问题）。其核心思想是在每个RDD中记录其上游数据是什么（父RDD，即数据依赖关系），以及当前RDD是如何通过上游数据计算得到的（`compute()`方法）。在错误发生时，可以根据lineage追根溯源，找到计算当前RDD所需的数据和操作。如果lineage上存在缓存数据，那么从缓存数据处截断lineage，即可得到简化后的操作。不管当前RDD本身是缓存数据还是非缓存数据，都可以通过lineage回溯方法找到计算该缓存数据所需的操作和数据。

## checkpoint机制

checkpoint机制通过对重要数据持久化来减少重新计算的时间。需要被checkpoint的RDD满足的特征是，RDD的数据依赖关系比较复杂且重新计算代价较高，如关联的数据过多、计算链过长、被多次重复使用等。

Spark中提供了`SparkContext#setCheckpointDir(directory)`接口来设置checkpoint的存储路径，提供了`RDD#checkpoint()`来实现checkpoint。

**checkpoint时机及计算顺序** 设置RDD checkpoint后只标记某个RDD需要持久化，当前作业结束后会启动专门的作业去完成checkpoint，需要checkpoint的RDD会被计算两次，推荐将需要被checkpoint的数据先进行缓存，额外启动的任务只需要将缓存数据进行checkpoint即可。

**checkpoint数据读取** checkpoint数据存储在分布式文件系统（HDFS）上，读取方式与从分布式文件系统读取输入数据相似，都是启动task来读取的，并且每个task读取一个分区。checkpoint数据格式为序列化的RDD，需要进行反序列化重新恢复RDD中的记录；checkpoint时存放了RDD的分区信息，重新读取后不仅恢复了RDD数据，也可以恢复其分区方法信息，便于决定后续操作的数据依赖关系。

checkpoint的写入过程不仅对RDD进行持久化，而且会切断该RDD的lineage，将该RDD与持久化到磁盘上的Checkpointed RDD进行关联。在实现中，RDD需要经过Initialized -> CheckpointingInProgress -> Checkpointed这3个阶段才能真正被checkpoint：

1. Initialized：当应用使用`RDD#checkpoint()`设定某个RDD需要被checkpoint时，Spark为该RDD添加一个checkpointData属性，用来管理该RDD相关的checkpoint信息（如 checkpoint的路径，checkpoint状态）

2. CheckpointingInProgress：当前作业结束后，会调用该作业最后一个RDD（finalRDD）的doCheckpoint()方法，该方法根据finalRDD的computing chain回溯扫描，遇到需要被checkpoint的RDD就将其标记为CheckpointingInProgress（checkpointData的cpState设置为CheckpointingInProgress），之后，Spark会调用runjob()再次提交一个作业完成checkpoint

3. Checkpointed：再次提交的作业对RDD完成checkpoint后，Spark会建立一个newRDD，类型为ReliableCheckpointRDD，用来表示被checkpoint到磁盘上的RDD，newRDD其实就是被checkpoint的RDD，保留了分区信息，但是将血缘关系（lineage）截断，不再保留依赖的数据和计算（因为被checkpoint的RDD已被持久化到可靠的分布式文件系统，不需要再保留其是如何计算得到的）。Spark通过将newRDD赋值给被checkpoint RDD checkpointData的cpRDD来关联二者。最后将checkpointData的cpState设置为Checkpointed。

Spark提供了`RDD#localCheckpoint()`方法来降低job lineage的复杂程度而不是为了持久化，功能上等价于数据缓存+checkpoint切断lineage功能。

### checkpoint与数据缓存的区别

1. 目的不同。数据缓存的目的是加速计算，即加速后续运行的作业。而checkpoint的目的是在作业运行失败后能够快速恢复，即加速当前需要重新运行的作业
2. 存储性质和位置不同。数据缓存是为了读写速度快，主要使用内存。而checkpoint是为了能够可靠读写，主要使用分布式文件系统
3. 写入速度和规则不同。数据缓存速度较快，对作业的执行时间影响较小，可以在作业运行时进行缓存。而checkpoint写入速度慢，为了减少对当前作业的时延影响，会额外启动专门的作业进行持久化
4. 对lineage影响不同。缓存RDD后，其lineage不受影响，缓存后的RDD丢失还可以通过重新计算得到。而checkpoint RDD后，会切断该RDD的lineage，因为该RDD已经被可靠存储，不需要再保留该RDD是如何计算得到的
5. 应用场景不同。数据缓存适用于会被多次读取、占用空间不是非常大的RDD，而checkpoint适用于数据依赖关系比较复杂、重新计算代价较高的RDD，如关联的数据过多、计算链过长、被多次重复使用
