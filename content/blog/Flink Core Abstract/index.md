---
title: Flink原理之核心抽象
date: "2022-12-20"
description: Flink抽象了Flink API作为数据流和在数据流上的操作，抽象了Transformation作为API层到执行层转换的中间层，抽象了算子、函数、数据分区作为运行时业务逻辑的载体，抽象了数据IO屏蔽外部存储的差异
tags: StreamExecutionEnvironment、StreamElement、Transformation、Function、StreamOperator
---

Flink抽象了Flink API作为数据流和在数据流上的操作，抽象了Transformation作为API层到执行层转换的中间层，抽象了算子、函数、数据分区作为运行时业务逻辑的载体，抽象了数据IO屏蔽外部存储的差异。函数用于实现业务逻辑；Transformation是一个逻辑概念，记录上游数据来源，将用户处理逻辑组织成流水线；算子在运行时从上游获取数据、交给用户自定义函数执行并将执行结果交给下游，同时提供容错支持。

## 环境对象

环境对象提供了作业开发和启动执行的入口、配置信息的传递^[在作业提交、部署、运行时可以获取这些配置信息]等，包括执行环境（StreamExecutionEnvironment），运行时环境（Environment）和运行时上下文（RuntimeContext）3种环境对象。

### 执行环境

执行环境是Flink应用开发时的概念，表示流计算作业的执行环境，是Flink作业开发和启动执行的入口，包括LocalStreamEnvironment、RemoteStreamEnvironment、StreamContextEnvironment^[Cli命令行或单元测试执行环境]、StreamPlanEnvironment^[用于在Flink Web UI管理界面中可视化展现Job时生成执行计划StreamGraph]、ScalaShellStreamEnvironment^[Scala Shell执行环境，用于在命令行中交互式开发FLink作业]。

LocalStreamEnvironment：表示本地执行环境^[在单个JVM中使用多线程模拟Flink集群]，一般用作本地开发、调试，基本工作流程如下：
1. 执行Flink作业的Main函数生成StreamGraph，转化为JobGraph
2. 设置任务运行的配置信息
3. 根据配置信息启动对应的LocalFlinkMiniCluster
4. 根据配置信息和MiniCluster生成对应的MiniClusterClient
5. 通过MiniClusterClient提交JobGraph到MiniCluster

RemoteStreamEnvironment：表示远程Flink集群的执行环境，基本工作流程如下：
1. 执行Flink作业的Main函数生成StreamGraph，转化为JobGraph
2. 设置任务运行的配置信息
3. 提交JobGraph到远程Flink集群

### 运行时环境

运行时环境是Flink应用运行时作业级别的概念，定义了在运行时刻Task所需要的所有配置信息，包括在静态配置和调度器调度之后生成的动态配置信息，包括RuntimeEnvironment和SavepointEnvironment两个实现类。RuntimeEnvironment在Task开始执行时进行初始化，封装了Task运行相关信息（包括配置信息和运行时的各种服务）。SavepointEnvironment是Environment的最小化实现，用在State Processor API（状态处理接口）中，State Processor API用于操作应用程序状态，借助批执行模式的DataStream API读写、修改保存点和检查点的数据。

### 运行时上下文

运行时上下文是Flink应用运行时任务级别的概念，是用户自定义函数运行时的上下文，用户自定义函数的每个实例都有一个RuntimeContext对象（在RichFunction中通过`getRuntimeContext()`方法可以访问该对象），让用户自定义函数在运行时能够获取到作业级别的信息（如Task名称、执行配置信息、并行度信息、状态等）。不同的使用场景中有不同的RuntimeContext，具体如下：
+ StreamingRuntimeContext: 在流计算UDF中使用，用来访问作业信息、状态等
+ SavepointRuntimeContext：在State Processor API中使用
+ CepRuntimeContext：在CEP复杂事件处理中使用

## 数据流元素（StreamElement）

数据流元素（StreamElement）有数据记录（StreamRecord）、延迟标记（LatencyMarker）、水印（Watermark）、流状态标记（StreamStatus）4种，在执行层面上都被序列化成二进制数据，形成混合的数据流，在算子中将混合数据流中的数据流元素反序列化出来，根据其类型分别进行处理。

+ StreamRecord：表示数据流中的一条记录（或一个事件），包括数据值本身和事件时间戳（可选）
+ LatencyMarker：用来近似评估数据从读取到写出之间的延迟^[不包含计算的延迟]，在Source中创建，并向下游发送，绕过业务处理逻辑，在Sink节点中使用LatencyMarker估计数据在整个DAG图中流转花费的时间，用来近似评估总体上的处理延迟，包括在数据源算子中周期性创建的时间戳、算子编号、数据源算子所在的Task编号
+ Watermark：是一个事件戳，用来告诉算子所有时间早于等于Watermark的记录（或事件）都已到达，不会再有比Watermark更早的记录，算子可以根据Watermark出发窗口的计算、清理资源等
+ StreamStatus：在数据源算子中生成，向下游沿着Dataflow传播，用来通知Task是否会继续接收到上游的记录或者Watermark，可以表示两种状态空闲状态（IDLE）和活动状态（ACTIVE）

## 数据转换（Transformation）

```plantuml
@startuml

!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
skinparam ArrowThickness 1
left to right direction

class Transformation
class PhysicalTransformation
class SourceTransformation
class SinkTransformation
class OneInputTransformation
class TwoInputTransformation
class SelectTransformation
class UnionTransformation
class PartitionTransformation
class SplitTransformation
class CoFeedbackTransformation
class FeedbackTransformation
class SideOutputTransformation

PhysicalTransformation -u-|> Transformation
SourceTransformation -u-|> PhysicalTransformation
SinkTransformation -u-|> PhysicalTransformation
OneInputTransformation -u-|> PhysicalTransformation
TwoInputTransformation -u-|> PhysicalTransformation

SplitTransformation -d-|> Transformation
SelectTransformation -d-|> Transformation
UnionTransformation -d-|> Transformation
PartitionTransformation -d-|> Transformation
CoFeedbackTransformation -d-|> Transformation
FeedbackTransformation -d-|> Transformation
SideOutputTransformation -d-|> Transformation

@enduml
```

Transformation是衔接DataStream API和Flink内核的逻辑结构，调用DataStream API的数据处理流水线最终会转换为Transformation流水线。

Transformation有两大类：物理transformation和虚拟Transformation。在运行时，DataStream API的调用都会被转换成Transformation，然后物理Transformation被转换为实际运行的算子，而虚拟Transformation则不会转换为具体的算子。另外，在Transformation类体系中所有的物理Transformation都继承自PhysicalTransformation。

Transformation包含了Flink运行时的一些关键参数：
+ name: transformation名称，用于可视化
+ uid: 由用户指定，主要用于在作业重启时再次分配跟之前相同的uid，可以持久保存状态
+ bufferTimeout: buffer超时时间
+ parallelism: 并行度
+ id: 基于静态累加器生成
+ outputType: 输出类型，用来序列化数据
+ slotSharingGroup: Slot共享组

物理Transformation一共有4种：
+ SourceTransformation: 用于从数据源读取数据，是Flink作业的起点，只有下游Transformation，没有上游Transformation。一个作业可以有多个SourceTransformation，从多个数据源读取数据
+ SinkTransformation: 用于将数据写到外部存储，是Flink作业的终点，只有上游Transformation，没有下游Transformation。一个作业可以有多个SinkTransformation，将数据写入不同的外部存储
+ OneInputTransformation: 单流输入Transformation，只接收一个输入流
+ TwoInputTransformation: 双流输入的Transformation，接收两种流（第1输入和第2输入）

虚拟Transformation：
+ SideOutputTransformation: 用于旁路输出，表示上游Transformation的一个分流，一个Transformation可以有多个分流，每个分流通过OUtputTag进行标识
+ SplitTransformation/SelectTransformation: SplitTransformation按条件将一个流拆分成多个流（只是逻辑上的拆分，只影响上游的流如何跟下游的流连接），SelectTransformation用来在下游选择数据流
+ PartitionTransformation: 用于改变输入元素的分区，需要接收一个StreamPartitioner对象进行分区
+ UnionTransformation: 用于将合并多个元素数据结构安全相同的输入流
+ FeedbackTransformation/CoFeedbackTransformation: 把符合条件的数据重新发回上游Transformation处理

## 算子（StreamOperator）

Flink作业运行时由Task组成一个Dataflow，每个Task中包含一个或者多个算子，1个算子就是1个计算步骤，具体的计算由算子中包装的Function来执行。

所有算子都包含了生命周期管理、状态与容错管理、数据处理3个方面的关键行为，算子中还定义了OperatorChain策略用于算子融合优化。

StreamTask作为算子的容器，负责管理算子的声明周期，算子核心生命周期阶段如下：
1. setup: 初始化环境、时间服务、注册监控等
2. open: 由各个具体的算子负责实现，包含了算子的初始化逻辑（如状态初始化等），执行该方法后才会执行Function进行数据的处理
3. close: 所有的数据处理完毕之后关闭算子，此时需要确保将所有的缓存数据向下游发送
4. dispose: 在算子生命周期的最后阶段执行，此时算子已经关闭，停止处理数据，进行资源的释放

算子负责状态管理，触发检查点时，保存状态快照，并且将快照异步保存到外部的分布式存储，当作业失败时负责从保存的快照中恢复状态。

算子对数据的处理，包括数据记录的处理（`processElement()`）、Watermark的处理（`processWatermark()`）、LatencyMarker的处理（`processLatencyMarker()`）。

异步算子的目的是解决与外部系统交互时网络延迟所导致的系统瓶颈问题。Async I/O的实现原理是连续地向数据库发送多个请求，哪个请求的回复先返回，就先处理哪个回复，从而使连续地请求之间不需要阻塞等待。对于后调用的请求先返回的情况，Flink在异步算子中提供了两种输出模式：
+ 顺序输出模式：先收到的数据元素先输出，后续数据元素的异步函数调用无论是否先完成，都需要等待。可以保证消息不乱序，但是可能增加延迟，降低算子的吞吐量。原理为数据进入算子，对每一条数据元素调用异步函数，并封装为ResultFuture放入到排队队列中（如果是Watermark，也会放入到队列中），输出时，严格按照队列中的顺序输出给下游
+ 无序输出模式：先处理完的数据元素先输出。延迟低、吞吐量高，但是不保证消息顺序。原理为数据进入算子，对每一条数据元素调用异步函数，并封装为ResultFuture放入到等待完成队列中（如果是Watermark，也会放入到队列中），当异步函数返回结果时，放入已完成队列，按照顺序输出给下游。无序输出模式并不是完全无序，仍然要保持Watermark不能超越其前面数据元素的原则。等待完成队列中将按照Watermark切分成组，组内可以无序输出，组之间必须严格保证顺序。

异步算子同时也支持对异步函数调用的超时处理，支持完整的容错特性。

## 函数（Function）

```plantuml
@startuml

!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
skinparam ArrowThickness 1

interface Function
interface RichFunction
interface AsyncFunction
interface SourceFunction
interface SinkFunction
interface ListCheckpointed

RichFunction -u-|> Function
AsyncFunction -u-|> Function
SourceFunction -u-|> Function
SinkFunction -u-|> Function

@enduml
```

按照输入和输出的不同特点分类，Flink中函数大概分为3类：
+ SourceFunction: 直接从外部数据存储读取数据，无上游
+ SinkFunction: 直接将数据写入外部存储，无下游
+ 一般Function: 用在作业的中间处理步骤中，有上游，也有下游。进一步可分为单流输入和双流输入两种，多流输入可以通过多个双流输入串联而成

**无状态函数** 用来做无状态计算（如MapFunction），一般都是直接继承接口或通过匿名类实现接口

**富函数（RichFunction）** 提供生命周期管理、访问RuntimeContext的能力，`open()`^[Function启动时执行初始化]和`close()`^[Function停止时执行清理，释放占用的资源]方法来管理Function生命周期，`getRuntimeContext()`和`setRuntimeContext()`方法来访问RuntimeContext，进而能够获取到执行时作业级别的参数信息，操作状态

**处理函数** 可以访问流应用程序所有非循环基本构建块，事件、状态和定时器。分为ProcessFunction、CoProcessFunction、KeyedProcessFunction、KeyedCoProcessFunction，Keyed类处理函数只能用在KeyedStream上，Co类处理函数是双流输入

**广播函数** 用于广播状态模式^[将一个数据流的内容广播到另一个流中]，分为BroadcastProcessFunction和KeyedBroadcastProcessFunction，有两个数据元素处理方法，`processElement()`负责处理一般的数据流，`processBroadcastElement()`负责处理广播数据流。`processElement()`方法只能使用只读的上下文，`processBroadcastElement()`方法可以使用支持读写的上下文

**异步函数（AsyncFunction）** 是对Java异步编程框架的封装，异步函数抽象类`RichAsyncFunction`实现AsyncFunction接口，继承AbstractRichFunction获得了生命周期管理和RuntimeContext的访问能力。AsyncFunction接口定义了两种行为，异步调用行为（`asyncInvoke()`）将调用结果封装到ResultFuture中，调用超时行为（`timeout()`）防止不释放资源

**数据源函数（SourceFunction）** 提供从外部存储读取数据的能力，有几个关键行为：生命周期管理、读取数据、数据发送、生成Watermark并向下游发送、空闲标记^[如果读取不到数据，则将该Task标记为空闲，向下游发送Status#Idle，阻止Watermark向下游传递]。数据发送、生成Watermark并向下游发送、空闲标记都定义在SourceContext中，SourceContext按照带不带时间分为NonTimestampContext和WatermarkContext。NonTimestampContext为所有元素赋予-1作为时间戳，即永远不会向下游发送Watermark，在实际处理中，各个计算节点会根据本地时间定义触发器，触发执行窗口计算，而不是根据Watermark来触发。WatermarkContext定义了与Watermark相关的行为，负责管理当前的StreamStatus，确保StreamStatus向下游传递，负责空闲检测逻辑，当超过设定的事件间隔而没有收到数据或者Watermark时认为Task处于空闲状态

**输出函数（SinkFunction）** 定义了数据写出到外部存储的行为。`TwoPhaseCommitSinkFunction`是Flink中实现端到端Extractly-Once的关键函数，提供框架级别的端到端Extractly-Once的支持

**检查点函数** 提供函数级别状态管理能力，在检查点函数接口（`ListCheckpointed`）中主要设计了状态快照的备份和恢复两种行为，`snapshotStat()`用于备份保存状态到外部存储，`restoreState()`负责初始化状态，执行从上一个检查点恢复状态的逻辑，还提供了状态重分布的支持

### 使用CoProcessFunction实现双流Join

即时双流join：
1. 创建1个State对象
2. 接收到输入流1的事件后更新State
3. 接收到输入流2的事件后遍历State，根据Join条件进行匹配，将匹配后的结果发送到下游

延迟双流join：在流式数据里，数据可能是乱序的，数据会延迟到达，并且为了提供处理效率，使用小批量计算模式，而不是每个事件触发一次Join计算
1. 创建2个State对象，分别缓存输入流1和输入流2的事件
2. 创建1个定时器，等待数据的到达，定时延迟触发Join计算
3. 接收到输入流1的事件后更新State
4. 接收到输入流2的事件后更新State
5. 定时器遍历State1和State2，根据Join条件进行匹配，将匹配后的结果发送到下游

### 延迟计算

窗口计算是典型的延迟计算，使用窗口暂存数据，使用Watermark触发窗口的计算。触发器在算子层面上提供支持，所有支持延迟计算的算子都继承了Triggerable接口。Triggerable接口主要定义了基于事件时间和基于处理时间的两种触发行为（`onEventTime()`和`onProcessingTime()`）。

```bob-svg
                                   Triggerable SteamOperator
               .---------------------------------------------------------------.
               |                             .------------------------------.  |
               |                             |  InternalTimeServiceManager  |  |
  InputStream  |   .----------------.        |     .-----------------.      |  |
---------------+-->|processWatermark|--------+---->| advanceWatermark|      |  |
               |   '----------------'        |     '-----------------'      |  |
               |                             |              |               |  |
               |                             |              v               |  |
               |                             |  .-------------------------. |  |
               |            +----------------+--|triggerTarget.onEventTime| |  |
               |            |                |  '-------------------------' |  |
               |            |                '------------------------------'  |
               |            |                                                  |
               |            |                      .-----------------.         |
               |            v                      | ProcessFunction |         |
               |      .-----------.                |    .---------.  |         | OutputStream
               |      |onEventTime|----------------+--->| onTimer |--+---------+-------------->
               |      '-----------'                |    '---------'  |         |
               |                                   '-----------------'         |
               '---------------------------------------------------------------'

```

## 数据分区（Partition）

对于分布式计算引擎，需要将数据切分，交给位于不同物理节点上的Task计算。StreamPartitioner是Flink中的数据流分区抽象接口，决定了在实际运行中的数据流分发模式。所有数据分区器都实现了ChannelSeletor接口，该接口中定义了负载均衡选择行为。

```Java
/**
 * 每一个分区器都知道下游通道数量，通道数量在一次作业运行中是固定的，除非修改作业并行度，否则该值是不会改变的
 */
public interface ChannelSelector<T extends IOReadableWritable> {
    // 下游可选channel的数量
    void setup(int numberOfChannels);
    // 选路方法
    int selectChannel(T record);
    // 是否向下游广播
    boolean isBroadcast();
}
```

StreamPartitioner有以下几种实现：
+ ForwardPartitioner: 用于在同一个OperatorChain中上下游算子之间的数据转发，实际上数据是直接传递给下游的
+ ShufflePartitioner: 以随机方式将元素进行分区，可以确保下游的Task能够均匀地获取数据，通过`DataStream#shuffle()`方法使用ShufflePartitioner
+ RebalancePartitioner: 以Round-robin方式将元素进行分区，可以确保下游的Task可以均匀地获得数据，避免数据倾斜，通过`DataStream#rebalance()`方法使用RebalancePartitioner
+ RescalingPartitioner: 根据上下游Task数量先对上下游分区进行划分，然后以Round-robin方式将元素分给下游对应的分区（而非所有分区），通过`DataStream#rescale()`方法使用RescalingPartitioner
+ BroadcastPartitioner: 将元素广播给所有分区，通过`DataStream#broadcast()`方法使用BroadcastPartitioner
+ KeyGroupStreamPartitioner: 根据KeyGroup索引编号进行分区，不是提供给用户用的，KeyedStream在构造Transformation时默认使用KeyedGroup分区形式
+ CustomPartitionerWrapper: 使用用户自定义分区函数，为每一个元素选择目标分区

## 连接器（Connector）

连接器基于SourceFunction和SinkFunction提供了从数据源读取/写入数据的能力，Flink在Flink-connectors模块中提供了内置的Connector，包含常见的数据源，如HDFS、Kafka、HBase等。Kafka连接器使用SinkFunction向Kafka集群的Topic写入数据，SinkFunction中使用了Kafka的Producer，使用SourceFunction从Kafka集群读取数据，SourceFunction的实现中使用了KafkaConsumerProducer。
