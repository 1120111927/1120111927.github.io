---
title: Flink基础
date: "2022-12-20"
description: Flink抽象了Flink API作为数据流和在数据流上的操作，抽象了Transformation作为API层到执行层转换的中间层，抽象了算子、函数、数据分区作为运行时业务逻辑的载体，抽象了数据IO屏蔽外部存储的差异
tags: StreamExecutionEnvironment、StreamElement、Transformation、Function、StreamOperator
---

```toc
ordered: true
class-name: "table-of-contents"
```

## 核心抽象

Flink抽象了Flink API作为数据流和在数据流上的操作，抽象了Transformation作为API层到执行层转换的中间层，抽象了算子、Function、数据分区作为运行时业务逻辑的载体，抽象了数据IO屏蔽外部存储的差异。Function用于实现业务逻辑；Transformation是一个逻辑概念，记录上游数据来源，将用户处理逻辑组织成流水线；算子在运行时从上游获取数据、交给用户自定义函数执行并将执行结果交给下游，同时提供容错支持。

### 环境对象

环境对象提供了作业开发和启动执行的入口、配置信息的传递^[在作业提交、部署、运行时可以获取这些配置信息]等，包括执行环境（StreamExecutionEnvironment），运行时环境（Environment）和运行时上下文（RuntimeContext）3种环境对象。

**执行环境**是Flink应用开发时的概念，表示流计算作业的执行环境，是Flink作业开发和启动执行的入口。分为以下几种

+ LocalStreamEnvironment：表示本地执行环境^[在单个JVM中使用多线程模拟Flink集群]，一般用作本地开发、调试，基本工作流程如下：
    1. 执行Flink作业的Main函数生成StreamGraph，转化为JobGraph
    2. 设置任务运行的配置信息
    3. 根据配置信息启动对应的LocalFlinkMiniCluster
    4. 根据配置信息和MiniCluster生成对应的MiniClusterClient
    5. 通过MiniClusterClient提交JobGraph到MiniCluster

+ RemoteStreamEnvironment：表示远程Flink集群的执行环境，基本工作流程如下：
    1. 执行Flink作业的Main函数生成StreamGraph，转化为JobGraph
    2. 设置任务运行的配置信息
    3. 提交JobGraph到远程Flink集群

+ StreamContextEnvironment：Cli命令行或单元测试执行环境

+ StreamPlanEnvironment：用于在Flink Web UI管理界面中可视化展现Job时生成执行计划StreamGraph

+ ScalaShellStreamEnvironment：Scala Shell执行环境，用于在命令行中交互式开发FLink作业

**运行时环境**是Flink应用运行时作业级别的概念，定义了在运行时刻Task所需要的所有配置信息，包括在静态配置和调度器调度之后生成的动态配置信息，有RuntimeEnvironment和SavepointEnvironment两个实现类
+ RuntimeEnvironment在Task开始执行时进行初始化，封装了Task运行相关信息（包括配置信息和运行时的各种服务）
+ SavepointEnvironment是Environment的最小化实现，用在State Processor API（状态处理接口）中，State Processor API用于操作应用程序状态，借助批执行模式的DataStream API读写、修改保存点和检查点的数据。

**运行时上下文**是Flink应用运行时任务级别的概念，是用户自定义函数运行时的上下文，用户自定义函数的每个实例都有一个RuntimeContext对象（在RichFunction中通过`getRuntimeContext()`方法可以访问该对象），让用户自定义函数在运行时能够获取到作业级别的信息（如Task名称、执行配置信息、并行度信息、状态等）。不同的使用场景中有不同的RuntimeContext，具体如下：
+ StreamingRuntimeContext: 在流计算UDF中使用，用来访问作业信息、状态等
+ SavepointRuntimeContext：在State Processor API中使用
+ CepRuntimeContext：在CEP复杂事件处理中使用

### 数据流元素（StreamElement）

<div class='wrapper' markdown='block'>

```plantuml
@startuml

!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
skinparam ArrowThickness 1

abstract class StreamElement
class StreamRecord<T> {
    T value
    long timestamp
    boolean hasTimestamp
}
class LatencyMarker {
    long markedTime
    OperatorID operatorId
    int subtaskIndex
}
class Watermark {
    long timestamp
}
class StreamStatus {
    int status
}

StreamElement <|-- StreamRecord
StreamElement <|-- LatencyMarker
StreamElement <|-- Watermark
StreamElement <|-- StreamStatus

@enduml
```

</div>

数据流元素（StreamElement）有以下4种^[Barrier不属于StreamElement，Flink单独处理]，在执行层面上都被序列化成二进制数据，形成混合的数据流，在算子中将混合数据流中的数据流元素反序列化出来，根据其类型分别进行处理。

+ **数据记录（StreamRecord）**：表示数据流中的一条记录（或一个事件），包括数据值本身和事件时间戳（可选）
+ **延迟标记（LatencyMarker）**：用来近似评估数据从读取到写出之间的延迟^[不包含计算的延迟]，在Source中生成，并向下游发送，绕过业务处理逻辑，在Sink节点中使用LatencyMarker估计数据在整个DAG图中流转花费的时间，用来近似评估总体上的处理延迟，包括在数据源算子中周期性创建的时间戳、算子编号、数据源算子所在的Task编号
+ **水印（Watermark）**：是一个时间戳，用来告诉算子所有时间早于等于Watermark的记录（或事件）都已到达，不会再有比Watermark更早的记录，算子可以根据Watermark触发窗口的计算、清理资源等
+ **流状态标记（StreamStatus）**：表示两种流状态空闲状态（IDLE）和活动状态（ACTIVE），在Source中生成，并向下游发送，用来通知Task是否会继续接收到上游的记录或者Watermark

### 数据转换（Transformation）

<div class='wrapper' markdown='block'>

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

</div>

Transformation是衔接DataStream API和Flink内核的逻辑结构，调用DataStream API的数据处理流水线最终会转换为Transformation流水线。

Transformation有两大类：物理Transformation和虚拟Transformation。在运行时，DataStream API的调用都会被转换成Transformation，然后物理Transformation被转换为实际运行的算子，而虚拟Transformation则不会转换为具体的算子。

Transformation包含了Flink运行时的一些关键参数：
+ name: transformation名称，用于可视化
+ uid: 由用户指定，主要用于在作业重启时再次分配跟之前相同的uid，可以持久保存状态
+ bufferTimeout: buffer超时时间
+ parallelism: 并行度
+ id: 基于静态累加器生成
+ outputType: 输出类型，用来序列化数据
+ slotSharingGroup: Slot共享组

物理Transformation：
+ SourceTransformation: 用于从数据源读取数据，是Flink作业的起点，只有下游Transformation，没有上游Transformation。一个作业可以有多个SourceTransformation，从多个数据源读取数据
+ SinkTransformation: 用于将数据写到外部存储，是Flink作业的终点，只有上游Transformation，没有下游Transformation。一个作业可以有多个SinkTransformation，将数据写入不同的外部存储
+ OneInputTransformation: 单流输入Transformation，只接收一个输入流
+ TwoInputTransformation: 双流输入的Transformation，接收两种流（第1输入和第2输入）

虚拟Transformation：
+ SideOutputTransformation: 用于旁路输出，表示上游Transformation的一个分流，一个Transformation可以有多个分流，每个分流通过OUtputTag进行标识
+ SplitTransformation / SelectTransformation: SplitTransformation按条件将一个流拆分成多个流（只是逻辑上的拆分，只影响上游的流如何跟下游的流连接），SelectTransformation用来在下游选择数据流
+ PartitionTransformation: 用于改变输入元素的分区，需要接收一个StreamPartitioner对象进行分区
+ UnionTransformation: 用于将合并多个元素数据结构安全相同的输入流
+ FeedbackTransformation / CoFeedbackTransformation: 把符合条件的数据重新发回上游Transformation处理

### 算子（StreamOperator）

Flink作业运行时由Task组成一个Dataflow，每个Task中包含一个或者多个StreamOperator，一个StreamOperator就是一个计算步骤，具体的计算由StreamOperator中包装的Function来执行。

所有StreamOperator都包含生命周期管理、状态与容错管理、数据处理3个方面的关键行为，StreamOperator中还定义了OperatorChain策略用于算子融合优化。

StreamTask作为StreamOperator的容器，负责管理StreamOperator的生命周期，StreamOperator生命周期核心阶段如下：
1. setup: 初始化环境、时间服务、注册监控等
2. open: 由各个具体的StreamOperator负责实现，包含了StreamOperator的初始化逻辑（如状态初始化等），执行该方法后才会执行Function进行数据的处理
3. close: 所有的数据处理完毕之后关闭StreamOperator，此时需要确保将所有的缓存数据向下游发送
4. dispose: StreamOperator生命周期的最后阶段，此时StreamOperator已经关闭，停止处理数据，进行资源的释放

StreamOperator负责状态管理，触发检查点时保存状态快照并且将快照异步保存到外部的分布式存储，当作业失败时负责从保存的快照中恢复状态。

StreamOperator对数据的处理，包括数据记录的处理（`processElement()`）、Watermark的处理（`processWatermark()`）、LatencyMarker的处理（`processLatencyMarker()`）。

**异步算子**用于解决与外部系统交互时网络延迟所导致的系统瓶颈问题。Async I/O的实现原理是连续地向数据库发送多个请求，回复先返回的请求先处理，从而使连续地请求之间不需要阻塞等待。对于后调用的请求先返回的情况，Flink在异步算子中提供了两种输出模式：
+ 顺序输出模式：先收到的数据元素先输出，后续数据元素的异步函数调用无论是否先完成，都需要等待。可以保证消息不乱序，但是可能增加延迟，降低算子的吞吐量。原理为数据进入算子，对每一条数据元素调用异步函数，并封装为ResultFuture放入到排队队列中（如果是Watermark，也会放入到队列中），输出时，严格按照队列中的顺序输出给下游
+ 无序输出模式：先处理完的数据元素先输出。延迟低、吞吐量高，但是不保证消息顺序。原理为数据进入算子，对每一条数据元素调用异步函数，并封装为ResultFuture放入到等待完成队列中（如果是Watermark，也会放入到队列中），当异步函数返回结果时，放入已完成队列，按照顺序输出给下游。无序输出模式并不是完全无序，仍然要保持Watermark不能超越其前面数据元素的原则。等待完成队列中将按照Watermark切分成组，组内可以无序输出，组之间必须严格保证顺序。

异步算子同时也支持对异步函数调用的超时处理，支持完整的容错特性。

### Function

<div class='wrapper' markdown='block'>

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

</div>

在Flink中，数据处理的业务逻辑位于Function的`processElement()`方法中，算子调用Function处理数据完毕之后，通过Collector接口将数据交给下一个算子。

按照输入和输出的不同特点分类，Flink中Function大概分为3类：
+ SourceFunction: 从外部存储读取数据，无上游
+ SinkFunction: 将数据写入外部存储，无下游
+ 一般Function: 用在作业的中间处理步骤中，有上游，也有下游。进一步可分为单流输入和双流输入两种，多流输入可以通过多个双流输入串联而成

**无状态函数** 用来做无状态计算（如MapFunction），一般都是直接继承接口或通过匿名类实现接口

**富函数（RichFunction）** 提供生命周期管理、访问RuntimeContext的能力，`open()`^[Function启动时执行初始化]和`close()`^[Function停止时执行清理，释放占用的资源]方法来管理Function生命周期，`getRuntimeContext()`和`setRuntimeContext()`方法来访问RuntimeContext，进而能够获取到执行时作业级别的参数信息，操作状态

**处理函数** 可以访问流应用程序所有非循环基本构建块，事件、状态和定时器。分为ProcessFunction、CoProcessFunction、KeyedProcessFunction、KeyedCoProcessFunction，Keyed类处理函数只能用在KeyedStream上，Co类处理函数是双流输入

**广播函数** 用于广播状态模式^[将一个数据流的内容广播到另一个流中]，分为BroadcastProcessFunction和KeyedBroadcastProcessFunction，有两个数据元素处理方法，`processElement()`负责处理一般的数据流，`processBroadcastElement()`负责处理广播数据流。`processElement()`方法只能使用只读的上下文，`processBroadcastElement()`方法可以使用支持读写的上下文

**异步函数（AsyncFunction）** 是对Java异步编程框架的封装，异步函数抽象类`RichAsyncFunction`实现AsyncFunction接口，继承AbstractRichFunction获得了生命周期管理和RuntimeContext的访问能力。AsyncFunction接口定义了两种行为，异步调用行为（`asyncInvoke()`）将调用结果封装到ResultFuture中，调用超时行为（`timeout()`）防止不释放资源

**数据源函数（SourceFunction）** 提供从外部存储读取数据的能力，有几个关键行为：生命周期管理、读取数据、数据发送、生成Watermark并向下游发送、空闲标记^[如果读取不到数据，则将该Task标记为空闲，向下游发送Status#Idle，阻止Watermark向下游传递]。数据发送、生成Watermark并向下游发送、空闲标记都定义在SourceContext中，SourceContext按照带不带时间分为NonTimestampContext和WatermarkContext。NonTimestampContext为所有元素赋予-1作为时间戳，即永远不会向下游发送Watermark，在实际处理中，各个计算节点会根据本地时间定义触发器，触发执行窗口计算，而不是根据Watermark来触发。WatermarkContext定义了与Watermark相关的行为，负责管理当前的StreamStatus，确保StreamStatus向下游传递，负责空闲检测逻辑，当超过设定的事件间隔而没有收到数据或者Watermark时认为Task处于空闲状态

**输出函数（SinkFunction）** 定义了数据写出到外部存储的行为。`TwoPhaseCommitSinkFunction`是Flink中实现端到端Extractly-Once的关键函数，提供框架级别的端到端Extractly-Once的支持

**检查点函数** 提供函数级别状态管理能力，在检查点函数接口（`ListCheckpointed`）中主要设计了状态快照的备份和恢复两种行为，`snapshotStat()`用于备份保存状态到外部存储，`restoreState()`负责初始化状态，执行从上一个检查点恢复状态的逻辑，还提供了状态重分布的支持

#### 使用CoProcessFunction实现双流Join

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

#### 延迟计算

窗口计算是典型的延迟计算(窗口暂存数据，Watermark触发窗口的计算)。触发器在StreamOperator层面上提供支持，所有支持延迟计算的StreamOperator都继承了Triggerable接口。Triggerable接口主要定义了基于事件时间和基于处理时间的两种触发行为（`onEventTime()`和`onProcessingTime()`）。

```bob-svg
                                   Triggerable SteamOperator
               .----------------------------------------------------------------.
               |                             .-------------------------------.  |
               |                             |  InternalTimeServiceManager   |  |
  InputStream  |  .------------------.       |     .------------------.      |  |
---------------+->| processWatermark |-------+---->| advanceWatermark |      |  |
               |  '------------------'       |     '--------+---------'      |  |
               |                             |              |                |  |
               |                             |              v                |  |
               |                             |  .-----------+--------------. |  |
               |            +----------------+--|triggerTarget.onEventTime | |  |
               |            |                |  '--------------------------' |  |
               |            |                '-------------------------------'  |
               |            |                                                   |
               |            |                      .-----------------.          |
               |            v                      | ProcessFunction |          |
               |      .-----+-----.                |    .---------.  |          | OutputStream
               |      |onEventTime|----------------+--->| onTimer |--+----------+-------------->
               |      '-----------'                |    '---------'  |          |
               |                                   '-----------------'          |
               '----------------------------------------------------------------'
```

### 数据分区（Partition）

对于分布式计算引擎，需要将数据切分，交给位于不同物理节点上的Task计算。StreamPartitioner是Flink中的数据流分区抽象接口，决定了在实际运行中的数据流分发模式。所有数据分区器都实现了ChannelSeletor接口，该接口中定义了负载均衡选择行为。

```Java
/**
 * 每一个分区器都知道下游通道数量，通道数量在一次作业运行中是固定的，除非修改作业并行度，否则该值是不会改变的
 */
public interface ChannelSelector<T extends IOReadableWritable> {
    // 下游channel的数量
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

### 连接器（Connector）

连接器基于SourceFunction和SinkFunction提供了从外部存储读取/写入数据的能力，Flink在Flink-connectors模块中提供了内置的连接器，包含常见的数据源，如HDFS、Kafka、HBase等。

#### Kafka连接器实现

Kafka连接器使用SinkFunction向Kafka集群的Topic写入数据，SinkFunction中使用了Kafka的Producer，使用SourceFunction从Kafka集群读取数据，SourceFunction的实现中使用了KafkaConsumerProducer。

## 状态（State）

对于流计算而言，事件持续不断地产生，如果每次计算都是相互独立的，不依赖于上下游的事件，则是无状态计算，如果计算需要依赖于之前或者后续的事件，则是有状态计算。状态用来保存中间计算结果或者缓存数据，为有状态计算提供容错。

按照数据结构的不同，Flink中定义了多种状态，应用于不同的场景：
+ `ValueState<T>`：单值状态，使用`update()`方法更新状态值，使用`value()`方法获取状态值
+ `ListState<T>`：列表状态，使用`add()` / `addAll()`方法添加元素，使用`get()`方法返回一个`Iterable<T>`来遍历状态值，使用`update()`覆盖当前列表
+ `MapState<K, V>`：Map状态，使用`put()` / `putAll()`方法添加键值对，使用`get()`方法获取元素，使用`entries()`、`keys()`、`values()`方法获取map、键、值的可迭代视图
+ `ReducingState<T>` / `AggregatingState<IN, OUT>` / `FoldingState<T, ACC>` ^[ReducingState聚合类型和添加的元素类型必须相同，而AggregatingState、FoldingState聚合类型和添加的元素类型可以不同]：聚合状态（表示添加到状态的所有值的聚合），使用`update()`方法添加元素（会使用提供的ReduceFunction / AggregateFunction / FoldFunction进行聚合），使用`get()`方法获取状态值

状态描述（StateDescriptor）用于描述状态信息（状态名称、类型信息、序列化/反序列化器、过期时间等），每一类State都有对应的StateDescriptor。运行时，在RichFunction和ProcessFunction中，通过RuntimeContext对象，使用StateDesctiptor从状态后端（StateBackend）中获取实际的状态实例。

状态按照是否有Key分为KeyedState和OperatorState两种。KeyedState在KeyedStream中使用，跟特定的Key绑定，即KeyedStream流上的每一个Key对应一个State对象，可以使用所有类型的状态，保存在StateBackend中。OperatorState跟特定算子的一个实例绑定，整个算子只对应一个状态，只支持ListState。

按照由Flink管理还是用户自行管理，状态分为原始状态（Raw State）和托管状态（Managed State）。原始状态是用户自定义的状态，Flink做快照时把整个状态当作一个整体，需要用户自己管理，使用byte数组读写状态内容。托管状态是由Flink框架管理的状态，其序列化和反序列化由FLink框架提供支持，用户无感知。

广播状态（BroadcastState）在广播状态模式中使用，必须是MapState。广播状态模式是指来自一个流的数据需要被广播到所有下游任务，在算子本地存储，在处理另一个流的时候依赖于广播的数据，广播状态模式需要使用广播函数进行处理^[广播函数提供了处理广播数据流和普通数据流的接口]。

状态管理需要考虑以下几点：
+ 状态数据的存储和访问：在任务内部，如何高效地保存状态数据和使用状态数据
+ 状态数据的备份和恢复：如何高效地将状态数据保存下来，避免状态备份降低集群地吞吐量，并且在Failover的时候恢复作业到失败前的状态
+ 状态数据的划分和动态扩容：如何对状态数据进行切分，在作业修改并行度导致任务数量改变的时候确保正确地恢复到任务
+ 状态数据的清理：如何及时清理过期的状态

### 状态数据的存储和访问

Flink中使用状态有两种典型场景：
+ 状态操作接口：使用状态对象本身存储、写入、更新数据，分为面向应用开发者的状态接口和内部状态接口（Flink框架使用）。面向开发者的状态接口只提供了对状态中数据的添加、更新、删除等基本的操作接口。内部状态接口除了对状态中数据的访问之外还提供了内部的运行时信息接口，如状态中数据的序列化器、命名空间、命名空间序列化器、命名空间合并的接口
+ 状态访问接口：从状态后端获取状态本身。Flink抽象了OperatorStateStore和KeyedStateStore两个状态访问接口，屏蔽状态后端。OperatorStateStore中使用内存以Map形式来保存数据，KeyedStateStore中使用RocksDBStateBackend或者HeapKeyedStateBackend来保存数据，获取/创建状态都交给了具体的状态后端处理

状态后端（StateBackend）用于存储状态，需具备在计算过程中提供访问状态的能力、将状态持久化到外部存储的容错能力。Flink内置了3种状态后端：
+ MemoryStateBackend（内存型状态后端）：纯内存，适用于验证、测试。运行时所需要的状态数据保存在TaskManager JVM堆上内存中，键值类型的状态、窗口算子的状态使用HashTable来保存数据、触发器等，执行检查点时，会把状态的快照数据保存到JobManager进程的内存中
+ FsStateBackend（文件型状态后端）：内存+文件，适用于长周期大规模的数据。运行时所需要的状态数据保存在TaskManager JVM堆上内存中，执行检查点时，会把状态的快照数据保存到配置的文件系统中（分布式文件系统或本地文件系统）
+ RocksDBStateBackend：RocksDB，适用于长周期大规模的数据。使用嵌入式的本地数据库RocksDB将流计算数据状态存储在本地磁盘中，执行检查点时，再将整个RocksDB中保存的状态数据全量或者增量持久化到配置的文件系统中，在JobManager内存中会存储少量的检查点数据。相比基于内存的状态后端，访问状态的成本高很多，可能导致数据流的吞吐量剧烈下降

MemoryStateBackend和FsStateBackend依赖于HeapKeyStateBackend，HeapKeyStateBackend使用StateTable存储数据，StateTable有NestedMapsStateTable和CopyOnWriteStateTable两个子类，NestedMapStateTabel使用两层嵌套的HashMap保存状态数据，支持同步快照。CopyOnWriteStateTable使用CopyOnWriteStateMap来保存数据，支持异步快照。

### 状态数据的备份和恢复

**全量持久化策略** 每次把全量的状态写入状态存储中，HeapKeyedStateBackend对应HeapSnapshotStrategy策略，RocksDBStateBackend对应RocksFullSnapshotStrategy策略。在执行持久化策略时，使用异步机制，每个算子独立启动1个独立的线程，将自身的状态写入分布式可靠存储中，在做持久化的过程中，状态可能会被持续修改，基于内存的状态后端使用CopyOnWriteStateTable来保证线程安全，RocksDBStateBackend则使用RocksDB的快照机制，使用快照来保证线程安全

**增量持久化策略** 每次持久化增量的状态，只有RocksDBStateBackend支持增量持久化。RocksDB是一个基于LSM-Tree的键值存储，新的数据保存在内存中，成为memtable，一旦memtable写满了，RocksDB就会将数据压缩并写入到磁盘，memtable的数据持久化到磁盘后就变成了不可变的sstable。RocksDB会在后台合并sstable并删除其中重复的数据，然后在RocksDB删除原来的sstable，替换成新合成的sstable，新的sstable包含了被删除的sstable中的信息，通过合并，历史sstable会合并成一个新的sstable，并删除这些历史sstable，可以减少检查点的历史文件，避免大量小文件的产生。因为sstable是不可变的，Flink对比前一个检查点创建和删除的RocksDB sstable文件就可以计算出状态有哪些改变。为了确保sstable是不可变的，Flink会在RocksDB上触发刷新操作，强制将memtable刷新到磁盘上，在Flink执行检查点时，会将新sstable持久化到存储中，同时保留引用。

### 状态数据的划分和动态扩容

OpeartorState重分布：
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

### 状态数据的清理

DataStream作业中可以通过StateDescriptor的`enableTimeToLive(stateTtlConfig)`方法对状态过期进行精细控制，对每一个状态设置清理的策略，StateTtlConfig中可以设置的内容如下：
+ 过期时间：超过多长时间未访问，视为状态过期
+ 过期时间更新策略：创建和写时更新、读取和写时更新
+ 状态可见性：未清理可用，超期则不可用

Flink SQL是高层抽象，没有状态概念，可以通过Flink Table API和SQL的参数配置选项设置状态清理策略（StreamQueryConfig的withIdleStateRetentionTime()）^[定时清理状态存在可能因为状态被清理而导致计算结果不完全准确的风险，过期时间一般为1.5天左右]。

默认情况下，只有在明确读出过期值时才会删除过期值。使用StateTtlConfig的`cleanupFullSnapshot()`设置完整快照时清理状态，在获取完整状态快照时激活清理，减少其大小（在当前实现下不清除本地状态，但在从上一个快照恢复的情况下，不会包括已删除的过期状态）。使用StateTtlConfig的`cleanupIncrementally()`设置增量清理状态，当进行状态访问或者清理数据时，在回调函数中进行处理，当每次增量清理触发时，遍历状态后端中的状态，清理掉过期的。

## 窗口

窗口（windowing）是指将stream切分成有界的数据块，然后对各个数据块进行处理。窗口实现为Window（具体实现有TimeWindow和GlobalWindow），Window本身不会存储窗口中的元素（其内部可能存储了一些元数据，如TimeWindow中有开始时间start和结束时间end），窗口中的元素实际在状态后端以键值对状态形式存储（键为Window，值为数据集合或聚合值）。

```
// 对于keyed stream，因为每个逻辑上的keyed stream都可以被单独处理，窗口计算可以由多个task并行（属于同一个 key 的元素会被发送到同一个 task）
stream
    .keyBy(...)                                        <-  仅 keyed 窗口需要
    .window(assigner)                                  <-  必填项："assigner"
    [.trigger(...)]                                    <-  可选项："trigger" (省略则使用默认 trigger)
    [.evictor(...)]                                    <-  可选项："evictor" (省略则不使用 evictor)
    [.allowedLateness(...)]                            <-  可选项："lateness" (省略则为 0)
    [.sideOutputLateData(...)]                         <-  可选项："output tag" (省略则不对迟到数据使用 side output)
    .<windowed transformation>(<window function>)      <-  必填项："function"
    [.getSideOutput(...)]                              <-  可选项："output tag"

// 对于non-keyed stream，原始stream不会被分割为多个逻辑上的stream，所以所有的窗口计算会被同一个 task 完成（并行度为1）
stream
    .windowAll(assigner)                              <-  必填项："assigner"
    [.trigger(...)]                                   <-  可选项："trigger" (else default trigger)
    [.evictor(...)]                                   <-  可选项："evictor" (else no evictor)
    [.allowedLateness(...)]                           <-  可选项："lateness" (else zero)
    [.sideOutputLateData(...)]                        <-  可选项："output tag" (else no side output for late data)
    .<windowed transformation>(<window function>)     <-  必填项："function"
    [.getSideOutput(...)]                             <-  可选项："output tag"
```

**window assigner** 实现为WindowAssigner，定义了stream中的元素如何被分发到各个窗口，即负责将stream中的每个数据分发到一个或多个窗口中，在`window()`或`windowAll()`中指定。Flink内置了常用的window assigner，包括滚动窗口（tumbling window）、滑动窗口（sliding window）、会话窗口（session window）和全局窗口（global window）。所有内置的window assigner都是基于时间分发数据的，基于时间的窗口（TimeWindow）用开始时间戳（包含）和结束时间戳（不包含）描述窗口的大小。

+ 滚动窗口：窗口大小固定，且各自之间不重叠，其assigner（TumblingProcessingTimeWindows、TumblingEventTimeWindows）分发元素到指定大小的窗口
+ 滑动窗口^[滚动窗口是一种特殊的滑动窗口]：窗口大小固定，各自之间可以重叠，window size指定窗口大小，window slide指定滑动距离（控制新窗口生成速率），其assigner（SlidingProcessingTimeWindows、SlidingEventTimeWindows）分发元素到指定大小的窗口
+ 会话窗口：窗口大小不固定（没有固定的开始或结束时间），在一段时间没有收到数据（即在一段不活跃的间隔）之后会关闭，并将接下来的数据分发到新的会话窗口，各自之间不重叠，session gap指定固定的会话间隔，session gap extractor指定动态的会话间隔，其assigner（ProcessingTimeSessionWindows、EventTimeSessionWindows、DynamicProcessingTimeSessionWindows、DynamicEventTimeSessionWindows）把数据按活跃的会话分组
+ 全局窗口：其assigner（GlobalWindows）将拥有相同 key 的所有数据分发到一个全局窗口，仅在指定了自定义trigger时有用，否则，计算不会发生，因为全局窗口没有天然的终点去触发其中积累的数据

会话窗口不同于滚动窗口和滑动窗口，它的切分依赖于事件的行为，而不是时间序列，在很多情况下会因为事件乱序使得原本相互独立的窗口因为新事件的到来导致窗口重叠，而必须进行窗口的合并。窗口的合并涉及3个要素：
+ 窗口对象的合并和清理：对于会话窗口，为每个事件分配一个SessionWindow，然后判断窗口是否需要与已有的窗口进行合并，窗口合并时按照窗口的起始时间进行排序，然后判断窗口之间是否存在时间重叠，重叠的窗口进行合并，将后序窗口合并到前序窗口中
+ 窗口状态的合并和清理：窗口合并的同时，窗口对应的状态也需要进行合并，默认复用最早的窗口的状态，将其他待合并窗口的状态合并到最早窗口的状态中
+ 窗口触发器的合并和清理：`Trigger#onMerge()`方法用于对触发器进行合并，触发器的合并实际上是删除合并的窗口的触发器，并为新窗口创建新的触发器

<div class="wrapper" markdown='block'>

```plantuml
@startuml

!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
skinparam ArrowThickness 1

abstract class WindowAssigner
class EventTimeSessionWindows {
    long sessionTimeout
}
class ProcessingTimeSessionWindows {
    long sessionTimeout
}
class DynamicEventTimeSessionWindows {
    SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor
}
class DynamicProcessingTimeSessionWindows {
    SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor
}
class TumblingEventTimeWindows {
    long size
    long offset
}
class TumblingProcessingTimeWindows {
    long size
    long offset
}
abstract class MergingWindowAssigner
class SlidingEventTimeWindows {
    long size
    long slide
    long offset
}
class SlidingProcessingTimeWindows {
    long size
    long slide
    long offset
}

WindowAssigner <|-- MergingWindowAssigner
MergingWindowAssigner <|-- EventTimeSessionWindows
MergingWindowAssigner <|-- ProcessingTimeSessionWindows
MergingWindowAssigner <|-- DynamicEventTimeSessionWindows
MergingWindowAssigner <|-- DynamicProcessingTimeSessionWindows
WindowAssigner <|-- TumblingEventTimeWindows
WindowAssigner <|-- TumblingProcessingTimeWindows
WindowAssigner <|-- SlidingEventTimeWindows
WindowAssigner <|-- SlidingProcessingTimeWindows
WindowAssigner <|-- GlobalWindows

@enduml
```

</div>

**window function** 实现为WindowFunction，定义了如何计算每个窗口中的数据，窗口函数有三种ReduceFunction、AggregateFunction和ProcessWindowFunction。ReduceFunction指定两条输入数据如何合并起来产生一条输出数据，输入和输出数据的类型必须相同。AggregateFunction接收输入数据的类型(IN，输入流的元素类型)、累加器的类型（ACC）和输出数据的类型（OUT）。AggregateFunction接口有如下几个方法： 把每一条元素加进累加器、创建初始累加器、合并两个累加器、从累加器中提取输出（OUT 类型）。ReduceFunction和AggregateFunction在每条数据到达窗口后进行增量聚合。ProcessWindowFunction会得到能够遍历当前窗口内所有数据的Iterable以及关于这个窗口的元信息，窗口中的数据无法被增量聚合，需要在窗口触发前缓存所有数据

**trigger** 实现为Trigger，决定了一个窗口何时可以被窗口函数处理。当trigger认定一个窗口可以被计算时，它就会触发，即 返回FIRE或 FIRE_AND_PURGE（这是让窗口算子发送当前窗口计算结果的信号），如果一个窗口指定了ProcessWindowFunction，所有的元素都会传给 ProcessWindowFunction，如果是ReduceFunction或AggregateFunction，则直接发送聚合的结果。FIRE会保留被触发的窗口中的内容，而 FIRE_AND_PURGE会删除这些内容。Trigger接口提供了五个方法来响应不同的事件：
+ `onElement()`方法在每个元素被加入窗口时调用
+ `onEventTime()`方法在注册的事件时间定时器（event-time timer）触发时调用
+ `onProcessingTime()`方法在注册的处理时间定时器（processing-time timer）触发时调用
+ `onMerge()`方法与有状态的trigger相关，该方法会在两个窗口合并时，将窗口对应trigger的状态进行合并
+ `clear()`方法处理在对应窗口被移除时所需的逻辑

其中，`onElement()`、`onEventTime()`、`onProcessingtime()`方法通过返回TriggerResult来决定trigger如何应对到达窗口的事件：CONTINUE（什么也不做）、FIRE（触发计算）、PURGE（清空窗口内的元素）、FIRE_AND_PURGE（触发计算，计算结束后清空窗口内的元素）。

Flink内置了EventTimeTrigger（在watermark越过窗口结束时间后直接触发）、ProcessingTimeTrigger（根据processing timer触发）、CountTrigger（在窗口中的元素超过预设的限制时触发）、PurgingTrigger（接收另一个trigger并将它转换成一个会清理数据的trigger）。其中，EventTimeTrigger是TumblingEventTimeWindows、SlidingEventTimeWindows、EventTimeSessionWindows等window assigner的默认trigger，ProcessingTimeTrigger是TumblingProcessingTimeWindows、SlidingProcessingTimeWindows、ProcessingTimeSessionWindows等window assigner的默认trigger。

```Java
abstract class Trigger<T, W extends Window> {
    abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx);
    abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx);
    abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx);
}
```

**evictor** 实现为Evictor接口，可以在trigger触发后、调用窗口函数之前或之后从窗口中删除元素。Evictor接口提供了两个方法`evictBefore()`、`evictAfter()`实现此功能，`evictBefore()`包含在调用窗口函数前的逻辑，而`evictAfter()`包含在窗口函数调用之后的逻辑，在调用窗口函数之前被移除的元素不会被窗口函数计算。Flink 内置有三个evictor：
+ CountEvictor：计数过滤器，在窗口中保留指定数量的元素，并从窗口头部开始丢弃其余元素
+ DeltaEvictor: 阈值过滤器，接收DeltaFunction和threshold参数，计算最后一个元素与窗口缓存中所有元素的差值，并移除差值大于或等于threshold的元素
+ TimeEvictor: 时间过滤器，接收interval参数，以毫秒表示，它会找到窗口中元素的最大timestamp max_ts并移除比max_ts - interval小的所有元素，即保留Window中最近一段时间内的元素，并丢弃其余元素

**allowed lateness** 定义了一个元素可以在迟到多长时间的情况下不被丢弃（默认值是0），在watermark超过窗口末端、到达窗口末端加上 allowed lateness之前的这段时间内到达的元素，依旧会被加入窗口。取决于窗口的trigger，一个迟到但没有被丢弃的元素可能会再次触发窗口。为了实现这个功能，Flink会将窗口状态保存到allowed lateness超时才会将窗口及其状态删除^[窗口的生命周期：一个窗口在第一个属于它的元素到达时就会被创建，然后在时间超过窗口的“结束时间戳 + 用户定义的 allowed lateness”时 被完全删除]。另外，可以通过旁路输出获取迟到的数据。

```Java
// 基于事件时间的滑动窗口数据元素分配
// SlidingEventTimeWindows.java
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
    if (timestamp > LONG.MIN_VALUE) {
        List<TimeWindow> windows = new ArrayList<>((int)(size/slide));
        Long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
        for (long start = lastStart; start > timestamp - size; start -= slide) {
            windows.add(new TimeWindow(start, start + size));
        }
        return windows;
    } else {
        throw new RuntimeException("Record has Long.MIN_VALUE timestamp (=no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call DataStream.assignTimestampsAndWatermarks(...)'?");
    }
}
```

### 水印

水印（Watermark）定义何时停止等待较早的事件，事件时间为t的水印代表t之前的事件都已经到达（水印之后时间戳<=t的任何事件都被称为延迟事件），Flink中事件时间的处理取决于水印生成器。通常用Watermark结合窗口来正确地处理乱序事件，水印用于权衡延迟和完整性（正确性），缩短水印边界时间来降低延迟，延长水印水印边界时间提高完整性，也可以实施混合方案，先快速产生初步结果，然后再处理延迟数据时更新这些结果。

#### DataStream Watermark生成

通常水印在SourceFunction中生成^[如果是并行计算的任务，多个并行执行的Source Function相互独立产生各自的水印]，Flink提供了额外的机制，允许在调用DataStream API操作之后，根据业务逻辑的需要，使用时间戳和水印生成器修改数据记录的时间戳和水印。

SourceFunction通过SourceContext的`collectWithTimestamp()`方法为数据元素分配时间戳，`emitWatermark()`方法向下游发送水印。

```Java
// SourceFunction中为数据元素分配时间戳和生成Watermark
public void run(SourceContext<MyType> ctx) {
    while (/* condition */) {
    +yType next = getNext();
    +tx.collectWithTimestamp(next, next.getEventTimestamp());
    +/ 生成Watermark并发送给下游
    +f (next.hasWatermarkTime()) {
    +   ctx.emitWatermark(new Watermark(next.getWatermarkTime()));
    +
    }
}
```

DataStream API中使用TimestampAssigner接口定义了时间戳的提取行为，包括AssignerWithPeriodicWatermarks和AssignerWithPunctuatedWatermarks，分别代表不同的Watermark生成策略。AssignerWithPeriodicWatermarks是周期性生成Watermark策略的顶层抽象接口，该接口的实现类周期性地生成Watermark，而不会针对每一个事件都生成。AssignerWithPunctuatedWatermarks对每一个事件都会尝试进行Watermark生成，但是如果生成的Watermark是null或Watermark小于之前的Watermark，则该Watermark不会发往下游^[发往下游也不会有任何效果，不会触发任何窗口的执行]。

#### Flink SQL Watermark生成

Flink SQL Watermark主要是在TableSource中生成的，其定义了3类生成策略：
+ PeridocWatermarksAssigner：周期性（一定时间间隔或达到一定的记录条数）地产生一个Watermark
    + AscendingTimestamps：递增Watermark，作用在Flink SQL中的Rowtime属性上，Watermark = 当前收到的数据元素的最大时间戳 - 1^[减1是为了确保有最大时间戳的事件不会被当作迟到数据丢弃]
    + BoundedOutOfOrderTimestamp：固定延迟Watermark，作用在Flink SQL的Rowtime属性上，Watermark = 当前收到的数据元素的最大时间戳-固定延迟
+ PuntuatedWatermarkAssigner：数据流中每一个递增的EventTime都会产生一个Watermark^[实际生产中Punctuated方式在TPS很高的场景下会产生大量的Watermark，在一定程度上会对下游算子造成压力，所以只有在实时性要求非常高的场景下才会选择Punctuated的方式进行Watermark的生成]
+ PreserveWatermak：用于DataStream API和Table & SQL混合编程，此时，Flink SQL中不设定Watermark策略，使用底层DataStream中的Watermark策略也是可以的，这时Flink SQL的Table Source中不做处理

#### 多流Watermark

Flink内部实现每一个边上只能有一个递增的Watermark，每当出现多流携带EventTime汇聚到一起（GroupBy或Union）时，Flink会选择所有流入的EventTime中最小的一个向下游流出，从而保证Watermark的单调递增和数据的完整性。

Watermark是在Source Function中生成或者在后续的DataStream API中生成的。Flink作业一般包含多个Task，每个Task运行一个或一组算子（OperatorChain）实例，Task在生成Watermark的时候是相互独立的，即在作业中存在多个并行的Watermark。Watermark在作业的DAG从上游向下游传递，算子收到上游Watermark后会更新其Watermark，如果新的Watermark大于算子的当前Watermark，则更新算子的Watermark为新Watermark，并发送给下游算子。

对于有多个上游输入的算子（如Union、KeyBy、partition之后的算子），在Flink底层执行模型上，多流输入会被分解为多个双流输入，所以对于多流Watermark的处理也就是双流Watermark的处理。Flink会选择两条流中较小的Watermark，即`Min(input1Watermark, input2Watermark)`，与算子当前的Watermark比较，如果大于算子当前的Watermark，则更新算子的Watermark为新的Watermark，并发送给下游。

```Java
// 双流输入的StreamOperator Watermark处理
// AbstractStreamOperator.java
public void processWatermark1(Watermark mark) throws Exception {
    input1Watermark = mark.getTimestamp();
    long newMin = Math.min(input1Watermark, input2Watermark);
    if (newMin > combinedWatermark) {
    +ombinedWatermark = newMin;
    +rocessWatermark(new Watermark(combinedWatermark));
    }
}
public void processWatermark2(Watermark mark) throws Exception {
    input2Watermark = mark.getTimestamp();
    long newMin = Math.min(input1Watermark, input2Watermark);
    if (newMin > combinedWatermark) {
    +ombinedWatermark = newMin;
    +rocessWatermark(new Watermark(combinedWatermark));
    }
}
```

### 定时器

定时器（实现为InternalTimer接口，InternalTimer实现类为InternalTimerImpl）是Flink提供的用于感知并利用处理时间/事件时间变化的机制。Timer是以Key级别注册的。

定时器服务（实现为TimerService接口和InternalTimerService接口）用来获取当前事件时间（`currentWatermark()`）和处理时间（`currentProcessingTime()`）、注册或删除定时器，仅支持keyed operator，InternalTimerServiceImpl是基于Java堆实现的InternalTimerService，其中使用两个包含TimerHeapInternalTimer的优先队列（KeyGroupedInternalPriorityQueue）分别维护事件时间定时器和处理时间定时器，定时器的注册和删除都是通过优先队列添加或删除元素实现的。AbstractStreamOperator（所有流算子的基类）中的`getInternalTimerService()`方法（最终调用InternalTimeServiceManager（具体实现为InternalTimeServiceManagerImpl）中的getInternalTimerService()方法）用于获取InternalTimerService实例，一个算子可以有多个定时器服务实例，定时器服务由名称区分。

InternalTimeServiceManager用于管理各个InternalTimeService，使用HashMap维护一个键下所有的定时器服务实例（键为定时器服务实例名称），如果使用同一个名称创建多次定时器服务实例，后续都返回第一次创建的实例。

Flink的定时器（Timer）使用InternalTimer接口定义行为，窗口的触发器与定时器是紧密联系的，在InternalTimerServiceImpl中触发Timer然后回调用户逻辑。对于事件时间，会根据Watermark的时间，从事件时间的定时器队列中找到比给定时间小的所有定时器，触发该Timer所在的算子，然后由算子去调用UDF中的`onTimer()`方法。处理时间也是类似的逻辑，区别在于，处理时间是从处理时间Timer优先级队列中找到Timer，处理时间依赖于当前系统，所以其使用的是周期性调度

```Java
public interface TimerService {
    long currentProcessingTime();       // 返回当前处理时间戳
    long currentWatermark();            // 返回当前事件时间戳
    void registerProcessingTimeTimer(long time);       // 注册一个指定处理时间触发的定时器
    void deleteProcessingTimeTimer(long time);         // 删除指定处理时间的定时器
    void registerEventTimeTimer(long time);            // 注册一个指定事件时间触发的定时器
    void deleteEventTimeTimer(long time);              // 注册指定事件时间的定时器
}

public interface InternalTimerService<N> {
    long currentProcessingTime();       // 返回当前处理时间戳
    long currentWatermark();            // 返回当前事件时间戳
    void registerProcessingTimeTimer(N namespace, long time);       // 注册一个指定处理时间触发的定时器
    void deleteProcessingTimeTimer(N namespace, long time);         // 删除指定处理时间的定时器
    void registerEventTimeTimer(N namespace, long time);            // 注册一个指定事件时间触发的定时器
    void deleteEventTimeTimer(N namespace, long time);              // 注册指定事件时间的定时器
    void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer);     // 对每个注册的事件时间定时器执行操作
    void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer);      // 对每个注册的处理时间定时器执行操作
}

class InternalTimerServiceImpl<K, N> implements InternalTimerService<N> {

    ProcessingTimeService processingTimeService;
    KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;
    KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;
    Triggerable<K, N> triggerTarget;

    // 启动InternalTimerServiceImpl
    public void startTimerService(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer, Triggerable<K, N> triggerTarget) {
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.triggerTarget = triggerTarget;
        this.keyDeserializer = null;
        this.namespaceDeserializer = null;
        InternalTimer<K, N> headTimer = processingTimeTimersQueue.peek();
        if (headTimer != null) {
            // 调用ProcessingTimeService
            nextTimer = processingTimeService.registerTimer(headTimer.getTimestamp(), this::onProcessingTime);
        }
    }

    // 注册处理时间定时器
    void registerProcessingTimeTimer(N namespace, long time) {

        InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
        if (processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
            long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
            // check if we need to re-schedule our timer to earlier
            if (time < nextTriggerTime) {
                if (nextTimer != null) {
                    nextTimer.cancel(false);
                }
                nextTimer = processingTimeService.registerTimer(time, this::onProcessingTime);
            }
        }
    }

    // 删除处理时间定时器
    void deleteProcessingTimeTimer(N namespace, long time) {
        processingTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    // 注册事件时间定时器
    void registerEventTimeTimer(N namespace, long time) {
        eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    // 删除事件时间定时器
    void deleteEventTimeTimer(N namespace, long time) {
        eventTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    public void advanceWatermark(long time) {
    currentWatermark = time;
    InternalTimer<K, N> timer;
    while((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
    +ventTimeTimersQueue.poll();
    +eyContext.setCurrentKey(timer.getKey());
    +riggerTarget.onEventTime(timer);
    }
    }
}

// InternalTimer接口
// InternalTimer.java
public interface InternalTimer<K, N> extends PriorityComparable<InternalTimer<?, ?>>, Keyed<K> {
    KeyExtractorFunction<InternalTimer<?, ?>> KEY_EXTRACTOR_FUNCTION = InternalTimer::getKey;
    PriorityComparator<InternalTimer<?, ?>> TIMER_COMPARATOR = (left, right) -> Long.compare(left.getTimestamp(), right.getTimestamp());
    long getTimestamp();
    K getKey();
    N getNamespace();
}

class TimerHeapInternalTimer<K, N> implements InternalTimer<K, N>, HeapPriorityQueueElement {
    K key;

}

// 事件时间触发与回调
// InternalTimerServiceImpl.java

abstract class AbstractStreamOperator<OUT> implements StreamOperator<OUT>, SetupableStreamOperator<OUT>, CheckpointedStreamOperator {

    InternalTimeServiceManager<?> timeServiceManager;

    void initializeState(StreamTaskStateInitializer streamTaskStateManager) {
        StreamOperatorStateContext context = streamTaskStateManager.streamOperatorStateContext(...);
        timeServiceManager = context.internalTimerServiceManager();
    }

    public <K, N> InternalTimerService<N> getInternalTimerService(String name, TypeSerializer<N> namespaceSerializer, Triggerable<K, N> triggerable) {
        InternalTimeServiceManager<K> keyedTimeServiceHandler = (InternalTimeServiceManager<K>) timeServiceManager;
        KeyedStateBackend<K> keyedStateBackend = getKeyedStateBackend();

        /** 最终调用InternalTimeServiceManager（具体实现为InternalTimeServiceManagerImpl）中的getInternalTimerService()方法
          * TimerSerializer<K, N> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
          * InternalTimerServiceImpl<K, N> timerService = (InternalTimerServiceImpl<K, N>) timerServices.get(name);       // timerServices为Map实例保存已经创建的定时器服务
          * if (timerService == null) {
          *     timerService = new InternalTimerServiceImpl<>(localKeyGroupRange, keyContext, processingTimeService, priorityQueueSetFactory.create("_timer_state/processing_" + name, timerSerializer), priorityQueueSetFactory.create("_timer_state/event_" + name, timerSerializer));
          *     timerServices.put(name, timerService);
          * }
          * timerService.startTimerService(timerSerializer.getKeySerializer(), timerSerializer.getNamespaceSerializer(), triggerable);
          * return timerService;
          */

        return keyedTimeServiceHandler.getInternalTimerService(name, keyedStateBackend.getKeySerializer(), namespaceSerializer, triggerable);
    }
}
```

#### 优先级队列

Flink在优先级队列中使用了KeyGroup，是按照KeyGroup去重的，并不是按照全局的Key去重。Flink自己实现了优先级队列来管理Timer，共有两种实现：
+ 基于堆内存的优先级队列HeapPriorityQueueSet：基于Java堆内存的优先级队列，实现思路与Java的PriorityQueue类似，使用了二叉树
+ 基于RocksDB的优先级队列：分为Cache+RocksDB量级，Cache中保存了前N个元素，其余的保存在RocksDB中，写入的时候采用Write-through策略，即写入Cache的同时要更新RocksDB中的数据，可能要访问磁盘。

基于堆内存的优先级队列比基于RocksDB的优先级队列性能好，但是受限于内存大小，无法容纳太多的数据；基于RocksDB的优先级队列牺牲了部分性能，可以容纳大量的数据。

### 窗口实现

```bob-svg
                               .----------------.
                               | WindowAssigner |
                               '--------+-------'
                                        ^
                                        |
                                        |
                              .---------+------.           .---------.        .-----------------.
            ,---------------->| processElement |---------->| Evictor +------->| Window Function +-------+
            |                 '---------+------'           '----+----'        '-----------------'       |
            |                           |                       ^                                       |
            |StreamElement              v                       |                         StreamElement |
            |                  .--------+-------.               |                                       |
            |                  |     Trigger    |               |                                       v
.-----------+---.              '--------+-------'               |                            .----------+----.
|     .--. .--. |                       ^                       |                            |     .--. .--. |
|"..."|  | |  | |                       |                       |                            |.... |  | |  | |
|     '--' '--' |              .--------+-------.       .-------+----------.                 |     '--' '--' |
'------+--------'              |  TimeService   +------>|   "onEventTime"  |                 '-------+-------'
       |                       '--------+-------'       | onProcessingTime |                         ^
       | Watermark                      ^               '------------------'                         |
       |                                |                                                            |
       |                      .---------+--------.                                                   |
       +--------------------->| processWatermark +---------------------------------------------------+
                              '------------------'
```


WindowOperator是窗口具体实现，每个元素进入WindowOperator时，首先会被交给WindowAssigner，WindowAssigner决定元素被放到哪个或哪些窗口（可能会创建新窗口或者合并旧的窗口），然后调用Trigger的`onElement()`方法判断是否触发计算。

另外，WindowOperator中注册了一个定时器，定时器超时时会调用其`onEventTime()`方法和`onProcessingTime()`方法，这两个方法中分别调用Trigger的`onEventTime()`方法和`onProcessingTime()`方法判断是否触发计算。

如果触发计算则调用计算函数处理窗口中的元素（如果是EvictingWindowOperator，会先过滤窗口中的元素），如果清空则清空窗口中的元素。

Flink对一些聚合类的窗口计算做了优化，这些计算不需要将窗口中的所有数据都保存下来，只需要保存一个中间结果值就可以了。每个进入窗口的元素都会执行一次聚合函数并修改中间结果值，这样可以大大降低内存的消耗并提升性能。但是如果是用户定义了Evictor，则不会启用对聚合窗口的优化，因为Evictor需要遍历窗口中的所有元素，必须将窗口中的所有元素都存下来。

```Java
class WindowOperator<K, IN, ACC, OUT, W extends Window> extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>> implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {
    WindowAssigner<? super IN, W> windowAssigner;
    KeySelector<IN, K> keySelector;
    Trigger<? super IN, ? super W> trigger;
    StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;
    long allowedLateness;
    OutputTag<IN> lateDataOutputTag;             // 延迟时间超过allowedLateness的元素将输出到这里
    TypeSerializer<K> keySerializer;             // 检查点中序列化键
    TypeSerializer<W> windowSerializer;          // 检查点中序列化窗口
    InternalWindowFunction<ACC, OUT, K, W> userFunction;      // 计算函数
    InternalTimerService<W> internalTimerService;

    public WindowOperator(WindowAssigner<? super IN, W> windowAssigner, TypeSerializer<W> windowSerializer, KeySelector<IN, K> keySelector, TypeSerializer<K> keySerializer, StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor, InternalWindowFunction<ACC, OUT, K, W> windowFunction, Trigger<? super IN, ? super W> trigger, long allowedLateness, OutputTag<IN> lateDataOutputTag) {
        super(windowFunction);

        this.windowAssigner = windowAssigner;
        this.windowSerializer = windowSerializer;
        this.keySelector = keySelector;
        this.keySerializer = keySerializer;
        this.windowStateDescriptor = windowStateDescriptor;
        this.trigger = trigger;
        this.allowedLateness = allowedLateness;
        this.lateDataOutputTag = lateDataOutputTag;
        this.userFunction = windowFunction;

        setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    public void open() {
        super.open();
        internalTimerService = getInternalTimerService("window-timers", windowSerializer, this);
        windowAssignerContext = new WindowAssigner.WindowAssignerContext() {
            @Override
            public long getCurrentProcessingTime() {
                return internalTimerService.currentProcessingTime();
            }
        };
        // 创建（或恢复）用于保存窗口内容的状态
        if (windowStateDescriptor != null) {
            windowState = (InternalAppendingState<K, W, IN, ACC, ACC>)getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
        }
    }

    public void processElement(StreamRecord<IN> element) {
        // 元素被分到的窗口
        Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp(), windowAssignerContext);
        K key = this.<K>getKeyedStateBackend().getCurrentKey();
        if (windowAssigner instanceof MergingWindowAssigner) {
            ......
        } else {
            for (W window : elementWindows) {
                windowState.setCurrentNamespace(window);
                // 添加元素
                windowState.add(element.getValue());

                triggerContext.key = key;
                triggerContext.window = window;
                TriggerResult triggerResult = triggerContext.onElement(element);
                if (triggerResult.isFire()) {
                    ACC contents = windowState.get();
                    emitWindowContents(actualWindow, contents);
                }
                if (triggerResult.isPurge()) {
                    windowState.clear();
                }
                registerCleanupTimer(actualWindow);
            }
        }
    }

    public void onEventTime(InternalTimer<K, W> timer) {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();
        if (windowAssigner instanceof MergingWindowAssigner) {
            ......
        } else {
            windowState.setCurrentNamespace(triggerContext.window);
        }

        TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            ACC contents = windowState.get();
            emitWindowContents(triggerContext.window, contents);
        }

        if (triggerResult.isPurge()) {
            windowState.clear();
        }

        if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
            clearAllState(triggerContext.window, windowState, mergingWindows);
        }

    }

    public void onProcessingTime(InternalTimer<K, W> timer) {
        triggerContext.key = timer.getKey();
        triggerContext.window = timer.getNamespace();
        if (windowAssigner instanceof MergingWindowAssigner) {
            ......
        } else {
            windowState.setCurrentNamespace(triggerContext.window);
        }

        TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

        if (triggerResult.isFire()) {
            ACC contents = windowState.get();
            emitWindowContents(triggerContext.window, contents);
        }

        if (triggerResult.isPurge()) {
            windowState.clear();
        }

        if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
            clearAllState(triggerContext.window, windowState, mergingWindows);
        }
    }

    // 调用计算函数处理窗口元素
    private void emitWindowContents(W window, ACC contents) throws Exception {
        timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
        processContext.window = window;
        userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
    }

    private void clearAllState(W window, AppendingState<IN, ACC> windowState, MergingWindowSet<W> mergingWindows) {
        windowState.clear();
        triggerContext.clear();
        processContext.window = window;
        processContext.clear();
    }

}
```

## 检查点（Checkpoint）

数据处理容错语义保证的可靠程度从低到高包含4个不同的层次：
+ 至多一次（At-Most-Once）：数据不重复处理，但可能丢失
+ 最少一次（At-Least-Once）：数据可能重复处理，但保证不丢失
+ 引擎内严格一次（Exactly-Once）：在计算引擎内部，数据不丢失、不重复
+ 端到端严格一次（End-to-End Exactly-Once）：从数据读取、引擎处理到写入外部存储的整个过程中，数据不重复、不丢失。端到端严格一次语义需要数据源支持可重放、外部存储支持事务机制、能够进行回滚

Flink基于轻量级分布式快照技术-异步屏障快照（Asynchronous Barrier Snapshots，ABS）算法^[借鉴了Chandy-Lamport算法，在数据源端插入 屏障（Barrier）来替代Chandy-Lamport算法中的Marker，通过控制屏障的同步来实现快照的备份和精确一次（Exactly-Once）语义]-提供了Checkpoint机制来实现容错。分布式快照技术的核心思想是生成分布式数据流和算子状态的一致性快照，并将这些快照作为发生故障时系统回退到的一致性检查点。基于检查点机制，在框架级别支持两阶段提交协议，实现了端到端严格一次的语义保证。

保存点（Savepoint）是基于Flink检查点机制的应用完整快照备份机制，可以在另一个集群或者另一个时间点，从保存的状态中将作业恢复回来，适用于应用升级、集群迁移、Flink集群版本更新等场景。保存点可以视为一个“算子ID->状态”的Map，对于每一个有状态的算子，Key是算子ID，Value是算子状态。

**异步屏障快照算法** 在数据源周期性注入屏障（Barrier）来切分数据流（屏障从不跨越记录、严格按顺序流动来作为两次快照对应记录集合的边界，每个屏障都携带其前记录对应检查点的ID），屏障作为数据流的一部分流经各个算子^[不同检查点对应的多个屏障可以在流中同时出现，即 多个检查点可以同时发生]，当一个算子从其所有输入流中收到了同一快照的屏障时，该算子就会保存自己的状态快照并向下游广播屏障，Sink算子从其所有输入流中收到了同一快照的屏障时，如果是引擎内严格一次处理保证，Sink算子对自己的状态进行快照，然后通知检查点协调器，当所有Sink算子都向检查点协调器汇报成功之后，检查点协调器向所有算子确认本次快照完成，如果是端到端严格一次处理保证，Sink算子对自己的状态进行快照，并预提交事务，再通知检查点协调器，当所有Sink算子都向检查点协调器汇报成功之后，检查点协调器向所有的算子确认本次快照完成，Sink算子提交事务，本次快照完成。当作业发生异常时，从最后一次成功的检查点中恢复状态。屏障生成不需要锁，其随着数据向下流动，也不会打断数据流，与检查点有关的所有操作都可以异步完成，可以对齐，也可以不对齐，算子可以异步快照它们的状态，因此非常轻量。

![barrier图示](./imgs/barrier.png)

**屏障对齐** 当一个算子有多个输入时，为了达到引擎内严格一次、端到端严格一次两种保证语义，必须要屏障对齐。对于有两个上游输入通道的算子，屏障对齐过程如下
1. 开始对齐：算子收到输入通道1中的屏障，输入通道2的屏障尚未到达
2. 对齐：算子继续从输入通道1接收数据，但是并不处理，而是保存在输入缓存（input buffer）中，等待输入通道2的屏障到达^[阻塞输入通道有负面效果，一旦某个输入通道发生延迟，屏障迟迟未到，会导致其它通道全部堵塞，系统吞吐大幅下降]
3. 执行检查点：输入通道2中的屏障到达，此时屏障前的记录已经更新了状态，屏障后的记录还未更新状态，保证了状态的准确性（同时还消去了原生Chandy-Lamport算法中记录输入流状态的步骤），算子开始对其状态进行异步快照（包含快照开始时所有数据源并行流的位置offset和指向每个算子状态的指针），接着向下游广播对应检查点的屏障而无需等待快照执行完毕
4. 继续处理数据：算子进行异步快照后，首先处理输入缓存中的数据，然后再从输入通道中获取数据

![barrier alignment图示](./imgs/barrier_alignment.png)

```
# 伪代码：
# 初始化Operator
upon event (init | input_channels, output_channels, fun, init_state)
do
    state := init_state;
    blocked_inputs := {};
    inputs := input_channels;
    outputs := output_channels;
    udf := fun;
# 收到Barrier的行为
upon event (receive | input, (barrier))
do
    # 将当前input通道加入blocked 集合，并block该通道，此通道的消息处理暂停
    if input != Nil
    then
        blocked_inputs := blocked_inputs ∪ {input};
        trigger (block | input);
    # 如果所有的通道都已经被block，说明所有的barrier都已经收到
    if blocked_inputs = inputs
    then
        blocked_inputs := {};
        # 向所有的outputs发出Barrier
        broadcast (send | outputs, (barrier));
        # 记录本节点当前状态
        trigger (snapshot | state);
        # 解除所有通道的block，继续处理消息
        for each inputs as input
            trigger (unblock | input);
```

**故障恢复** 发生故障时，Flink选择最近完成的检查点k，接着系统重新部署整个分布式数据流，并将检查点k中快照的状态分发给每个算子。数据源被设置成从位置Sk开始读取数据（如对于Apache Kafka，意味着通知消费者从Offset Sk开始拉取数据）。如果使用了增量快照，算子从最近一次全量快照开始，然后应用更新该状态的一连串增量快照。

+ 自动检查点恢复：可以在配置文件中提供全局配置，也可以在代码中为Job特别设定。支持以下重启策略
    + 固定延迟重启策略：配置参数fixed-delay，会尝试一个给定的次数来重启作业，如果超过了最大的重启次数，Job最终将失败，在连续的两次重启尝试之间，重启策略会等待一个固定的时间，默认为Integer.MAX_VALUE次
    + 失败率重启策略：配置参数failure-rate，在作业失败后会重启，但是超过失败率后，作业会最终被认定失败，在两个连续的重启尝试之间，重启策略会等待一个固定的时间
    + 直接失败策略：配置参数None，失败不重启

+ 手动检查点恢复：在启动之时通过设置-s参数指定检查点目录的功能，让新的jobId读取该检查点元文件信息和状态信息，从而达到指定时间节点启动作业的目的
    + 外部检查点：检查点执行完成时，在用户给定的外部持久化存储保护，当作业失败（或取消）时，外部存储的检查点会保留下来，用户在恢复时需要提供用于恢复的作业状态的检查点路径
    + 保存点：用户通过命令触发，由用户手动创建、清理，使用了标准化格式存储，允许作业升级或者配置变更，用户在恢复时需要提供用于恢复的作业状态的保存点路径

从保存点恢复作业需要考虑以下几点：
1. 算子的顺序改变：如果算子对应的UID没变，则可以恢复，如果对应的UID变了则恢复失败
2. 作业中添加了新的算子：如果是无状态算子，则没有影响，可以正常恢复，如果是有状态的算子，跟无状态的算子一样处理
3. 从作业中删除了一个有状态的算子：从保存点恢复时通过在命令中添加--allowNonRestoreState（或 -n）跳过无法恢复的算子

### 非对齐检查点

屏障对齐是阻塞式的，如果作业出现反压，数据流动的速度减慢，屏障到达下游算子的延迟就会变大，进而影响到检查点完成的延时（变大甚至超时失败）。如果反压长久不能得到解决，快照数据与实际数据之间的差距就越来越明显，一旦作业failover，势必丢失较多的处理进度。另一方面，作业恢复后需要重新处理的数据又会积压，加重反压，造成恶性循环。

为了规避风险，Flink 1.11开始支持非对齐检查点（unaligned checkpoint）。非对齐检查点机制取消了屏障对齐，与原生Chandy-Lamport算法更为相似一些（需要由算子来记录输入流的状态）。具体步骤如下：

1. 当输入流中的第一个屏障保存到输入缓冲区（input buffer）中时，算子开始进行Checkpoint
2. 算子通过将屏障添加到输出缓冲区（output buffer）的结尾来立即向下游算子广播屏障
3. 算子标记所有未处理的需要异步存储的记录（第一个屏障所在输入缓冲区中的记录、其他输入流对应屏障前的记录、输出缓冲区中的记录）并创建其状态的快照

算子仅在标记缓冲区、广播屏障、创建快照时短暂停止处理输入。

![stream_unaligning图示](./imgs/stream_unaligning.png)

非对齐Checkpoint故障恢复时，除了开始处理来自上游算子的数据前先恢复未处理的数据，其余都和Checkpoint故障恢复操作相同。

对齐检查点和非对齐检查点区别：
1. 对齐检查点在最后一个屏障到达算子时触发，非对齐检查点在第一个屏障到达算子时就触发
2. 对齐检查点在第一个屏障到最后一个屏障到达的区间内是阻塞的，而非对齐检查点不需要阻塞
3. 对齐检查点能够保持快照N~N + 1之间的边界，但非对齐检查点模糊了这个边界

非对齐检查点适用于容易产生反压且I/O压力较小的场景，对齐检查点的At Least Once方式适用于可以容忍重复数据或者在业务逻辑保证幂等性的场景，且能有效的减少反压。

### 两阶段提交协议

Flink设计实现了一种两阶段提交协议（预提交阶段和提交阶段，依赖于两阶段检查点机制），能够保证端到端严格一次，为满足以下条件的Source/Sink提供端到端严格一次支持：
1. 数据源支持断点读取，即能够记录上次读取的位置，失败之后能够从断点处继续读取
2. 外部存储支持回滚机制或者满足幂等性，回滚机制指当作业失败之后能够将部分写入的结果回滚到写入之前的状态；幂等性指重复写入不会带来错误的结果

**预提交阶段** Sink把要写入外部存储的数据以状态的形式保存到状态后端中，同时以事务的方式将数据写入外部存储。如果在预提交阶段任何一个算子发生异常，导致检查点没有备份到状态后端，所有其他算子的检查点执行也必须被终止，Flink回滚到最近成功完成的检查点

**提交阶段** 预提交阶段完成之后，通知所有的算子，确认检查点已成功完成，然后进入提交阶段，JobMaster为作业中每个算子发起检查点已完成的回调逻辑。在预提交阶段，数据实际上已经写入外部存储，但是因为事务原因是不可读的，所有的Sink算子提交成功之后，一旦预提交完成，必须确保提交外部事务也要成功。如果提交外部事务失败，Flink应用就会崩溃，然后根据用户重启策略进行回滚，回滚到预提交时的状态，之后再次重试提交

Flink抽取两阶段提交协议公共逻辑封装进TwoPhaseCommitSinkFunction抽象类，该类继承了CheckpointedFunction接口（在预提交阶段，能够通过检查点将待写出的数据可靠地存储起来）和CheckpointListener接口（在提交阶段，能够接收JobMaster的确认通知，触发提交外部事务）。

以基于文件的Sink为例，若要实现端到端严格一次，最重要的是以下4种方法：
1. beginTransaction：开启一个事务，在临时目录下创建一个临时文件，之后写入数据到该文件中，此过程为不同的事务创建隔离，避免数据混淆
2. preCommit：在预提交阶段，将缓存数据块写出到创建的临时文件，然后关闭该文件，确保不再写入新数据到该文件，同时开启一个新事务，执行属于下一个检查点的写入操作，此过程用于准备需要提交的数据，并且将不同事务的数据隔离开来
3. commit：在提交阶段，以原子操作的方式将上一阶段的文件写入真正的文件目录下。如果提交失败，Flink应用会重启，并调用TwoPhaseCommitSinkFunction的recoverAndCommit()方法尝试恢复并重新提交事务^[两阶段提交可能会导致数据输出的延迟，即需要等待JobMaster确认检查点完成才会写入真正的文件目录]
4. abort：一旦终止事务，删除临时文件

### 检查点执行过程

```bob-svg
                                    .---------------------.
           trigger Checkpoint       |     JobMaster       |
      +-----------------------------+   .-------------.   |
      |                             |   | Checkpoint  |   |
      |     +---------------------->+   | Coordinator |   +<-------------------------+
      |     |   report "succ/fail"  |   '-------------'   |    report "succ/fail"    |
      |     |                       '----------+----------'                          |
      |     |                                  ^                                     |
      v     |                                  | report "succ/fail"                  |
 .----+-----+----.                             |                              .------+--------.
 |  StreamSink   |                     .-------+--------.                     |  StreamSink   |
 |               |-------------------->+ StreamOperator +-------------------->+               |
 | Sink Operator | Checkpoint Barrier  '----------------' Checkpoint Barrier  | Sink Operator |
 '---------------'                                                            '---------------'
```

JobMaster中的检查点协调器（CheckpointCoordinator）组件负责检查点的管理，包括触发检查点、确认检查点完成。触发检查点时，CheckpointCoordinator向Source算子注入Barrier消息，然后等待所有的Task通知检查点确认完成（检查点协调器持有所有Task在确认完成消息中上报的状态句柄）。检查点的具体执行者则是作业的各个Task，各个Task再将检查点的执行交给算子，算子是最底层的执行者。

在执行检查点的过程中，TaskManager和JobManager之间通过消息确认检查点执行成功还是取消，Flink中设计了检查点消息类体系，检查点消息中有3个重要信息，该检查点所属的作业标识（JobID）、检查点编号、Task标识（ExecutionAttemptID）
+ AcknowledgeCheckpoint消息：从TaskExecutor发往JobMaster，告知算子的快照备份完成
+ DeclineCheckpoint消息：从TaskExecutor发送JobMaster，告知算子无法执行快照备份

检查点执行具体过程：
1. JobMaster触发检查点：CheckpointCoordinator通知执行数据读取的SourceTask产生CheckpointBarrier事件并注入数据流中
    1. 前置检查：
        1. 未启用Checkpoint、作业关闭过程中或尚未达到触发检查点的最小间隔时都不允许执行
        2. 检查是否所有需要执行检查点的Task都处于执行状态，能够执行检查点和向JobMaster汇报
        3. 执行CheckpointID = CheckpointIdCounter.getAndIncrement()，生成一个新的id，然后生成一个PendingCheckpoint^[PendingCheckpoint是一个启动了的检查点，但是还没有被确认，等到所有的Task都确认了本次检查点，那么这个检查点对象将转化为一个CompletedCheckpoint]
        4. 检查点执行超时时取消本次检查点
        5. 触发MasterHooks^[用户可以定义一些额外的操作，用以增强检查点的功能]
        6. 再次执行步骤1和2中的检查，如果一切正常，则向各个SourceStreamTask发送通知，触发检查点执行
    2. 向Task发送触发检查点消息：Execution表示一次ExecutionVertex的执行，对应于Task实例，在JobMaster端通过Execution的Slot可以找到对应的TaskManagerGateway，远程触发Task的检查点
2. 各算子执行检查点并汇报：JobMaster通过TaskManagerGateway触发TaskManager执行检查点，TaskManager则转交给Task执行
    1. Task层面的检查点执行准备：Task类中的CheckpointMetaData对象确保Task处于Running状态，把工作转交给StreamTask，而StreamTask也转交给更具体的类，直到最终执行用户编写的函数
    2. StreamTask执行检查点：首先在OperatorChain上执行准备CheckpointBarrier的工作，然后向下游所有Task广播CheckpointBarrier，最后触发自己的检查点^[这样做可以尽快将CheckpointBarrier广播到下游，避免影响下游CheckpointBarrier对齐，降低整个检查点执行过程的耗时]
    3. 算子生成快照：在StreamTask中经过一系列简单调用之后，异步触发OperatorChain中所有算子的检查点。算子开始从StateBackend中深度复制状态数据，并持久化到外部存储中。注册回调，执行完检查点后向JobMaster发出CompletedCheckpoint消息
    4. 算子保存快照与状态持久化：触发保存快照的动作之后，首先对OperatorState和KeyState分别进行处理，如果是异步的，则将状态写入外部存储。持久化策略负责将状态写入目标存储
    5. Task报告检查点完成：当一个算子完成其状态的持久化之后，就会向JobMaster发送检查点完成消息，具体逻辑在reportCompletedSnapshotStates中，该方法又把任务委托给了RpcCheckpointResponder类。在向JobMaster汇报的消息中，TaskStateSnapshot中保存了本次检查点的状态数据^[内存型StateBackend中保存的是真实的状态数据，文件型StateBackend中保存的是状态的句柄]，在分布式文件系统中的保存路径也是通过TaskStateSnapshot中保存的信息恢复回来的。状态的句柄分为OperatorStateHandle和KeyedStateHandle，分别对应OperatorState和KeyState，同时也区分了原始状态和托管状态
3. JobMaster确认检查点完成：JobMaster通过调度器SchedulerNG任务把消息交给CheckpointCoordinator的receiveAcknowledgeMessage()方法来响应算子检查点完成事件。CheckpointCoordinator在触发检查点时，会生成一个PendingCheckpoint，保存所有算子的ID。当PendingCheckpoint收到一个算子的完成检查点的消息时，就把这个算子从未完成检查点的节点集合移动到已完成的集合。当所有的算子都报告完成了检查点时，CheckpointCoordinator会触发`completePendingCheckpoint()`方法，该方法做了以下事情：
    1. 把pendingCgCheckpoint转换为CompletedCheckpoint
    2. 把CompletedCheckpoint加入已完成的检查点集合，并从未完成检查点集合删除该检查点，CompletedCheckpoint中保存了状态的句柄、状态的存储路径、元信息的句柄等信息
    3. 向各个算子发出RPC请求，通知该检查点已完成

```Java
// CheckpointCoordinator触发检查点
// CheckpointCoordinator.java
public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(long timestamp, CheckpointProperties props, String externalSavepointLocation, boolean isPeriodic, boolean advanceToEndOfTime) {
    ......
    long checkpointID = checkpointIdCounter.getAndIncrement();
    // 通知作业的各个SourceTask触发Checkpoint
    for (Execution execution : executions) {
        execution.triggerCheckpoint(checkpointID, timestamp, checkpointOptions);  // 内部调用Execution的triggerCheckpointHelper()方法
    }
    ......
}

// 通知Task执行检查点
// Execution.java
private void triggerCheckpointHelper(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
    LogicalSlot slot = assignedResource;
    if (slot != null) {
        taskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
        taskManagerGateway.triggerCheckpoint(attemptId, getVertex().getJobId(), checkpointId, timestamp, checkpointOptions, advanceToEndOfEventTime);        // 最终调用Task的triggerCheckpointBarrier方法
    }
}

// Task处理检查点
// Task.java
public void triggerCheckpointBarrier(final long checkpointID, final long checkpointTimestamp, final CheckpointOptions checkpointOptions, final boolean advanceToEndOfEventtime) {
    AbstractInvokable invokable = this.invokable;
    CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointID, checkpointTimestamp);
    invokable.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
}

// StreamTask触发Checkpoint
// StreamTask.java
public Future<Boolean> triggerCheckpointAsync(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
    return mailboxProcessor.getMainMailboxExecutor().submit(() -> triggerCheckpoint(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime), "checkpoint %s with %s", checkpointMetaData, checkpointOptions);
}

// StreamTask执行检查点
// StreamTask.java
private boolean performCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics, boolean advanceToEndOfTime) throws Exception {
    ......
    long checkpointId = checkpointMetaData.getCheckpointId();
    // 下面的3个步骤，在实现时采用的是异步方式，后续步骤并不等待前边的操作完成
    // operatorChain为这个任务执行的算子链
    operatorChain.prepareSnapshotPreBarrier(checkpointId);
    /** 向下游发送Barrier
      * public void broadcastCheckpointBarrier(long id, long timestamp, CheckpointOptions checkpointOptions) {
      *     CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, checkpointOptions);
      *     for (RecordWriterOutput<?> streamOutput : streamOutputs) {
      *         streamOutput.broadcastEvent(barrier);
      *     }
      * }
      */
    operatorChain.broadcastCheckpointBarrier(checkpointId, checkpointMetaData.getTimestamp(), checkpointOptions);
    // 快照状态
    // 内部创建CheckpointingOperation实例并调用其executeCheckpointing()方法执行检查点
    checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);
    ......
}

// StreamTask中异步触发算子检查点
// SteamTask.java
class CheckpointingOperation {

    public void executeCheckpointing() {
        ......
        for (StreamOperator<?> op : allOperators) {
            /** 内部调用StreamOperator的snapshotState方法
              *
              * snapshotState(snapshotContext);      // 持久化原始State
              * snapshotInProgress.setOperatorStateManagedFuture(operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));  // 持久化OperatorState
              * snapshotInProgress.setkeyedStateManagedFuture(keyedStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));         // 持久化KeyedState
              */
            checkpointStreamOperator(op);
        }

         // 启动线程执行Checkpoint并注册清理行为
         AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(owner, operatorSnapshotsInProgress, checkpointMetaData, checkpointMetrics, startAsyncPartNano);
         owner.cancelables.registerCloseable(asyncCheckpointRunnable);
         / 注册runnable，执行完checkpoint之后，向JM发出CompletedCheckpoint
         owner.asyncOperationsThreadPool.submit(asyncCheckpointRunnable);
    }
}

// 汇报检查点完成
// RpcCheckpointResponder.java
public void acknowledgeCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
    // 向CheckpointCoordinator发送消息报告Checkpoint完成
    checkpointCoordinatorGateway.acknowledgeCheckpoint(jobID, executionAttemptID, checkpointId, checkpointMetrics, subtaskState);
}

// 确认检查点完成
// CheckpointCoordinator.java
// 响应消息，判断checkpoint是否完成或者丢弃
public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message, String taskManagerLocationInfo) {
    ......
    switch (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getSubtaskState(), message.getCheckpointMetrics())) {
        case SUCCESS:
            if (checkpoint.isFullyAcknowledged()) {
                completePendingCheckpoint(checkpoint);
            }
        break;
        .....
    }
}

// 向Task发送检查点完成消息
private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws CheckpointException {
    ......
    for (ExecutionVertex ev : tasksToCommitTo) {
        Execution ee = ev.getCurrentExecutionAttempt();
        if (ee != null) {
            ee.notifyCheckpointComplete(checkpointId, timestamp);
        }
    }
}
```

### 检查点恢复过程

作业状态以算子为单元进行恢复，包括OperatorState恢复、KeyedState恢复、函数State恢复。

```Java
// OperatorState恢复
// 在初始化算子状态时，从OperatorStateStore中获取ListState类型的状态，由OperatorStateStore负责从对应的StateBackend中读取状态重新赋予算子中的状态变量
// 异步算子恢复状态示例
// AsyncWaitOperator中有一个名为"_async_wait_operator_state_"的状态，在算子初始化状态时，对其进行了恢复
// AsyncWaitOperator.java
public void initializeState(StateInitializationContext context) {
    super.initializeState(context);
    recoveredStreamElements = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
}

// KeyedState恢复
// WindowOperator恢复KeyedState，WindowOperator使用了KeyedState在StateBackend中保存窗口数据、定时器等
public void open() {
    super.open();
    ......
    if (windowStateDescriptor != null) {
        windowState = (InternalAppendingState<K, W, IN, ACC, ACC>)getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
    }
}
// 函数State恢复
// 主要是针对有状态函数（继承了CheckpointedFunction接口或ListCheckpointed接口），在Flink内置的有状态函数主要是Source、Sink函数，为了支持端到端严格一次的情况，Source函数需要能够保存数据读取的断点位置，作业故障恢复后能够从断点位置开始读取。在Sink写出数据到外部存储时，有时也会需要恢复状态（如BucketingSink和TwoPhaseCommitSinkFunction）
// StreamingFunctionUtils.java
private static boolean tryRestoreFunction(StateInitializationContext context, Function userFunction) {
    if (userFunction instanceof CheckpointedFunction) {
        ((CheckpointedFunction) userFunction).initializeState(context);
        return true;
    }
    ......
}
```

### 检查点监控

Flink的Web界面提供了以下标签页来监视作业的 checkpoint 信息：
+ 概览（Overview）：列出了以下统计信息，这些统计信息在 JobManager 丢失时无法保存，如果 JobManager 发生故障转移，这些统计信息将重置
    + Checkpoint Counts：包括Triggered（自作业开始以来触发的 checkpoint 总数）、In Progress（当前正在进行的 checkpoint 数量）、Completed（自作业开始以来成功完成的 checkpoint 总数）、Failed（自作业开始以来失败的 checkpoint 总数）、Restored（自作业开始以来进行的恢复操作的次数）
    + Latest Completed Checkpoint：最近成功完成的checkpoint
    + Latest Failed Checkpoint：最近失败的checkpoint
    + Latest Savepoint：最近触发的 savepoint 及其外部路径
    + Latest Restore：最近恢复操作信息（ID、Restore Time、Type、Path），有Restore from Checkpoint（从Checkpoint恢复）和Restore from Savepoint（从Savepoint恢复）两种类型
+ 历史记录（History）：保存有关最近触发的 checkpoint 的统计信息，包括当前正在进行的 checkpoint。配置项web.checkpoints.history设置历史记录所保存的最近检查点的数量（默认是10）
    + ID：checkpoint的ID
    + Status：Checkpoint 的当前状态，可以是正在进行（In Progress）、已完成（Completed） 或失败（Failed））
    + Acknowledged：已确认完成的子任务数量与总任务数量
    + Trigger Time：在 JobManager 上发起 checkpoint 的时间
    + Latest Acknowledgement：JobManager 接收到任何 subtask 的最新确认的时间
    + End to End Duration：从触发时间戳到最后一次确认的持续时间，完整 checkpoint 的端到端持续时间由确认 checkpoint 的最后一个 subtask 确定。这个时间通常大于单个 subtask 实际 checkpoint state 所需的时间
    + Checkpointed Data Size：在此次checkpoint的sync以及async阶段中持久化的数据量。如果启用了增量 checkpoint或者changelog，则此值可能会与全量checkpoint数据量产生区别
    + Full Checkpoint Data Size：所有已确认的 subtask 的 checkpoint 的全量数据大小
    + Processed (persisted) in-flight data：在 checkpoint 对齐期间（从接收第一个和最后一个 checkpoint barrier 之间的时间）所有已确认的 subtask 处理/持久化 的大约字节数。如果启用了 unaligned checkpoint，持久化的字节数可能会大于0
+ 摘要信息（Summary）：计算了所有已完成 checkpoint 的端到端持续时间、增量/全量Checkpoint 数据大小和 checkpoint alignment 期间缓冲的字节数的简单 min / average / maximum 统计信息
+ 配置信息（Configuration）：列出了指定的配置
    + Checkpointing Mode：恰好一次（Exactly Once）或者至少一次（At least Once）
    + Interval：配置的 checkpoint 触发间隔
    + Timeout：超时时间，超时之后，JobManager 取消 checkpoint 并触发新的 checkpoint
    + Minimum Pause Between Checkpoints：Checkpoint 之间所需的最小暂停时间（Checkpoint 成功完成后，至少要等这段时间再触发下一个，这可能会延迟正常的间隔）
    + Maximum Concurrent Checkpoints：可以同时进行的最大 checkpoint 个数
    + Persist Checkpoints Externally：启用或禁用持久化 checkpoint 到外部系统

Checkpoint详细信息：点击某个 checkpoint 的 More details 链接时，将获得其所有 operator 的 Minimum/Average/Maximum 摘要信息，以及每个 subtask 单独的详细量化信息
+ Sync Duration：Checkpoint 同步部分的持续时间，包括 operator 的快照状态，并阻塞 subtask 上的所有其他活动（处理记录、触发计时器等）
+ Async Duration：Checkpoint 异步部分的持续时间，包括将 checkpoint 写入设置的文件系统所需的时间。unaligned checkpoint还包括 subtask 必须等待最后一个 checkpoint barrier 到达的时间（checkpoint alignment 持续时间）以及持久化数据所需的时间
+ Alignment Duration：处理第一个和最后一个 checkpoint barrier 之间的时间。对于 checkpoint alignment 机制的 checkpoint，在 checkpoint alignment 过程中，已经接收到 checkpoint barrier 的 channel 将阻塞并停止处理后续的数据
+ Start Delay：从 checkpoint barrier 创建开始到 subtask 收到第一个 checkpoint barrier 所用的时间
Unaligned Checkpoint：Checkpoint 完成的时候是否是一个 unaligned checkpoint。在 alignment 超时的时候 aligned checkpoint 可以自动切换成 unaligned checkpoint

### 检查点配置

检查点目录结构：
```
/{state.checkpoints.dir}
    /{job-id}
        |
        + --shared/         // 多个检查点共用的状态
        + --taskowned/      // JobManager不能删除的状态
        + --chk-1/          // 某个检查点专有状态
        + --chk-2/
        + --chk-3/
        ...   
```

检查点配置：
```Java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Checkpoint默认禁用，通过调用StreamExecutionEnvironment的enableCheckpointing()方法启用，
env.enableCheckpointing(n);      // n为checkpoint间隔（ms）

// 设置检查点路径
env.getCheckpointConfig().setCheckpointStorage(checkpointPath);

// 设置检查点模式，精确一次：CheckpointingMode.EXACTLY_ONCE，CheckpointingMode.AT_LEAST_ONCE
// 对于大多数应用来说，精确一次是较好的选择。至少一次可能与某些延迟超低（始终只有几毫秒）的应用的关联较大
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// 设置Checkpoint之间的最小时间
// 定义在checkpoint之间需要多久的时间，以确保流应用在checkpoint之间有足够的进展
// 这个值也意味着并发checkpoint的数目是一
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(ms);

// 设置checkpoint超时
// 如果checkpoint执行的时间超过了该配置的阈值，还在进行中的checkpoint操作就会被抛弃
env.getCheckpointConfig().setCheckpointTimeout(ms);

// checkpoint可容忍连续失败次数
// 定义可容忍多少次连续的checkpoint失败，超过这个阈值之后会触发作业错误（fail over）。默认次数为“0”，即不容忍checkpoint失败，作业将在第一次checkpoint失败时fail over。可容忍的checkpoint失败仅适用于下列情形：Job Manager的IOException，TaskManager做checkpoint时异步部分的失败， checkpoint超时等。TaskManager做checkpoint时同步部分的失败会直接触发作业fail over。其它的checkpoint失败（如一个checkpoint被另一个checkpoint包含）会被忽略掉
env.getCheckpointConfig().setTolerableCheckpointFailureNumber(times);

// 并发checkpoint数目
// 默认情况下，在上一个checkpoint未完成（失败或者成功）的情况下，系统不会触发另一个checkpoint
// 不能和 checkpoint间的最小时间 同时使用
env.getCheckpointConfig().setMaxConcurrentCheckpoints(n);

// 周期存储checkpoint到外部系统中
// Externalized checkpoint将它们的元数据写到持久化存储上并且在job失败的时候不会被自动删除
// ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：job取消时保留检查点
// ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：job取消时删除检查点
env.getCheckpointConfig().setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// 开启非对齐检查点
env.getCheckpointConfig().enableUnalignedCheckpoints();
```
