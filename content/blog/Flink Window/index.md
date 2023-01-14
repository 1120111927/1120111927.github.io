---
title: Flink时间与窗口
date: "2023-01-12"
description: Flink中有三种时间类型，包括事件时间、处理时间、摄取时间，不同的时间有其各自的适用场景。窗口是流上的重要概念，在Flink有计数窗口、时间窗口、会话窗口三大类。Watermark机制基于时间，使用窗口进行数据流切分，按照窗口进行计算，触发窗口统计。在Flink内部，算子的Watermark会选择来自上游的多个算子中最小的Watermark作为其当前的Watermark并向下游广播。时间服务是实现窗口的重要基石，其与窗口、Watermark机制协同配合，共同实现了流上的窗口运算。窗口和时间服务同时依赖于Flink的状态机制，支持可靠的容错
tags: 事件时间、处理时间、窗口、Watermark
---

Lambda架构的基本思想是，在批处理器旁边运行一个流处理系统，它们都执行基本相同的计算。流式处理系统提供低延迟、不准确的结果，每过一段时间，批处理系统持续滚动处理并计算出正确的结果，修正流处理系统的计算结果。

设计良好的流处理系统实际上是批处理的功能超集，流处理取代Lambda架构需要解决两个问题：
1. 正确性：流计算需要与批处理一样计算准确，强一致性是正确处理的前提^[流处理系统中实现强一致性参考MillWheel: Fault-Tolerant Stream Processing at Internet Scale和Discretized Streams: Fault-Tolerant Streaming Computation at Scale]
2. 时间推理工具：批流统一的关键，对于乱序无界的数据流，数据产生的时间和数据真正被处理的时间之间的偏差很大，用于推理时间的工具至关重要。批处理本质是处理有限不变的数据集，流处理本质是处理无限持续产生的数据集，窗口就是流和批处理的桥梁，对流上的数据进行窗口切分，每一个窗口一旦到了计算的时刻，就可以被看成一个不可变的数据集，在触发计算之前，窗口的数据可能会持续地改变，因此对窗口的数据进行计算就是批处理^[参考The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing]。

Flink中有三种时间类型，包括事件时间、处理时间、摄取时间，不同的时间有其各自的适用场景。窗口是流上的重要概念，在Flink有计数窗口、时间窗口、会话窗口三大类。Watermark机制基于时间，使用窗口进行数据流切分，按照窗口进行计算，触发窗口统计。在Flink内部，算子的Watermark会选择来自上游的多个算子中最小的Watermark作为其当前的Watermark并向下游广播。时间服务是实现窗口的重要基石，其与窗口、Watermark机制协同配合，共同实现了流上的窗口运算。窗口和时间服务同时依赖于Flink的状态机制，支持可靠的容错。

在Flink中定义了3种时间类型：
+ 事件时间（Event Time）：指事件发生时的时间，通过事件时间能够还原出来事件发生的顺序。事件时间一旦确定不会改变，使用事件时间不依赖操作系统的时钟，无论执行多少次，可以保证计算结果是一样的
+ 处理时间（Processing Time）：指消息被计算引擎处理的时间，以各个计算节点的本地时间为准。处理时间是不断变化的，使用处理时间依赖于操作系统的时钟，重复执行基于窗口的统计作业，结果可能是不同的
+ 摄取时间（Ingestion Time）：指事件进入流处理系统的事件

窗口用于切分数据集，在Flink中提供了3种默认窗口，其执行时的算子是WindowOperator：
+ 计数窗口（Count Window）：
    + 滚动计数窗口（Tumble Count Window）：累积固定个数的元素就视为一个窗口
    + 滑动计数窗口（Sliding Count Window）：每新增滑动步长指定个数的元素，就生成一个包含窗口大小指定个数元素的窗口
+ 时间窗口（Time Window）：
    + 滚动时间窗口（Tumble Time Window）：表示在时间上按照指定窗口大小切分的窗口，窗口之间不会相互重叠
    + 滑动时间窗口（Sliding Time Window）：表示在时间上按照指定窗口大小、滑动步长切分的窗口，滑动窗口之间可能会存在相互重叠的情况
+ 会话窗口（Session Window）：每当超过一段时间，该窗口没有收到新的数据元素，则视为窗口结束，无法事先确定窗口的长度、元素个数，窗口之间不会相互重叠

## 窗口原理与机制

窗口算子负责处理窗口，数据流源源不断地进入算子，每一个数据元素进入算子时，首先会被交给WindowAssigner。WindowAssigner决定元素被放到哪个或哪些窗口，在这个窗口中可能会创建新窗口或者合并旧的窗口。在Window Operator中可能同时存在多个窗口，一个元素可以被放入多个窗口中。

Window本身只是一个ID标识符^[其内部可能存储了一些元数据，如TimeWindow中有开始和结束时间]，并不会存储窗口中的元素，窗口中的元素实际存储在Key/Value状态中，Key为Window，Value为数据集合（或聚合值）。为了保证窗口的容错性，Window实现依赖Flink的状态机制。

每一个Window都拥有一个属于自己的Trigger，Trigger上有定时器，用来决定一个窗口何时能够被计算或清除。每当有元素被分配到该窗口，或者之前注册的定时器超时时，Trigger都会被调用。

Trigger被触发后，窗口中的元素集合就会交给Evictor。Evictor主要用来遍历窗口中的元素列表，并决定最先进入窗口的多少个元素需要被移除。剩余的元素会交给用户指定的函数进行窗口的计算。如果没有Evictor的话，窗口中的所有元素会一起交给函数进行计算。

计算函数收到窗口的元素，计算出窗口的结果值，并发送给下游。窗口的结果值可以是一个也可以是多个。DataStream API上可以接收不同类型的计算函数，包括预定义的`sum()`、`min()`、`max()`，以及`ReduceFunction`、`FoldFunction`和`WindowFunction`。`WindowFunction`是最通用的计算函数，其他的预定义函数基本上都是基于该函数实现的。

Flink对一些聚合类的窗口计算做了优化，这些计算不需要将窗口中的所有数据都保存下来，只需要保存一个中间结果值就可以了。每个进入窗口的元素都会执行一次聚合函数并修改中间结果值，这样可以大大降低内存的消耗并提升性能。但是如果是用户定义了Evictor，则不会启用对聚合窗口的优化，因为Evictor需要遍历窗口中的所有元素，必须将窗口中的所有元素都存下来

**WindowAssigner** 用来决定某个元素被分配到哪个/哪些窗口中去

**WindowTrigger**

**WindowEvictor** 用于在Window函数执行前或后，从Window中过滤元素，可以理解为窗口数据的过滤器，Flink内置了3中窗口数据过滤器
+ CountEvictor：计数过滤器，在Window中保留指定数量的元素，并从窗口头部开始丢弃其余元素
+ DeltaEvictor：阈值过滤器，计算窗口中每个数据记录，然后与一个事先定义好的阈值做比较，丢弃超过阈值的数据记录
+ TimeEvictor：时间过滤器，保留Window中最近一段时间内的元素，并丢弃其余元素

**Window函数** 数据经过WindowAssigner后，被分配到不同的Window中，接下来通过窗口函数对窗口内的数据进行处理，窗口函数主要分为两种
+ 增量计算函数：指窗口中保存一份中间数据，每流入一个新元素，新元素都会与中间数据两两合一，生成新的中间数据，再保存到窗口中，如ReduceFunction、AggregateFunction、FoldFunction。增量计算的优点是数据到达后立即计算，窗口只保存中间结果，计算效率高，但是无法满足特殊业务需求
+ 全量计算函数：指先缓存该窗口中的所有元素，等到触发条件后对窗口内的所有元素执行计算，Flink内置的ProcessWindowFunction就是全量计算函数。全量计算通过全量缓存，实现灵活计算，但是计算效率稍低

## 水印

水印（Watermark）用于处理乱序事件，正确地处理乱序事件，通常用Watermark机制结合窗口来实现。水印保证特定的时间后一定会触发窗口进行计算，避免无限期等待延迟太久的数据。

### DataStream Watermark生成

通常Watermark在Source Function中生成，如果是并行计算的任务，在多个并行执行的Source Function中，相互独立产生各自的Watermark。Flink提供了额外的机制，允许在调用DataStream API操作之后，根据业务逻辑的需要，使用时间戳和Watermark生成器修改数据记录的时间戳和Watermark。

Source Function通过`SourceContext#collectWithTimestamp()`和`SourceContext#emitWatermark()`直接为数据元素分配时间戳，同时也会向下游发送Watermark。

```Java
// SourceFunction中为数据元素分配时间戳和生成Watermark
@Override
public void run(SourceContext<MyType> ctx) throws Exception {
    while (/* condition */) {
        MyType next = getNext();
        ctx.collectWithTimestamp(next, next.getEventTimestamp());
        // 生成Watermark并发送给下游
        if (next.hasWatermarkTime()) {
            ctx.emitWatermark(new Watermark(next.getWatermarkTime()));
        }
    }
}
```

DataStream API中使用TimestampAssigner接口定义了时间戳的提取行为，包括AssignerWithPeriodicWatermarks和AssignerWithPunctuatedWatermarks，分别代表不同的Watermark生成策略。AssignerWithPeriodicWatermarks是周期性生成Watermark策略的顶层抽象接口，该接口的实现类周期性地生成Watermark，而不会针对每一个事件都生成。AssignerWithPunctuatedWatermarks对每一个事件都会尝试进行Watermark生成，但是如果生成的Watermark是null或Watermark小于之前的Watermark，则该Watermark不会发往下游^[发往下游也不会有任何效果，不会触发任何窗口的执行]。

### Flink SQL Watermark生成

Flink SQL Watermark主要是在TableSource中生成的，其定义了3类生成策略：
+ PeridocWatermarksAssigner：周期性（一定时间间隔或达到一定的记录条数）地产生一个Watermark
    + AscendingTimestamps：递增Watermark，作用在Flink SQL中的Rowtime属性上，Watermark = 当前收到的数据元素的最大时间戳 - 1^[减1是为了确保有最大时间戳的事件不会被当作迟到数据丢弃]
    + BoundedOutOfOrderTimestamp：固定延迟Watermark，作用在Flink SQL的Rowtime属性上，Watermark = 当前收到的数据元素的最大时间戳-固定延迟
+ PuntuatedWatermarkAssigner：数据流中每一个递增的EventTime都会产生一个Watermark^[实际生产中Punctuated方式在TPS很高的场景下会产生大量的Watermark，在一定程度上会对下游算子造成压力，所以只有在实时性要求非常高的场景下才会选择Punctuated的方式进行Watermark的生成]
+ PreserveWatermak：用于DataStream API和Table & SQL混合编程，此时，Flink SQL中不设定Watermark策略，使用底层DataStream中的Watermark策略也是可以的，这时Flink SQL的Table Source中不做处理

### 多流Watermark

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
        combinedWatermark = newMin;
        processWatermark(new Watermark(combinedWatermark));
    }
}
public void processWatermark2(Watermark mark) throws Exception {
    input2Watermark = mark.getTimestamp();
    long newMin = Math.min(input1Watermark, input2Watermark);
    if (newMin > combinedWatermark) {
        combinedWatermark = newMin;
        processWatermark(new Watermark(combinedWatermark));
    }
}
```

## 时间服务

在开发层面上，会在KeyedProcessFunction中和Window中使用到时间概念。一般情况下，在`KeyedProcessFunction#processElement()`方法中会用到Timer，注册Timer后重写其`onTimer()`方法，在Watermark超过Timer的时间点之后，触发回调`onTimer()`，根据时间类型的不同，可以注册事件时间和处理时间两种Timer。

在执行层面上，在算子中使用时间服务（TimerService）来创建定时器（Timer），并且在Timer触发的时候进行回调，从而进行业务逻辑处理。

### 定时器服务

定时器服务（TimerService）在算子中提供定时器的管理行为，包括定时器的注册和删除。窗口算子（WindowOperator）中使用了InternalTimerService来管理定时器（Timer），其初始化是在`WindowOperator#open()`中实现的。对于InternalService，有几个元素比较重要：名称、命名空间类型N（及其序列化器）、键类型K（及其序列化器）和Triggerable对象（支持延时计算的算子，继承了Triggerable接口来实现回调）。一个算子中可以有多个InternalTimeService，通过名称进行区分^[在WindowOperator算子中是window-timers，在KeyedProcessOpreator中是user-timers，在CepOperator中是watermark-callbacks]。InternalTimerService接口的实现类是InternalTimerServiceImpl，Timer的实现类是InternalTimer。InternalTimerServiceImpl使用了两个TimerHeapInternalTimer的优先队列（HeapPriorityQueueSet），分别用于维护事件时间和处理时间的Timer。InternalTimeServiceManager是Task级别提供的InternalTimeService集中管理器，其使用Map保存了当前所有的InternalTimeService，Map的Key是InternalTimerService的名字。

```Java
// 注册Timer入口
// 注册处理时间Timer方法
KeyedProcessFunction#Context#timeService().registerProcessingTimeTimer(long time)
// 注册事件时间Timer方法
KeyedProcessFunction#Context#timeService().registerEventTimeTimer(long time)
```

### 定时器

Flink的定时器（Timer）使用InternalTimer接口定义行为，窗口的触发器与定时器是紧密联系的，在InternalTimerServiceImpl中触发Timer然后回调用户逻辑。对于事件时间，会根据Watermark的时间，从事件时间的定时器队列中找到比给定时间小的所有定时器，触发该Timer所在的算子，然后由算子去调用UDF中的`onTimer()`方法。处理时间也是类似的逻辑，区别在于，处理时间是从处理时间Timer优先级队列中找到Timer，处理时间依赖于当前系统，所以其使用的是周期性调度

```Java
// InternalTimer接口
// InternalTimer.java
public interface InternalTimer<K, N> extends PriorityComparable<InternalTimer<?, ?>>, Keyed<K> {
    KeyExtractorFunction<InternalTimer<?, ?>> KEY_EXTRACTOR_FUNCTION = InternalTimer::getKey;
    PriorityComparator<InternalTimer<?, ?>> TIMER_COMPARATOR = (left, right) -> Long.compare(left.getTimestamp(), right.getTimestamp());
    long getTimestamp();
    K getKey();
    N getNamespace();
}

// 事件时间触发与回调
// InternalTimerServiceImpl.java
public void advanceWatermark(long time) throws Exception {
    currentWatermark = time;
    InternalTimer<K, N> timer;
    while((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
        eventTimeTimersQueue.poll();
        keyContext.setCurrentKey(timer.getKey());
        triggerTarget.onEventTime(timer);
    }
}
```

### 优先级队列

Flink在优先级队列中使用了KeyGroup，是按照KeyGroup去重的，并不是按照全局的Key去重。Flink自己实现了优先级队列来管理Timer，共有两种实现：
+ 基于堆内存的优先级队列HeapPriorityQueueSet：基于Java堆内存的优先级队列，实现思路与Java的PriorityQueue类似，使用了二叉树
+ 基于RocksDB的优先级队列：分为Cache+RocksDB量级，Cache中保存了前N个元素，其余的保存在RocksDB中，写入的时候采用Write-through策略，即写入Cache的同时要更新RocksDB中的数据，可能要访问磁盘。

基于堆内存的优先级队列比基于RocksDB的优先级队列性能好，但是受限于内存大小，无法容纳太多的数据；基于RocksDB的优先级队列牺牲了部分性能，可以容纳大量的数据。

## 窗口实现

WindowOperator中数据处理过程：

```bob-svg

                              ,----------------,
                              | WindowAssigner |
                              '--------+-------'
                                       ^
                                       |
                                       |
                              ,----------------,           ,---------,        ,-----------------,
            ,---------------->| processElement |---------->| Evictor +------->| Window Function +-------+
            |                 '--------+-------'           '----+----'        '-----------------'       |
            |                          |                        ^                                       |
            |StreamElement             v                        |                         StreamElement |
            |                 ,--------+-------,                |                                       |
            |                 |     Trigger    |                |                                       v
,-----------+--,              '--------+-------'                |                           ,-----------+---,
|    ,--, ,--, |                       ^                        |                           |     ,--, ,--, |
|....|  | |  | |                       |                        |                           |.... |  | |  | |
|    '--' '--' |              ,--------+-------,       ,--------+---------,                 |     '--' '--' |
'------+-------'              |  TimeService   +------>|    onEventTime   |                 '-------+-------'
       |                      '--------+-------'       | onProcessingTime |                         ^
       | Watermark                     ^               '------------------'                         |
       |                               |                                                            |
       |                      ,--------+-------,                                                    |
       +--------------------->|processWatermark+----------------------------------------------------+
                              '----------------'

```

### 时间窗口

按照时间类型分，时间窗口分为处理时间窗口和事件时间窗口，按照窗口行为，时间窗口分为滚动窗口和滑动窗口。滚动窗口的关键属性有两个：Offset（窗口的起始时间）和Size（窗口的长度）。滑动窗口的关键属性有3个：Offset（窗口的起始时间）、Size（窗口的长度）、Slide（滑动距离）。滚动窗口可以看作是滑动距离与窗口长度相等的滑动窗口。在数据元素分配窗口的时候，对于滚动窗口，一个数据元素只属于一个窗口，但可能属于多个滑动窗口。Flink会为每个数据元素分配一个或者多个TimeWindow对象，然后使用TimeWindow对象作为Key来操作窗口对应的状态。

```Java
// 基于事件时间的滑动窗口数据元素分配
// SlidingEventTimeWindows.java
@Override
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
    if (timestamp > LONG.MIN_VALUE) {
        List<TimeWindow> windows = new ArrayList<>((int)(size/slide));
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
        for (long start = lastStart; start > timestamp - size; start -= slide) {
            windows.add(new TimeWindow(start, start + size));
        }
        return windows;
    } else {
        throw new RuntimeException("Record has Long.MIN_VALUE timestamp (=no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call DataStream.assignTimestampsAndWatermarks(...)'?");
    }
}
```

### 会话窗口

Flink中提供了4种Session Window的默认实现：
+ ProcessingTimeSessionWindows：处理时间会话窗口，使用固定会话间隔时长
+ DynamicProcessingTimeSessionWindows：处理时间会话窗口，使用自定义会话间隔
+ EventTimeSessionWindows：事件时间会话窗口，使用固定会话间隔时长
+ DynamicEventTimeSessionWindows：事件时间会话窗口，使用自定义会话间隔时长

会话窗口不同于时间窗口，它的切分依赖于事件的行为，而不是时间序列，在很多情况下会因为事件乱序使得原本相互独立的窗口因为新事件的到来导致窗口重叠，而必须进行窗口的合并。窗口的合并涉及3个要素：
+ 窗口对象的合并和清理：对于会话窗口，为每个事件分配一个SessionWindow，然后判断窗口是否需要与已有的窗口进行合并，窗口合并时按照窗口的起始时间进行排序，然后判断窗口之间是否存在时间重叠，重叠的窗口进行合并，将后序窗口合并到前序窗口中
+ 窗口状态的合并和清理：窗口合并的同时，窗口对应的状态也需要进行合并，默认复用最早的窗口的状态，将其他待合并窗口的状态合并到最早窗口的状态中
+ 窗口触发器的合并和清理：`Trigger#onMerge()`方法用于对触发器进行合并，触发器的合并实际上是删除合并的窗口的触发器，并为新窗口创建新的触发器

### 计数窗口

在DataStream API中没有定义计数窗口的实体类，使用GlobalWindow来实现CountWindow。滚动计数窗口和滑动计数窗口依托于GlobalWindow实现，从实现上来说，对于一个Key，滚动计数窗口全局只有一个窗口对象，使用CountTrigger来实现窗口的触发，使用Evictor来实现窗口滑动和窗口数据的清理。

```Java
// DataStream API中使用CountWindow
// KeyedStream.java
// 滚动计数窗口
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size) {
    return window(GlobalWindows.create())
        .trigger(PurgingTrigger.of(CountTrigger.of(size)));
}

// 滑动计数窗口
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
    return window(GlobalWindows.create())
        .evictor(CountEvictor.of(size))
        .trigger(CountTrigger.of(slide));
}
```
