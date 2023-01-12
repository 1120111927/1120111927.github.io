---
title: Flink数据传递
date: "2023-01-07"
description: Flink进行数据交换有3种模式：本地线程内的数据交换、本地线程间的数据交换、跨网络的数据交换。Flink是基于PUSH的数据传输模型，在网络层面的数据交换必须使用流控机制确保集群的稳定并实现高效的数据处理，Flink在1.15种引入了基于信用的流控机制
tags: 数据交换模式、基于信用的流控机制、反压机制
---

在分布式计算过程中，不同计算节点之间传递数据，一般有PULL模式和PUSH模式两种选择。Flink Batch模式采用PULL模式，将计算过程分成多个阶段，上游完全计算完毕之后，下游从上游拉取数据开始下一阶段计算，直到最终所有的阶段都计算完毕，输出结果，Batch作业结束退出。Flink Stream模式采用PUSH模式，上游主动向下游推送数据，上下游之间采用生产者-消费者模式，下游收到数据触发计算，没有数据则进入等待状态。另外，PUSH模式的数据处理过程也叫作流水线（Pipeline），提到Pipeline时一般指PUSH模式的数据处理过程。

|对比点|Pull|Push|
|---|---|---|
|延迟|延迟高（等待上游所有计算完成）|低延迟（上游边计算边向下游输出）|
|下游状态|有状态，需要知道何时拉取|无状态|
|上游状态|无状态|有状态，需要了解每个下游的推送点|
|链接状态|短连接|长连接|

在Flink中，数据处理的业务逻辑位于UDF的`processElement()方法中，算子调用UDF处理数据完毕之后，通过Collector接口将数据交给下一个算子。

Flink中有3种数据传递方式：
+ 本地线程内的数据传递：是最简单、效率最高的传递形式，其本质是属于同一个OperatorChain的算子之间的数据传递，上游的算子处理数据，然后通过Collector接口直接调用下游算子的`processElement()`方法
+ 本地线程之间的数据传递：位于同一个TaskManager的不同Task的算子之间，通过本地内存进行数据传递。这些Task线程共享同一个BufferPool，通过`wait()`/`notifyAll()`来同步，InputGate负责读取Buffer或Event。本地线程间的数据交换经历5个步骤
    1. 上游算子将数据写入ResultSubPartition中
    2. ResultSubPartition将数据刷新写入LocalBufferPool中，然后通过`inputChannelWithData.notifyAll()`方法唤醒下游算子所在线程
    3. 下游算子所在线程首先调用LocalInputChannel从LocalBuffer中读取数据，然后进行数据的反序列化，将反序列化之后的数据交给算子中的用户代码进行业务处理。如果没有数据则通过InputGate中的`inputChannelWithData.with()`方法阻塞等待数据
+ 跨网络的数据交换：是运行在不同TaskManager JVM中的Task之间的数据传递，与本地线程间的数据交换类似，不同点在于，当没有Buffer可以消费时，会通过PartitionRequestClient向上游Task所在进程发起RPC请求，远程的PartitionRequestServerHandler接收到请求后，读取ResultPartition管理的Buffer，并返回给Client。跨网络的数据交换经历9个步骤
    1. 下游算子所在算子从InputGate的RemoteChannel中消费数据，如果没有数据则阻塞在RemoteInputChannel中的receivedBuffers上，等待数据
    2. 上游算子持续处理数据，并将数据写入ResultSubPartition中
    3. ResultSubPartition通知PartitionRequestQueue有新的数据
    4. PartitionRequestQueue从ResultSub读取数据
    5. ResultSubPartition将数据通过PartitionRequestServerHandler写入Netty Channel，准备写入下游Netty
    6. Netty将数据封装到Response消息中，推送给下游，此处需要下游对上游的request请求，用来建立数据从上游到下游的通道，此请求是对ResultSubPartition的数据请求，创建了PartitionRequestQueue
    7. 下游Netty收到Response消息，进行解码
    8. CreditBasedPartitionRequestClientHandler将解码后的数据写入RemoteInputChannel的Buffer缓冲队列中，然后唤醒下游Task所在线程消费数据
    9. 从下游Task所在线程RemoteInputChannel中读取Buffer缓冲队列中的数据，然后进行数据的反序列化，交给算子中的用户代码进行业务处理

MemorySegment是最底层内存抽象，Buffer用于数据传输，StreamRecord是从Java对象转为Buffer的中间对象。

**RecordWriter** 负责向下游输出Task处理的结果数据，面向StreamRecord，直接处理算子的输出结果，底层依赖于ResultPartitionWriter^[ResultPartitionWriter面向Buffer]。在RecordWriter中实现了数据分区语义，将开发时对数据分区API（核心抽象是ChannelSelector）的调用转换成了实际的物理操作（如`DataStream#shuffle()`）。RecordWriter类负责将StreamRecord（数据元素StreamElement和事件Event）进行序列化，调用SpaningRecordSerializer，再调用BufferBuilder写入MemorySegment中（每个Task都有自己的LocalBufferPool，LocalBufferPool中包含了多个MemorySegment）。RecordWriter提供单播和广播两种写入方式

+ 单播：根据ChannelSelector，对数据流中的每一条数据记录进行选路，有选择地写入一个输出通道的ResultSubPartition中，适用于非BroadcastPartition。默认使用RoundRobinChannelSelector，使用RoundRobin算法选择输出通道循环写入本地输出通道对应的ResultPartition，发送到下游Task

```bob-svg
                ,---------------------,                      ,-------------------------,
                |  ChannelSelector    |                      |     ResultPartition     |
                |   RecordWriter      |            Record1   | ,---------------------, |
                |  ,--------------,   |           +----------+>| ResultSubPartition0 | |
                |  | StreamRecord |   |           |          | '---------------------' |
                |  '-------+------'   |           |Record2   | ,---------------------, |
                |          |          |           |----------+>| ResultSubPartition1 | |
       Dataflow |          v          |           |          | '---------------------' |
      --------->+ ,-----------------, +-----------+Record3   | ,---------------------, |
                | | ChannelSelector | |           |----------+>| ResultSubPartition2 | |
                | '--------+--------' |           |          | '---------------------' |
                |          |          |           |  ...     |           ...           |
                |          v          |           |          |                         |
                | ,-----------------, |           |          | ,---------------------, |
                | |   Serializer    | |           +----------+>| ResultSubPartitionN | |
                | '-----------------' |            RecordN   | '---------------------' |
                '---------------------'                      '-------------------------'
```

+ 广播：向下游所有Task发送相同的数据，即在所有ResultSubPartition中写入N份相同数据。实际实现时，对于广播类型的输出，只会写入编号为0的ResultPartition中，下游Task对于广播类型的数据，都会从编号为0的ResultSubPartition中获取数据

```bob-svg
                ,---------------------,                      ,-------------------------,
                |  ChannelSelector    |                      |     ResultPartition     |
                |   RecordWriter      |            Records   | ,---------------------, |
                |  ,--------------,   |           +----------+>| ResultSubPartition0 | |
                |  | StreamRecord |   |           |          | '---------------------' |
                |  '-------+------'   |           |          | ,---------------------, |
                |          |          |           |          | | ResultSubPartition1 | |
       Dataflow |          |          |           |          | '---------------------' |
      --------->+          |          +-----------+          | ,---------------------, |
                |          |          |                      | | ResultSubPartition2 | |
                |          |          |                      | '---------------------' |
                |          |          |                      |           ...           |
                |          v          |                      |                         |
                | ,-----------------, |                      | ,---------------------, |
                | |   Serializer    | |                      | | ResultSubPartitionN | |
                | '-----------------' |                      | '---------------------' |
                '---------------------'                      '-------------------------'
```

```Java
// 单播数据写入
public void emit(T record) throws IOException, InterruptedException {
    // 遍历通道选择器选出的通道ResultSubPartition
    for (int targetChannel : channelSelector.selectChannels(record, numChannels)) {
        // 获得当前通道对应的序列化器
        RecordSerializer<T> serializer = serializers[targetChannel];
        synchronized(serializer) {
            // 向序列化器中加入记录，加入的记录会被序列化并存入序列化器内部的Buffer中
            SerializationResult result = serializer.addRecord(record);
            // 如果Buffer已经存满
            // 如果记录的数据无法被单个Buffer所容纳，将会被拆分成多个Buffer存储，直到数据写完
            while (result.isFullBuffer()) {
                // 获得当前存储记录数据的Buffer
                Buffer buffer = serializer.getCurrentBuffer();
                // 将Buffer写入ResultPartition中特定的ResultSubPartition
                if (buffer != null) {
                    writeBuffer(buffer, targetChannel, serializer);
                }
                // 向缓冲池请求一个新的Buffer
                buffer = writer.getBufferProvider().requestBufferBlocking();
                // 将新Buffer继续用来序列化记录的剩余数据，然后再次循环这段逻辑，直到数据全部被写入Buffer
                result = serializer.setNextBuffer(buffer);
            }
        }
    }
}
```

**数据记录序列化器（RecordSerializer）** 负责序列化数据，SpanningRecordSerializer是唯一实现。SpanningRecordSerializer是一种支持跨内存段的序列化器，其实现借助于中间缓冲区来缓存序列化后的数据，然后再往真正的目标Buffer里写，写时维护limit和position两个变量，limit表示目标Buffer内存段长度，position表示当前写入位置。在序列化数据写入内存段（Buffer）时存在3种不同的反序列化结果，以枚举SerializationResult表示
+ PARTIAL_RECORD_MEMORY_SEGMENT_FULL：内存段已满但记录的数据没有完全写完，需要申请新的内存段继续写入
+ FULL_RECORD_MEMORY_SEGMENT_FULL：内存段写满，记录的数据已全部写入
+ FULL_RECORD：记录的数据全部写入，但内存段没有满
```Java
// 数据序列化处理
// SpanningRecordSerializer.java
private SerializationResult getSerializationResult() {
    // 如果数据Buffer中没有更多的数据且长度Buffer里也没有更多的数据，该判断可确认记录数据已全部写完
    if (!this.dataBuffer.hasRemaining() && !this.lengthBuffer.hasRemaining()) {
        // 接着判断写入位置跟内存段的结束位置之间的关系，如果写入位置小于结束位置，则说明数据全部写入
        // 否则说明数据全部写入且内存段也写满
        return (this.posiiton < this.limit) ? SerializationResult.FULL_RECORD : SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
    }
    // 若任何一个Buffer中仍存有数据，则记录被标记为部分写入
    return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
}

```

**数据记录反序列化器（RecordDeserializer）** 负责反序列化数据，SpillingAdaptiveSpanningRecordDeserializer是其唯一实现，适用于数据相对较大且跨多个内存段的数据元素的反序列化，支持将溢出的数据写入临时文件。反序列化时也存在不同的反序列化结果，以枚举DeserializationResult表示
+ PARTIAL_RECORD：表示记录并为完全被读取，但缓冲中的数据已被消费完成
+ INTERMEDIATERECORDFROM_BUFFER：表示记录的数据已被完全读取，但缓冲中的数据并未被完全消费
+ LASTRECORDFROM_BUFFER：记录被完全读取，且缓冲中的数据也正好被完全消费

**结果子分区视图（ResultSubPartitionView）** 定义了ResultSubPartition中读取数据、释放资源等抽象行为，其中`getNextBuffer()`是最重要的方法，用来获取Buffer，有两种实现
+ PipelinedSubPartitionView：用来读取PipelinedSubPartition中的数据
+ BoundedBlockingSubPartitionReader：用来读取BoundedBlockingSubPartition中的数据

**数据输出（Output）** 是算子向下游传递的数据抽象，定义了向下游发送StreamRecord、Watermark、LatencyMark的行为，对于StreamRecord，多了一个SideOutput的行为定义
+ WatermarkGaugeExposingOutput：定义了统计Watermark监控指标计算行为，将最后一次发送给下游的Watermark作为其指标值
+ RecordWriterOutput：包装了RecordWriter，使用RecordWriter把数据交给数据交换层，RecordWriter主要用来在线程间、网络间实现数据序列化、写入
+ ChainingOutput/CopyingChainingOutput：用于OperatorChain内存算子之间传递数据，并不会有序列化的过程，直接在Output中调用下游算子的`processElement()`方法，在同一个线程内的算子直接传递数据
+ DirectedOutut/CopyingDirectedOutput：包装类，基于一组OutputSelector选择发送给下游哪些Task
+ BroadcastingOutputCollector/CopyingBroadcastingOutputCollector：包装类，内部包含了一组Output，向所有的下游Task广播数据
+ CountingOutput：其他Output实现类的包装类，没有任何业务逻辑，只是用来记录其他Output实现类向下游发送的数据元素的个数，并作为监控指标反馈给Flink集群

## 数据传递

数据在Task之间传递分为以下几个大的步骤：
1. 数据在本算子处理完后，交给RecordWriter，每条记录都经过ChannelSelector，找到对应的结果子分区
2. 每个结果子分区都有一个独有的序列化器，把这条数据记录序列化为二进制数据
3. 数据被写入结果分区下的各个子分区中，此时该数据已经存入DirectBuffer（MemorySegment）
4. 单独的线程控制数据的flush速度，一旦触发flush，则通过Netty的nio通道向对端写入
5. 对端的Netty Client接收到数据，解码出来，把数据复制到Buffer中，然后通知InputChannel
6. 有可用的数据时，下游算子从阻塞醒来，从InputChannel取出Buffer，再反序列化成数据记录，交给算子执行用户代码（UDF）

### 数据读取

ResultSubPartitionView接收到数据可用通知之后，有两类对象会接收到该通知：LocalInputChannel和CreditBasedSequenceNumberingViewReader。LocalInputChannel是本地JVM线程之间的数据传递，CreditBasedSequenceNumberingViewReader是跨JVM或跨网络的数据传输。无论是本地线程间数据交换还是跨网络的数据交换，数据消费端数据消费的线程会一直阻塞在InputGate上，等待可用的数据，并将可用的数据转换成StreamRecord交给算子进行处理。基本过程为`StreamTask#processInput()` -> `StreamOneInputStreamOperator#processInput()` -> `StreamTaskNetworkInput#emitNext()` -> `SpillingAdataptiveSpanningRecordDeserializer()`，从NetworkInput中读取数据反序列化为StreamRecord，然后交给DataOutput向下游发送。数据反序列化时，使用DataInputView从内存中读取二进制数据，根据数据的类型进行不同的反序列化。对于数据记录，则使用其类型对应的序列化器，对于其他类型的数据元素，如Watermark、LatencyMarker、StreamState，直接读取其二进制数据转换为数值类型

```Java
// StreamTaskNetworkInput读取数据元素
// StreamTaskNetworkInput.java
@Override
public InputStatus emitNext(DataOutput<T> output) throws Exception {
    while (true) {
        // 从反序列化器中获取数据流元素
        if (currentRecordDeserializer != null) {
            DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
            if (result.isBufferConsumed()) {
                currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
                currentRecordDeserializer = null;
            }
            if (result.isFullRecord()) {
                processElement(deserializationDelegate.getInstance(), output);
                return InputStatus.MORE_AVAILABLE;
            }
        }
        // 从InputGate中获取下一个Buffer
        Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
        if (bufferOrEvent.isPresent()) {
            // 如果是Buffer，则设置RecordDeserializer处理下一条记录，否则释放RecordDeserializer
            processBufferOrEvent(bufferOrEvent.get());
        } else {
            if (checkpointedInputGateisFinished()) {
                checkState(checkpointedInputGate.getAvailabelFuture().isDone(), "Finished BarrierHandler should be available");
                if (!checkpointedInputGate.isEmpty()) {
                    throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
                }
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.NOTHING_AVAILABLE;
        }
    }
}

// 从DataInputView反序列化数据
@Override
public StreamElement deserialize(DataInputView source) throws IOException {
    int tag = source.readByte();
    if (tag == TAG_REC_WITH_TIMESTAMP) {
        long timestamp = source.readLong();
        return new StreamRecord<T>(typeSerializer.deserialize(source), timestamp);
    } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
        return new StreamRecord<T>(typeSerializer.deserialize(source));
    } else if (tag == TAG_WATERMARK) {
        return new Watermark(source.readLong());
    } else if (tag == TAG_STREAM_STATUS) {
        return new StreamStatus(source.readInt());
    } else if (tag == TAG_LATENCY_MARKER) {
        return new LatencyMarker(source.readLong(), new OperatorID(source.readLong(), source.readLong()), source.readInt());
    } else {
        throw new IOException("Corrupt stream, found tag: " + tag);
    }
}
```

### 数据写出

Task调用算子执行UDF之后，需要将数据交给下游进行处理，RecordWriter类负责将StreamRecord进行序列化，调用SpaningRecordSerializer序列化数据后，通过Channel编号选择结果子分区，将数据复制到结果子分区中，在写入过程中，序列化器将数据复制到BufferBuilder中，再调用BufferBuilder写入MemorySegment中^[每个Task都有自己的LocalBufferPool，LocalBufferPool中包含了多个MemorySegment]。

```Java
// RecordWriter写出数据
// RecordWriter.java
protected void emit(T record, int targetChannel) throws IOException, InterruptedException {
    checkErroneous();
    // 把StreamRecord序列化为二进制数组
    serializer.serializeRecord(record);
    // 将数据写入ResultPartition的MemorySegment中
    if (copyFromSerializerToTargetChannel(targetChannel)) {
        serializer.prune();
    }
}
// 复制序列化后的数据到目标Channel
// RecordWriter.java
protected boolean copyFromSerializerToTargetChannel(int targetChannel) throws IOException, InterruptedException {
    serializer.reset();
    boolean pruneTriggered = false;
    // 获取对应targetChannel的BufferBuilder
    BufferBuilder buffer Builder= getBufferBuilder(targetChannel);
    // 将序列化器中StreamRecord的二进制数据复制到Buffer中
    SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
    while (result.isFullBuffer()) {
        finishBufferBuilder(bufferBuilder);
        // 数据完全写入到Buffer中，写入完毕
        if (result.isFullRecord()) {
            pruneTriggered = true;
            emptyCurrentBufferBuilder(targetChannel);
            break;
        }
        // 数据没有完全写入，Buffer已满，则申请新的BufferBuilder继续写入
        bufferBuilder = requestNewBufferBuilder(targetChannel);
        result = serializer.copyToBufferBuilder(bufferBuilder);
    }
    checkState(!serializer.hasSerializedData(), "All data should be written at once");
    // 数据写入之后，通过flushAlways控制是立即发送还是延迟发送
    if (flushAlways) {
        flushTargetPartition(targetChannel);
    }
    return pruneTriggered;
}
```

### 数据清理

RecordWriter将StreamRecord序列化完成之后，会根据flushAlways参数决定是否立即将数据进行推送。当所有数据都写入完成后需要调用Flush方法将可能残留在序列化器Buffer中的数据都强制输出。Flush方法会遍历每个ResultSubPartition，然后依次取出该ResultSubPartition对应的序列化器，如果其中还有残留的数据，则将数据全部输出。数据写入之后，无论是即时Flush还是定时Flush，根据结果子分区的类型的不同，行为都会有所不同。

**PipelinedSubPartition** 即时刷新（一条数据向下游推送一次）延迟最低，但吞吐量下降。Flink默认单独启动一个线程，默认100ms刷新一次^[本质上是一种mini-batch，这种mini-batch只是为了增大吞吐量，与Spark的mini-batch处理不是一个概念]。ResultPartition遍历自身的PipelinedSubPartition，逐一进行Flush，Flush之后，通知ResultSubPartitionView有可用数据，可以进行数据的读取了
```Java
// PipelinedSubPartition写入Buffer并通知
// PipelinedSubPartition.java
private void notifyDataAvailable() {
    if (readView != null) {
        readView.notifyDataAvailable();
    }
}
```

**BoundedBlockingSubPartition** 定时刷新每隔一定时间周期将该时间周期内的数据进行批量处理，默认是100ms刷新一次数据刷新的时候根据ResultPartition中ResultSubPartition的类型的不同有不同的刷新行为。ResultPartition遍历自身的BoundedBlockingSubPartition，逐一进行Flush，写入之后回收Buffer，该类型的SubPartition并不会触发数据可用通知

```Java
// BoundedBlockingSubPartition写入Buffer无通知
// BoundedBlockingSubPartition.java
private void writeAndCloseBufferConsumer(BufferConsumer bufferConsumer) throws IOException {
    try {
        final Buffer buffer = bufferConsumer.build();
        try {
            if (canBeCompressed(buffer)) {
                final Buffer compressedBuffer = parent.bufferCompressor.compressToIntermediateBuffer(buffer);
                data.writeBuffer(compressedBuffer);
                if (compressedBuffer != buffer) {
                    compressedBuffer.recycleBuffer();
                }
            } else {
                data.writeBuffer(buffer);
            }
            numBuffersAndEventsWritten++;
            if (buffer.isBuffer()) {
                numDataBuffersWritten++;
            }
        } finally {
            buffer.recycleBuffer();
        }
    } finally {
        bufferConsumer.close();
    }
}
```

## 反压

对于基于PUSH的数据传输模型，如果下游的处理能力无法应对上游节点的数据发送速度，会导致数据在下游处理节点累积，最终导致资源耗尽甚至系统崩溃。反压（BackPressure）机制通过对上游节点发送数据的速度进行流量控制来保持整个流计算系统的稳定性。JVM垃圾回收的停顿导致数据的堆积、瞬时流量爆发、Task中业务逻辑复杂导致数据记录处理比较慢等都会导致反压。

从逻辑上来说，Flink作业的Task之间的连接关系一般是多对多的，上游Task生成多个结果子分区，每个子分区对应下游Task的一个InputChannel，下游Task从上游多个Task获取数据。Flink的Task在物理层面上是基于TCP连接，且TaskManager上的所有Task通过多路复用共享一个TCP连接，每个Task产生各自的结果分区（ResultPartition），每个结果分区拆分成多个单独的结果子分区（ResultSubPartition），与下游的InputChannel一一对应，在网络传输这一层次上，Flink不再区分单个记录，而是将一组序列化记录写入网格缓冲区中。每个Task可用的本地最大缓冲池大小为：Task的InputChannel数量 * buffers-per-channel + floating-buffers-per-gate。

**基于TCP的反压机制** 当Task的发送缓冲池耗尽时，即结果子分区的Buffer队列中或更底层的基于Netty的网络栈的网络缓冲区满时，生产者Task就会被阻塞，无法继续处理数据，开始产生背压。当Task的接收缓冲区耗尽时，也是类似的效果。如果响应Task的缓冲池中没有可用的Buffer，Flink停止从该InputChannel读取，直到在缓冲区中有可用Buffer时才会开始读取数据并交给算子进行处理。这将对使用该TCP连接的所有Task造成背压，影响范围是整个TaskManager上运行的Task

**基于信用（Credit）的反压机制** Flink 1.5版本引入，用来解决同一个TaskManager上Task之间相互影响的问题。该机制可以确保下游总是有足够的内存接收上游的数据，不会出现下游无法接收上游数据的情况。基于信用的反压机制作用于Flink的数据传输层，在结果子分区（ResultSubPartition）和输入通道（InputChannel）引入了信用机制，每个远端的InputChannel都有自己的一组独占缓冲区，不再使用共享的本地缓冲池。LocalBufferPool的大小是浮动的，其中的缓存（Buffer）可用于所有InputChannel，称为浮动缓存（Float Buffer）。下游接收端将当前可用的Buffer数量作为信用值（1Buffer = 1信用）通知给上游，每个结果子分区（ResultSubPartition）将跟踪其对应的InputChannel的信用值。如果信用可用，则缓存仅转发到较底层的网络栈，每发送一个Buffer都会对InputChannel的信用值减1，发送Buffer的同时，还会发送当前结果子分区（ResultSubPartition）队列中的积压数据量（Block Size），下游的接收端会根据积压数据量从浮动缓冲区申请适当数量的Buffer，以便更快地处理上游积压等待发送的数据，下游接收端首先会尝试获取与Backlog大小一样多的Buffer，但浮动缓冲区的可用Buffer数量可能不够，只能获取一部分甚至获取不到Buffer，下游接收端会充分利用获取得到的Buffer，并且会持续等待新的可用Buffer。这种反馈策略保证了不会在公用的Netty和TCP这一层堆积数据而影响其他Task通信。基于信用的流量控制使用配置参数buffer-per-channel参数来设置独占的缓冲池大小，使用配置参数floating-buffers-per-gate设置InputGate输入网关的缓冲池大小，输入网关的缓冲池由属于该网关的InputChannel共享，其缓冲区上限与基于TCP的反压机制相同。一般情况下，使用这两个参数的默认值，理论上可以达到与不采用反压机制的吞吐量一样高。
