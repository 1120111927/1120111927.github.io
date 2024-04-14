---
title: Flink内存管理
date: "2023-12-25"
description: 借助MemeorySegment，自定义的序列化机制，利用堆外内存等手段，相比于直接在JVM堆上创建对象的方式，Flink可以获得更好的性能表现，并且可以更高效地利用内存
tags: 
---

```toc
ordered: true
class-name: "table-of-contents"
```

基于JVM的大数据处理分析引擎直接使用JVM堆内存来管理数据对象会导致一系列问题：大量的数据对象不停地创建和失效，难于管理和控制堆内存，容易引发OOM问题；为了处理海量数据而分配了较大的内存空间，GC开销很容易达到50%以上，严重影响性能；Java对象存储本身存在开销，对于小对象数据集，对象头、对齐填充这些存储开销非常浪费。因此，主流的数据处理引擎（如Spark、Flink等）都有一套自己的内存管理机制。

Flink基于MemorySegment和二进制数据直接管理数据对象，将对象序列化到一个个预先分配的MemorySegment中，有以下几种好处：

+ 保证内存安全：由于分配的MemorySegment的数量是固定的，因而可以准确地追踪MemorySegment的使用情况
+ 减少了GC的压力：因为分配的MemorySegment是长生命周期的对象，数据都以二进制形式存放，且MemorySegment可以回收重用，所以MemorySegment会一直保留在老年代不会被GC；而由用户代码生成的对象基本都是短生命周期的，Minor GC可以快速回收这部分对象，尽可能减少Major GC的频率。此外，MemorySegment还可以配置为使用堆外内存，进而避免GC
+ 节省内存空间：数据对象序列化后以二进制形式保存在MemorySegment中，减少了对象存储的开销
+ 高效的二进制操作和缓存友好的计算：可以直接基于二进制数据进行比较等操作，避免了反复进行序列化于反序列；另外，二进制形式可以把相关的值，以及hash值，键值和指针等相邻地放进内存中，这使得数据结构可以对高速缓存更友好

Flink实现了一套自己的序列化框架。主要是出于以下考虑：首先，比较和操作二进制数据需要准确了解序列化的布局，针对二进制数据的操作来配置序列化的布局可以显著提升性能；其次，Flink应用所处理的数据对象类型通常是完全已知的，由于数据集对象的类型固定，对于数据集可以只保存一份对象Schema信息，进一步节省存储空间。Flink可以处理任意的Java或Scala对象，而不必实现特定的接口，对于Flink Java应用，Flink通过反射框架获取用户自定义函数返回的类型，对于Flink Scala应用，Flink通过Scala Compiler分析用户自定义函数返回的类型。每一种数据类型都对应一个TypeInfomation，通过TypeInfomation可以获取到对应数据类型的序列化器TypeSerializer，对于可以用作key的数据类型，TypeInfomation还可以生成TypeComparator，用来直接在序列化后的二进制数据上进行compare、hash等操作。

MemorySegment是一段固定长度的内存（默认32KB），是Flink中最小的内存分配单元，通常不直接构造，而是通过MemorySegmentFactory创建。MemorySegment提供了高效的读写方法，它的底层可以是堆上的字节数组（byte[]），也可以是堆外的ByteBuffer，通过Unsafe实现同一份代码管理堆内存和堆外内存^[Unsafe的方法会根据对象引用表现出不同的行为，如getLong(Object reference, long offset)方法在reference不为null时，会取该对象的地址，加上offset，从相对地址处取出8字节；而在reference为null时，offset就是要操作的绝对地址。所以，通过控制对象引用的值，就可以灵活地管理堆外内存和堆内存]。Flink还提供了AbstractPagedInputView和AbstractPagedOutputView（分别实现了java.io.DataOutput和java.io.DataInput接口^[DataInput接口用于从二进制流中读取字节，并重构所有Java基本类型数据，DataOutput接口用于将数据从任意Java基本类型转换为一系列字节，并将这些字节写入二进制流]）来通过一种逻辑视图的方式操作多块连续的MemorySegment。

<details>

<summary>具体实现</summary>

```Java
class MemorySegment {
    byte[] heapMemory;                // 堆内存引用
    long address;                     // 堆外内存地址
    ByteBuffer offHeapBuffer;         // 堆外内存引用
    sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    // 基于堆内存创建MemorySegment
    MemorySegment(byte[] buffer, Object owner) {
        this.heapMemory = buffer;
        this.offHeapBuffer = null;
        this.size = buffer.length;
        this.address = BYTE_ARRAY_BASE_OFFSET;  // UNSAFE.arrayBaseOffset(byte[].class)
        this.addressLimit = this.address + this.size;
        this.owner = owner;
        this.allowWrap = true;
        this.cleaner = null;
        this.isFreedAtomic = new AtomicBoolean(false);
    }

    // 基于堆外内存创建MemorySegment
    MemorySegment(
        ByteBuffer buffer,
        Object owner,
        boolean allowWrap,
        Runnable cleaner) {
        this.heapMemory = null;
        this.offHeapBuffer = buffer;
        this.size = buffer.capacity();
        this.address = getByteBufferAddress(buffer);
        this.addressLimit = this.address + this.size;
        this.owner = owner;
        this.allowWrap = allowWrap;
        this.cleaner = cleaner;
        this.isFreedAtomic = new AtomicBoolean(false);
    }

    boolean isOffHeap() {
        return heapMemory == null;
    }

    long getLong(int index) {
        long pos = address + index;
        if (index >= 0 && pos <= addressLimit - 8) {
            // 能够在一个实现中同时操作堆内存和堆外内存的关键
            return UNSAFE.getLong(heapMemory, pos);
        }
    }
}
```

</details>

抛开JVM内存模型，单从主要使用方式来看，TaskManager的内存主要分为三个部分：

+ Network Buffers：一组MemorySegment，主要用于网络传输，在TaskManager启动时分配，通过NetworkEnvironment和NetworkBufferPool进行管理
+ Managed Memory：一组MemorySegment，主要用于Batch模式下的sorting、hashing和cache等，通过MemoryManager进行管理
+ Remaining JVM heap：余下的堆内存留给TaskManager的数据结构以及用户代码处理数据时使用。TaskManager自身的数据结构并不会占用太多内存，因而主要都是供用户代码使用，用户代码创建的对象通常生命周期都较短

Network Buffers管理按照BufferPoolFactory -> BufferPool -> Buffer这样的结构进行组织。

Buffer接口是对池化的MemorySegment的包装，带有引用计数，使用两个指针分别表示写入的位置和读取的位置，具体实现类是NetworkBuffer（继承自Netty的AbstractReferenceCountedByteBuf，很方便地实现了引用计数和读写指针功能）。BufferBuilder和BufferConsumer构成了写入和消费Buffer的通用模式：通过BufferBuilder向底层的MemorySegment写入数据，再通过BufferConsumer生成只读的Buffer，读取BufferBuilder写入的数据。这两个类都不是线程安全的，但可以实现一个线程写入，另一个线程读取的效果。

BufferPool接口继承了BufferProvider和BufferRecycler接口，提供了申请以及回收Buffer的功能，具体实现类是LocalBufferPool，LocalBufferPool中Buffer的数量可以动态调整。BufferPoolFactory接口是BufferPool的工厂，用于创建及销毁BufferPool，具体实现类是NetworkBufferPool。NetworkBufferPool在初始化时创建一组MemorySegment，这些MemorySegment会在所有LocalBufferPool之间进行均匀分配。

<details>

<summary>具体实现</summary>

```Java
class NetworkBufferPool
    implements BufferPoolFactory, MemorySegmentProvider
{
    int totalNumberOfMemorySegments;
    int memorySegmentSize;
    // 所有可用的MemorySegment
    ArrayDeque<MemorySegment> availableMemorySegments;
    int numTotalRequiredBuffers;
    // 管理的BufferPool
    Set<LocalBufferPool> allBufferPools = new HashSet<>();
    // 初始化时创建一组MemorySegment
    NetworkBufferPool(
        int numberOfSegmentsToAllocate,
        int segmentSize,
        Duration requestSegmentsTimeout) {
        this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;
        this.memorySegmentSize = segmentSize;
        this.requestSegmentsTimeout = requestSegmentsTimeout;
        long sizeInLong = (long) segmentSize;
        this.availableMemorySegments = new ArrayDeque<>(numberOfSegmentsToAllocate);
        for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
            availableMemorySegments.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(segmentSize, null));
        }
        long allocatedMb = (sizeInLong * availableMemorySegments.size()) >> 20;
        LOG.info("Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).", ...);
    }
}
```

</details>

MemoryManager是管理Managed Memory的类，主要通过内部接口MemoryPool来管理所有的MemorySegment，在Batch模式下使用。Managed Memory相比于Network Buffers管理更为简单，因为不需要Buffer的那一层封装。
