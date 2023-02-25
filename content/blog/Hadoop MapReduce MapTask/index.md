---
title: Hadoop MapReduce MapTask源码解析
date: "2023-02-25"
description:
tags:
---

```Java
/**
  * MapOutputBuffer是默认的缓冲区类，可以通过mapreduce.job.map.output.collector.class设置
  */
class MapOutputBuffer<K extends Object, V extends Object> implements MapOutputCollector<K, V>, IndexedSortable {

    int partitions;                                     // 分区数目，即reduce任务数量

    IntBuffer kvmeta;                                   // 元数据缓冲区
    int kvstart;                                        // 记录了元数据溢写的起始位置
    int kvend;                                          // 记录了元数据溢写的末尾位置
    int kvindex;                                        // 记录了下一个元数据的写入位置
    int equator;                                        // 记录了序列化数据/元数据的起始位置
    int bufstart;                                       // 记录了序列化数据溢写的起始位置
    int bufend;                                         // marks beginning of collectable
    int bufmark;                                        // 记录了记录的末尾位置
    int bufindex;                                       // 记录了序列化数据的末尾位置
    int bufvoid;                                        // 记录了存放数据的最大位置

    byte[] kvbuffer;                                    // 环形缓冲区，物理意义上的缓冲区

    int VALSTART = 0;                                   // 元数据项中存储value开始位置的数据的偏移量
    int KEYSTART = 1;                                   // 元数据项中存储key开始位置的数据的偏移量
    int PARTITION = 2;                                  // 元数据项中存储键值对分区号的数据的偏移量
    int VALLEN = 3;                                     // 元数据项中存储value长度的数据的偏移量
    int NMETA = 4;                                      // 元数据项大小（Int数）
    int METASIZE = NMETA * 4;                           // 元数据项大小（字节数）

    int maxRec;                                         // 元数据缓冲区可存储元数据最大数目
    int softLimit;
    boolean spillInProgress;                            // 是否在溢写
    int bufferRemaining;                                // 缓冲区剩余可写入字节数
    int numSpills = 0;                                  // 溢写次数
    int minSpillsForCombine;                            // 对所有spill.out文件进行合并操作时，进行combiner的最少文件数

    Class<K> keyClass;                                  // key的类
    Class<V> valClass;                                  // value的类
    RawComparator<K> comparator;                        // 排序用的比较器
    Serializer<K> keySerializer;                        // key序列化器
    Serializer<V> valSerializer;                        // value序列化器
    CombinerRunner<K,V> combinerRunner;                 // combiner操作类，可以通过配置项mapreduce.job.combine.class设置，默认为null，必须是Reducer类的子类

    boolean spillThreadRunning = false;                 // 溢写线程是否运行
    ReentrantLock spillLock = new ReentrantLock();      // 用于溢写的可重入锁
    SpillThread spillThread = new SpillThread();        // 溢写线程，负责在缓冲区写满后将其内容溢写到文件系统上的spill文件中
    BlockingBuffer bb = new BlockingBuffer();           // 核心是Buffer类，提供了对缓冲区进行的操作
    FileSystem rfs;                                     // 存放spill文件的文件系统

    /** 初始化
      * 设置溢写占比，当使用内存超过一个阈值时就进行溢写，默认是0.8
      * 设置kvbuffer大小
      * 通过反射获取sorter对象
      * 创建kvbuffer，设置equator和其他参数值
      * bufstart = bufend = bufindex = equator = 0
      * bufvoid = kvbuffer.length
      * kvstart = kvend = kvindex = bufvoid - METASIZE
      */
    public void init(MapOutputCollector.Context context) {
        partitions = job.getNumReduceTasks();                                    // 分区数
        rfs = ((LocalFileSystem)FileSystem.getLocal(job)).getRaw();              //
        // 溢写占比，通过配置项mapreduce.map.sort.spill.percent设置，默认为0.8
        float spillper = job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);
        // 环形缓冲区大小，通过配置项mapreduce.task.io.sort.mb设置环形缓冲区大小，默认是100M
        int sortmb = job.getInt(MRJobConfig.IO_SORT_MB, MRJobConfig.DEFAULT_IO_SORT_MB);
        // 获取排序对象，通过配置项map.sort.class设置（必须是IndexSorter子类），默认为快排
        sorter = ReflectionUtils.newInstance(job.getClass(MRJobConfig.MAP_SORT_CLASS, QuickSort.class, IndexedSorter.class), job);

        // 获取sortmb字节大小
        int maxMemUsage = sortmb << 20;
        // 保证缓冲区大小是METASIZE的整数倍
        maxMemUsage -= maxMemUsage % METASIZE;
        // 初始化环形缓冲区
        kvbuffer = new byte[maxMemUsage];
        bufvoid = kvbuffer.length;
        // 将环形缓冲区包装为IntBuffer
        kvmeta = ByteBuffer.wrap(kvbuffer).order(ByteOrder.nativeOrder()).asIntBuffer();
        setEquator(0)
        bufstart = bufend = bufindex = equator;
        kvstart = kvend = kvindex;
        maxRec = kvmeta.capacity() / NMETA;
        softLimit = (int)(kvbuffer.length * spillper);
        bufferRemaining = softLimit;

        comparator = job.getOutputKeyComparator();
        keyClass = (Class<K>)job.getMapOutputKeyClass();
        valClass = (Class<V>)job.getMapOutputValueClass();
        serializationFactory = new SerializationFactory(job);
        keySerializer = serializationFactory.getSerializer(keyClass);
        keySerializer.open(bb);
        valSerializer = serializationFactory.getSerializer(valClass);
        valSerializer.open(bb);

        combinerRunner = CombinerRunner.create(...);

        spillInProgress = false;
        // 运行combiner的最少溢写次数，由配置项mapreduce.map.combine.minspills设置，默认是3
        minSpillsForCombine = job.getInt(JobContext.MAP_COMBINE_MIN_SPILLS, 3);
        // 设置一个守护线程用于溢写
        spillThread.setDaemon(true);
        spillThread.setName("SpillThread");
        // 可重入锁加锁
        spillLock.lock();
        try {
            spillThread.start();               // 开始执行溢写守护线程
            while (!spillThreadRunning) {
                spillDone.await();
            }
        } finally {
            spillLock.unlock();
        }
    }

    void setEquator(int pos) {
      equator = pos;
      // 对齐为元数据大小的整数倍
      int aligned = pos - (pos % METASIZE);
      // 设置kvindex为第一个元数据写入位置
      kvindex = (int)(((long)aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
    }

    /** 写入记录
      * 先判断是否满足溢写条件，如果不满足：
      * + 将key序列化存放在bufindex所指向的为孩子，如果key值被分割在kvbuffer两端，则需要调整key位置，使得key连续，执行后bufindex指向下一个空位置
      * + 将value序列化存放在bufindex指向的位置，并更新bufindex指向下一个空位置
      * + 存放meta数据，将meta数据按valuestart、keystart、partition、valuelength顺序存放
      * + 存放完成后kvindex - 16; bufstart、bufend、kvstart、kvend不变
      */
    public void collect(K key, V value, int partition) {
        // 缓冲区剩余可放入空间减去16B（16B是元数据大小）
        bufferRemaining -= METASIZE;
        // 如果剩余空间小于等于0，开始溢写
        if (bufferRemaining <= 0) {
            ......
        }
        // key插入位置
        int keystart = bufindex;
        // 将key序列化写入kvbuffer中，并更新bufindex（最终调用MapTask$MapOutputBuffer$Buffer#write()方法）
        keySerializer.serialize(key);
        // 调整key位置，防止key跨界现象
        // 由于key是排序关键字，通常需要交给RawComparator进行排序，而它要求排序关键字必须在内存中连续存储，因此不允许key跨界存储
        if (bufindex < keystart) {
            bb.shiftBufferedKey();
            keystart = 0;
        }
        // value插入位置
        int valstart = bufindex;
        // 将value序列化写入kvbuffer中，并更新bufindex
        valSerializer.serialize(value);
        // value长度为0时，序列化器将不执行写操作，为了确保检查边界条件以及维护kvindex变量，对buffer进行零长度写入
        bb.write(b0, 0, 0);
        // 标记记录
        /*
          int markRecord() {
              bufmark = bufindex;
              return bufindex;
          }
        */
        int valend = bb.markRecord();
        // 往缓冲区写入元数据信息
        // kvindex为当前元数据的起始位置
        kvmeta.put(kvindex + PARTITION, partition);                                 // 写入分区信息
        kvmeta.put(kvindex + KEYSTART, keystart);                                   // 写入key开始位置
        kvmeta.put(kvindex + VALSTART, valstart);                                   // 写入value开始位置
        kvmeta.put(kvindex + VALLEN, distanceTo(valstart, valend));                 // 写入value长度
        kvindex = (kvindex - NMETA + kvmeta.capacity()) % kvmeta.capacity();        // 更新kvindex
    }
}

/** 溢写
  * 往缓冲区写入记录时，如果达到了溢写条件（bufferRemaining - METASIZE <= 0 && usedByte >= softLimit），则进行溢写
  * 调用startSpill()方法开始溢写，修改kvend指向当前kvindex - 4的位置，bufend指向bufmark位置
  * 通过spillReady.signal()通知spillThread运行，执行spillThread#run()方法
  * run()方法中首先会调用MapTask#MapOutputCollector#sortAndSpill()，真正的执行溢写，并且进行排序
  * sortAndSpill()首先会创建两个变量mend、mstart分别表示meta信息在kvbuffer中的结束和开始位置
  * 通过当前numSpills创建一个溢写文件，溢写文件保存在文件系统中
  * 调用排序算法，传入mend和mstart值，对kvbuffer的元数据进行排序，比较记录大小时，首先比较记录的分区号（partition）、然后比较key值，排序结束后，按partition和key递增顺序调整meta位置
  * 遍历分区号，然后遍历排序后的kvbuffer的meta信息，判断当前meta的partition信息是否等于该分区号，相等就向文件写出
  * 写入信息时，如果value不连续，被存储在kvbuffer两端，则会读取两部分值，合并成value
  * sortAndSpill()结束后，修改kvstart=kvend，bufstart=bufend
  * 溢写结束后需要调整equator和kvindex、bufindex值，放入这次发生溢写的数据；首先计算未使用的缓冲区大小distkvi、键值对平均大小avgRec，然后使用公式计算新equator位置newPos
  * 根据newPos值更新equator和kvindex、bufindex，更新时，bufindex=newPos，但kvindex需要16B对齐，使用新的kvindex、bufindex存放本次引起溢写的数据
  * 当下一次往kvbuffer写入数据时，判断上一次是否spill过且spill完成，是则调用resetSpill()方法修改kvstart、kvend、bufstart、bufend，接下来使用新的位置继续写入数据
  */
spillLock.lock();
try {
    do {
        if (!spillInProgress) {          // 确认溢写线程没有运行
            int kvbidx = 4 * kvindex;    // 将kvindex（Int计数）转换为字节计数
            int kvbend = 4 * kvend;      // 将kvend（Int计数）转换为字节计数
            // kvindex和bufindex之间（包括equator部分）是序列化的未溢写的数据
            int bUsed = distanceTo(kvbidx, bufindex);        // bUsed表示已用的缓冲区大小
            boolean bufsoftlimit = bUsed >= softLimit;
            // 如果满足下述条件，则之前发生过一次溢写
            if ((kvbend + METASIZE) % kvbuffer.length != equator - (equator % METASIZE)) {
                // 溢写结束，回收空间
                /*
                  void resetSpill() {
                      // 获取当前equator位置
                      int e = equator;
                      bufstart = bufend = e;
                      // aligned为小于equator位置且可以整除METASIZE的最大数（元数据为从大到小放置）
                      int aligned = e - (e % METASIZE);
                      kvstart = kvend = (int) (((long)aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
                  }
                */
                resetSpill();
                bufferRemaining = Math.min(distanceTo(bufindex, kvbidx) - 2 * METASIZE, softLimit - bUsed) - METASIZE;
                continue;
            } else if (bufsoftlimit && kvindex != kvend) {     // 超过了溢写范围且缓冲区不为空
                // 溢写记录
                // 执行溢写
                /*
                  void startSpill() {
                      kvend = (kvindex + NMETA) % kvmeta.capacity();
                      bufend = bufmark;
                      spillInProgress = true;  // 设置溢写线程标志
                      spillReady.signal();  // 通知信号量
                  }
                */
                startSpill();
                // 计算记录的平均占用字节数，用于确定新的equator值
                int avgRec = (int)(mapOutputByteCounter.getCounter() / mapOutputRecordCounter.getCounter());
                // 利用记录平均大小估计剩余空间元数据占用大小，需要保证剩余空间最少存入一个元数据，考虑到aligned，应该至少为2 * METASIZE - 1，而且为了更多的空间存储反序列化数据，元数据最多占剩余空间的1/2
                // 确保 kvindex >= bufindex
                int distkvi = distanceTo(bufindex, kvbidx);     // 未使用字节数
                int newPos = (bufindex + Math.max(2 * METASIZE - 1, Math.min(distkvi / 2, distkvi / (METASIZE + avgRec) * METASIZE))) % kvbuffer.length;   // equator新值
                // 更新equator值，并且重新分配当前元数据存放的位置
                setEquator(newPos);
                bufmark = bufindex = newPos;
                int serBound = 4 * kvend;
                // 更新bufferRemaining，元数据空间、序列化数据空间、软限制中的最小值
                bufferRemaining = Math.min(distanceTo(bufend, newPos), Math.min(distanceTo(newPos, serBound), softLimit)) - 2 * METASIZE;
            }
        }
    } while (false);
} finally {
    spillLock.unlock();
}

class BlockingBuffer extends DataOutputStream {

    /**
      * 判断首部是否有足够的空间存放key，如果有，分两次复制，先将首部的部分key复制到headbytetelen的位置，然后将末尾的部分key复制到首部，移动bufindex，重置bufferRemaining的值，否则先将首部的部分key写入keytmp中，然后分两次写入，再次调用Buffer.write
      */
    void shiftBufferedKey() {
        int headbytelen = bufvoid - bufmark;
        bufvoid = bufmark;
        int kvbidx = 4 * kvindex;
        int kvbend = 4 * kvend;
        int avail = Math.min(distanceTo(0, kvbidx), distanceTo(0, kvbend));
        if (bufindex + headbytelen < avail) {
            System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
            System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
            bufindex += headbytelen;
            bufferRemaining -= kvbuffer.length - bufvoid;
        } else {
            byte[] keytmp = new byte[bufindex];
            System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
            bufindex = 0;
            out.write(kvbuffer, bufmark, headbytelen);
            out.write(keytmp);
        }
    }
}

class SpillThread extends Thread {
    @Override
    public void run() {
        spillLock.lock();
        spillThreadRunning = true;
        while (true) {
            spillDone.signal();
            // 确认溢写线程没有运行
            while (!spillInProgress) {
                spillReady.await();
            }
            spillLock.unlock();
            sortAndSpill();
            spillLock.lock();
            if (bufend < bufstart) {
                bufvoid = kvbuffer.length;
            }
            kvstart = kvend;
            bufstart = bufend;
            spillInProgress = false;
        }
        spillLock.unlock();
        spillThreadRunning = false;
    }
}

void sortAndSpill() {
    //approximate the length of the output file to be the length of the
    //buffer + header lengths for the partitions
    // 输出文件大小近似为序列化数据和分区头之和
    long size = distanceTo(bufstart, bufend, bufvoid) + partitions * APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    FSDataOutputStream partitionOut = null;
    // 创建输出文件
    // 一个大小为分区数*24B的缓冲区并包装为long数组
    SpillRecord spillRec = new SpillRecord(partitions);
    // 当前溢写文件路径
    Path filename = mapOutputFile.getSpillFileForWrite(numSpills, size);
    // 获取溢写文件输出流
    out = rfs.create(filename);
    // 元数据溢写开始位置
    int mstart = kvend / NMETA;
    // 元数据溢写结束位置
    int mend = 1 + (kvstart >= kvend ? kvstart : kvmeta.capacity() + kvstart) / NMETA;
    // 调整元数据位置
    sorter.sort(MapOutputBuffer.this, mstart, mend, reporter);
    int spindex = mstart;
    IndexRecord rec = new IndexRecord();
    InMemValBytes value = new InMemValBytes();
    // 遍历分区号，写入相应的分区
    for (int i = 0; i < partitions; ++i) {
        IFile.Writer<K, V> writer = null;
        long segmentStart = out.getPos();
        partitionOut = CryptoUtils.wrapIfNecessary(job, out, false);
        writer = new Writer<K, V>(job, partitionOut, keyClass, valClass, codec, spilledRecordsCounter);
        if (combinerRunner == null) {     // 如果没有设置combinerRunner
            // 直接溢写
            DataInputBuffer key = new DataInputBuffer();
            // 只要还有元数据并且该元数据保存的partition信息是当前遍历的分区号，就溢写
            while (spindex < mend && kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
                int kvoff = offsetFor(spindex % maxRec);        // 当前元数据所在位置
                int keystart = kvmeta.get(kvoff + KEYSTART);    // key在kvbuffer的开始位置
                int valstart = kvmeta.get(kvoff + VALSTART);    // value在kvbuffer的开始位置
                // key对象和value对象分别用来保存数据的key和value，内部是一个buffer
                // key对象中的buffer直接使用kvbuffer
                // value对象中的buffer，如果value没有跨过bufvoid时直接使用kvbuffer，否则新建一个buffer，然后从kvbuffer中获取value值
                key.reset(kvbuffer, keystart, valstart - keystart);
                getVBytesForOffset(kvoff, value);
                // 写出
                writer.append(key, value);
                ++spindex;
            }
        } else {
            int spstart = spindex;
            while (spindex < mend && kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
                ++spindex;
            }
            // 当分区内的记录少于某个阈值是避免combiner
            if (spstart != spindex) {
                combineCollector.setWriter(writer);
                RawKeyValueIterator kvIter = new MRResultIterator(spstart, spindex);
                combinerRunner.combine(kvIter, combineCollector);
            }
        }

        writer.close();
        // record offsets
        rec.startOffset = segmentStart;
        rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
        rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
        spillRec.putIndex(rec, i);

        writer = null;
        if (null != writer) writer.close();
    }
}
```