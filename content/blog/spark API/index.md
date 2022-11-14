---
title: Spark RDD和DataFrame
date: "2021-04-19"
description: 
tags: RDD, DataFrame, Dataset, Spark SQL, Structured API
---

## RDD

Spark的low-level API是指RDD、SparkContext和分布式共享变量（distributed shared variable，包括累加器和广播变量），SparkContext是low-level API的入口，通过SparkSession对象获取`SparkSession#sparkContext`。

RDD（Resilient Distributed Datasets，弹性分布式数据集）是Spark的基础数据模型，用于表示所有内存中和磁盘中的分布式数据实体，Dataset或DataFrame程序都将被编译成RDD，Spark UI也使用RDD来描述作业执行（可以通过`RDD#setName(name: String)`方法设置RDD在Spark UI中的名称）。RDD只是一个逻辑概念，表示不可变的、分区的、可以被并行处理的记录集合，在内存中并不会真正地为某个RDD分配存储空间（除非被缓存），其数据只会在计算中产生。

RDD具有5大特性：

+ 分区列表（partitions）：RDD包含多个分布在集群不同节点上的分区，分区的数目影响对RDD进行并行计算的并行度。分区是一个逻辑概念，变换前后的新旧分区在物理上可能是同一块内存或存储，以解决函数式不变性导致的内存需求无限扩张问题。默认值是该程序分配到的CPU核数，如果是从HDFS文件创建，默认为文件的数据块数
  + `RDD#coalesce(numPartitions: Int, shuffle: Boolean)`（shuffle指定是否进行shuffle，可选，默认值为false）、`RDD#repartition(numPartitions: Int)`方法用于改变分区数目、`OrderedRDD#repartitionAndSortWithinPartitions(partitioner: Partitioner)`（使用指定的分区器对RDD重新分区，并在分区内根据键对元素排序，比先`repartition()`再在分区内排序高效）
  + `RDD#getNumPartitions`方法用于返回分区数目

+ 计算函数（compute）：封装了从父RDD到当前RDD的计算逻辑。Spark中RDD的计算以分区为单位，而且计算函数都是在遍历迭代器，不需要保存每次计算的结果

+ 依赖列表（dependencies）：记录了RDD的父RDD（数据依赖关系），不仅用来划分阶段（stage），在正向计算时得到输出结果，也可以在错误发生时，用来回溯需要重新计算的数据，达到错误容忍的目的。存在两种依赖：窄依赖（Narrow Dependencies）和宽依赖（Wide Dependencies）。窄依赖是指父RDD的每个分区都至多被一个子RDD的分区使用，而宽依赖是指多个子RDD的分区依赖一个父RDD的分区。如 map操作是一种窄依赖，join操作是一种宽依赖（除非父RDD已经基于Hash策略被划分过了）。窄依赖允许在单个集群节点上流水线式执行，宽依赖需要所有的父RDD数据可用，并且数据已经通过Shuffle操作；在窄依赖中，节点失败后的恢复更加高效，只需重新计算丢失的父级分区，并且这些丢失的父级分区可以并行地在不同节点上重新计算，在宽依赖的继承关系中，单个失败的节点可能导致一个RDD地所有先祖RDD中的一些分区丢失，导致计算重新执行

+ 分区器（partitioner）：定义了分区逻辑，分区对于Shuffle类操作很关键，决定了该操作的父RDD和子RDD之间的依赖类型，如join操作，如果协同划分的话，两个父RDD之间、父RDD与子RDD之间能形成一致的分区划分，即同一个Key保证被映射到同一个分区，这样就能形成窄依赖，反之，如果没有协同划分，导致宽依赖。协同划分是指指定相同的分区器以产生前后一致的分区。分区器只存在于键值对RDD中，对于非键值对RDD，其值为None，通过`PairRDD#partitonBy(partitoner: Partitioner)`方法得到使用指定partitioner的RDD副本。Spark默认提供两种分区器：哈希分区函数（HashPartitioner）和范围分区函数（RangePartitioner）。哈希分区使用记录的哈希值来对数据进行分区，只需知道分区个数，就能将数据确定性地划分到某个分区，常用于数据Shuffle阶段。范围分区按照元素的大小关系将其划分到不同分区，每个分区表示一个数据区域，需要统计RDD中数据的最大值和最小值来提前划分好数据区域，一般采用抽样方法估算数据区域边界，常用于排序任务。另外还有水平分区，指按照记录的索引进行分区，常用于输入数据的划分，如 `sparkContext.parallelize(list)`按照元素的下标分区，处理HDFS数据时按照128M为单位将输入数据划分为数据块。

+ 优先位置列表（preferredLocations）：在Spark生成任务的有向无环图（DAG）时，会尽可能地把计算分配到靠近数据的位置（data locality），减少数据网络传输。RDD产生的时候存在优先位置，如HadoopRDD分区的优先位置就是HDFS块所在的节点；当RDD分区被缓存，则计算应该发送到缓存分区所在的节点进行，再不然回溯RDD的血统一直找到具有优先位置属性的父RDD，并据此决定子RDD的位置

RDD提供了转换（transformation，延迟执行）和动作（action，立即执行）来操作分布式数据。运行Python RDD程序等价于对数据逐行调用Python UDF，首先将数据序列化后传给Python进程，然后在Python进程完成操作，最后将结果反序列化后传给JVM，会带来巨大的性能损失。

RDD中没有Row的概念，每条记录都只是Java、Scala或Python原生对象，这带来巨大的灵活性，但是Spark不知道记录的内部结构，很多操作和优化需要用户手动处理。

**自定义序列化**任何需要并行处理的对象（或函数）都必须是可序列化的，Kryo序列化比Java序列化快很多，但是不支持所有的可序列化类型，并且需要注册需要序列化的类来获取最高性能。通过在SparkConf中设置`spark.serializer`为`org.apache.spark.serializer.KryoSerializer`来使用Kryo序列化，该配置项用于设置工作节点之间shuffle数据和序列化RDD到磁盘的序列化器[^Spark 2.0.0及之后，Spark shuffle简单类型的RDD时默认使用Kryo序列化]。通过`SparkConf#registerKryoClasses()`方法注册需要序列化的类。

### RDD子类

#### MapPartitionsRDD

对父RDD的每个分区应用指定函数的RDD。

```scala
class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U], // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
```

+ prev：父RDD
+ f：将元组`(TaskContext, partition index, input iterator)`映射为输出iterator的函数
+ preservesPartitioning：input函数是否保留分区器，一般是false，除非prev是键值对RDD并且input函数没有修改键
+ isFromBarrier：表示该RDD是否由RDDBarrier转换而来
+ isOrderSensitive：函数是否是顺序敏感的，如果是顺序敏感的，那么输入顺序改变时它将返回完全不同的结果

partitioner：`if (preservesPartitioning) firstParent[T].partitions` else None

partitions: `firstParent[T].partitions`

compute：`f(context, split.index, firstParent[T].iterator(split, context))`

#### ParallecCollectionRDD

```scala
class ParallelCollectionRDD[T: ClassTag](
    sc: SparkContext,
    data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]])
    extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
    slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    new InterruptibleIterator(context, s.asInstanceOf[ParallelCollectionPartition[T]].iterator)
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    locationPrefs.getOrElse(s.index, Nil)
  }
}
```

### 操作

Spark中的操作大致可以分为四类操作：

+ 创建操作（creation operation）：用于RDD创建工作。RDD创建只有两种方法，一种是来自于内存集合和外部存储系统，一种是通过转换操作生成的RDD
+ 转换操作（transformation operation）：将RDD通过一定的操作变换成新的RDD，RDD转换操作是惰性操作，只是定义了一个新的RDD，并没有立即执行
+ 控制操作（control operation）：进行RDD持久化，可以让RDD按不同的存储策略保存在磁盘或者内存中
+ 行动操作（action operation）：能够触发Spark运行的操作。Spark中行动操作分为两类，一类的操作结果变成Scala集合或者变量，另一类将RDD保存到外部文件系统或者数据库中

### 创建操作

有三种方式创建RDD：从DataFrame、Dataset创建RDD、从Scala/Java集合创建RDD和从外部存储系统创建RDD。

通过调用`SparkContext#parallelize(seq: Seq[T], numSlices: Int)`或`SparkContext#makeRDD(seq: Seq[T], numSlices: Int)`方法（numSlices可选，默认值为defaultParallelism）从Scala/Java集合创建RDD。

通过调用以下方法从Hadoop支持的存储系统（如本地文件、HDFS、HBase）创建RDD，支持文本文件、SequenceFiles和任何Hadoop InputFormat格式。

+ `textFile(path: String, minPartitions: Int)`：读取HDFS、本地文件系统或其他Hadoop支持的文件系统上的文本文件，返回`RDD[String]`。支持整个文件目录读取，文件可以是文本或者压缩文件^[如gzip等，自动执行解压缩并加载数据]。默认情况下为每一个数据块创建一个分区，minPartitions（可选，默认值为defaultMinPartition，不能少于HDFS数据块的分片数）指定分区数目

+ `wholeTextFiles(path: String, minPartitions: Int)`：读取HDFS、本地文件系统或其他Hadoop支持的文件系统上包含文本文件的目录^[文本文件编码必须为UTF-8]，返回`RDD[(String, String)]`，其中键是每个文件的路径，值是文件的内容。path指定目录，可以是逗号分隔的目录列表；minPartitions指定分区数目

+ `hadoopRDD[K, V](conf: JobConf, inputFormatClass: Class[_ <: InputFormat[K, V]], keyClass: Class[K], valueClass: Class[V], minPartitions: Int = defaultMinPartitions): RDD[(K, V)]`：读取任意hadoop支持的存储系统。conf设置数据集的JobConf；inputFormatClass要读取数据的存储格式；keyClass inputFormatClass键的class；valueClass inputFormatClass值的class；minPartition指定生成的Hadoop分片的最小数目

+ `newAPIHadoopRDD(conf: Configuration, fClass: Class[F], kClass: Class[K], vClass: Class[V])`：使用新api读取任意Hadoop支持的存储系统。conf设置数据集的Configuration；fClass要读取数据的存储格式；kClass fClass键的类；vClass fClass值的类

#### 转换操作

**filter操作**：`RDD#filter(f: (T) => Boolean)`、`OrderedRDD#filterByRange(lower: K, upper: K)`

**map操作**：`RDD#map(f: (T) => U)`、`PairRDD#mapValues(f: (V) => U)`、`RDD#mapPartitions(f: (Iterator[T] => Iterator[U]), preservesPartitioning: Boolean)`（preservesPartitioning设置是否保存Partitioner，可选，默认为false^[一般都应该为false，除非这是一个Pair RDD并且传入的函数不修改Key]）、`RDD#mapPartitionsWithIndex(f: (Int, Iterator[T] => Iterator[U]), preservesPartitioning: Boolean)`

**reduce操作**：`RDD#reduce(f: (T, T) => T)`、`PairRDD#reduceByKey(f: (V, V) => V)`

**flatMap操作**：`RDD#flatMap(f: (T) => TraversableOnce[U])`、`PairRDD#flatMapValues(f: (T) => TraversableOnce[U])`

**聚合操作**：`RDD#groupBy(f: (T) => K)`/`PariRDD#groupByKey()`[^应用函数之前，每个执行器都必须在内存中保存某个键的所有值]（返回`RDD[(K, Iterable[T])]`，性能较差，如果是为了对每个结果进行聚合，使用性能更好的`PairRDD#aggregateByKey()`或者`PairRDD#reduceByKey()`）、`PairRDD#aggregateByKey(zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U)`（seqOp用于合并同一个分区中的值，combOp用于合并不同分区中值）[^包含`ByKey`的方法仅支持键值对RDD]、`PairRDD#foldByKey(zeroValue: V)(func: (V, V) => V)`（零值可能被合并多次，绝对不能改变结果，如连接列表操作时零值为Nil，加法操作时零值为0，乘法操作时零值为1）、`PairRDD#combineByKey(createCombiner: (V) => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)`（createCombiner：组合器函数，用于将V类型转换成C类型，输入参数为键值对中的键；mergeValue：合并值函数，将一个C类型和一个V类型值合并成一个C类型；mergeCombiners：合并组合器函数，用于将两个C类型值合并成一个C类型）

**去重操作**：`RDD#distinct()`、`RDD#distinct(numPartitions: Int)`

**排序操作**：`RDD#sortBy(f: (T) => K, ascending: Boolean, numPartitions: Int)`（ascending可选，默认为true；numPartitions可选，默认为`this.partitions.length`）、`OrderedRDD#sortByKey(ascending: Boolean, numPartitions: Int)`（根据key对RDD排序，ascending指定是否使用ASC，可选，默认值为true；numPartitions指定分区数目，可选，默认值为RDD分区数目）

**抽样操作**：`RDD#sample(withReplacement: Boolean, fraction: Double, seed: Long)`（withReplacement=true表示有放回的抽样^[当withReplacement为false时，fraction表示选择每个元素的概率，取值范围为[0, 1]，当withReplacement为true时，fraction表示选择每个元素的期望次数，必须大于等于0]，seed表示随机数种子，可选，默认值为`Utils.random.nextLong`）、`PairRDD#sampleByKey(withReplacement: Boolean, fraction: Double, seed: Long)`、`PairRDD#sampleByKeyExact(withReplacement: Boolean, fraction: Double, seed: Long)`

**关联操作**：`PairRDD#join(other: RDD[(K, W)])`（关联操作，返回`RDD[(K, (V, W))]`）、`PairRDD#leftOuterJoin(other: RDD[(K, W)])`、`PairRDD#rightOuterJoin(other: RDD[(K, W)])`、`PairRDD#cogroup(other: RDD[(K, W)])`/`PairRDD#groupWith(other: RDD[(K, W)])`（根据键关联多个键值对RDD，相当于SQL中的全外关联，返回`RDD[(K, (Iterable[V], Iterable[W]))]`，有`cogroup(other1, other2)`、`cogroup(other1, other2, other3)`变体）、`cartesian(other: RDD[U])`（笛卡尔积）、`RDD#zip(other: RDD[U])`（返回`RDD[(T, U)`，将两个RDD组合成Key/Value形式的RDD，要求两个RDD的分区数目以及每个每区下的元素都相同）、`RDD#zipPartitions(other: RDD[U])(f: (Iterator[T], Iterator[U]) => Iterator[V])`（返回`RDD[V]`，将多个RDD按照分区组合成Key/Value形式的RDD，要求这些RDD具有相同的分区数，但是对于每个分区内的元素数量没有要求，有`zipPartitions(other1, other2)`、`zipPartitions(other1, other2, other3)`等变体）、`RDD#zipWithIndex()`（返回`RDD[(T, Long)]`，将RDD中的元素和这个元素在RDD中的ID（索引号）组成Key/Value对，索引号基于分区索引以及每个元素在分区内的顺序，第一个分区中的第一个元素索引为0，最后一个分区中的最后一个元素索引最大。当RDD包含多个分区时，需要启动一个Spark作业来计算每个分区的开始索引号。RDD分区中元素无序时，多次计算间同一个元素的索引不一定相同。如果需要分区内元素有序来保证同一个元素索引相同，应该对RDD使用`sortByKey()`或将其保存到文件中）、`RDD#zipWithUniqueId()`（返回`RDD[(T, Long)]`，将RDD中的元素和一个唯一ID组合成Key/Value对，唯一ID生成算法：每个分区中第一个元素的唯一ID值为该分区索引号；每个分区中第N个元素的唯一ID值为：（前一个元素的唯一ID值）+（该RDD总的分区数）。不需要启动一个Spark作业。RDD分区中元素无序时，多次计算间同一个元素的唯一ID不一定相同。如果需要分区内元素有序来保证同一个元素唯一ID值相同，应该对RDD使用`sortByKey()`或将其保存到文件中）

**交**：`RDD#intersection(other: RDD[T])`

**并**：`RDD#union(other: RDD[T])`、`++(other: RDD[T])`

**差**：`RDD#subtract(other: RDD[T])`、`PairRDD#subtractByKey(other: RDD[(K, W)])`

**pipe操作**：`RDD#pipe(command: String)`（调用外部程序处理RDD中的元素，存在变体`pipe(command, envMap)`、`pipe(command, envMap, printPipeContext, printRDDElement, separateWorkingDir, bufferSize, encoding)`，command为要运行的命令，env为要设置的环境变量，printPipeContext用于在处理元素前输出pipe context，值为行输出函数，如out.println，printRDDElement用于自定义如何pipe元素，该函数第一个参数为RDD中的元素，第二个参数为行输出函数，separateWorkingDir表示为每个任务使用单独的工作目录，bufferSizze指定piped进程标准输入的buffer大小，encoding指定编码）

**glom**：`RDD#glom()`（将RDD的每个分区转变成数组）

**PairRDD操作**：`PairRDD#keyBy(f: (T) => K)`（将RDD中的元素转换为元组）、`PairRDD#keys`、`PairRDD#values`

#### 控制操作

持久化RDD：`RDD#cache()/RDD#persist()`（以`MEMORY_ONLY`存储级别持久化RDD，变体`RDD#persist(newLevel: StorageLevel)`指定存储级别）

checkpoint：`RDD#checkpoint()`（将RDD保存到`SparkContext.setCheckpointDir`指定目录下的文件中，并移除RDD的所有父依赖。必须在对RDD执行任何动作前调用，强烈推荐将RDD持久化到内存中，否则将需要再次计算。设置检查点对包含宽依赖的长血统的RDD是非常有用的，可以避免占用过多的系统资源和节点失败情况下重新计算成本过高的问题。与持久化操作（`cache`、`persist`）相比，`checkpoint`会删除RDD的所有父依赖。DataFrame/Dataset API不支持checkpoint）、`RDD#localCheckpoint()`（为了性能牺牲了容错，将RDD保存到执行器本地存储，和动态分配一起使用时并不安全）、`RDD#getCheckpointFile`（获取RDD checkpoint目录名称，不支持local checkpoint）、`RDD#isCheckpointed`（检查RDD是否有对应的checkpoint）

barrier：`RDD#barrier()`（用于支持屏障调度）

#### 行动操作

获取RDD中元素数目：`RDD#count()`、`RDD#countApprox(timeout: Long, confidence: Double)`（confidence指定统计置信度，可选，默认值为0.95）、`countApproxDistinct(p: Int, sp: Int)`、`RDD#countByValue()`（统计RDD中各个值出现的次数，返回`(value, count)`形式的map）、`RDD#countByValueApprox(timeout: Long, confidence: Double)`、`PairRDD#countByKey()`、`PairRDD#countByKeyApprox(timeout: Long, confidence: Double)`

获取RDD中的元素：`RDD#first()`（返回RDD中第一个元素）、`RDD#take(num: Int)`（返回RDD中前n个元素）、`RDD#takeOrdered(num: Int)`（返回RDD中前n小的元素）、`RDD#takeSample(withReplacement: Boolean, num: Int, seed: Long)`（从RDD中抽取n个元素）、`RDD#collect()`（以数组形式返回RDD中的所有元素）、`PairRDD#lookup(key: K)`（以列表形式返回指定键对应的所有值）

遍历RDD中的元素：`RDD#foreach(f: T => Unit)`、`RDD#foreachPartition(f: (Iterator[T]) => Unit)`

聚合操作：`RDD#aggregate(zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U)`（通过指定的聚合函数与零值先聚合每个分区内的所有元素，然后聚合所有分区的结果）、`RDD#treeAggregate(zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U, depth: Int)`（在driver执行最终聚合之前先进行部分节点的聚合，以多层树的方式减少最终聚合占用的内存，depth指定树的深度，可选，默认值是2）、`RDD#fold(zeroValue: U)(op: (T, T) => T)`

保存到外部系统：`RDD#saveAsTextFile(path: String)`（保存为文本文件，变体`saveAsTextFile(path, codec)`可以指定压缩格式）、`RDD#saveAsObjectFile(path: String)`（保存为Sequence文件）、`PairRDD#saveAsNewAPIHadoopDataset(conf: JobConf)`、`PairRDD#saveAsNewAPIHadoopFile(path: String)`

### 广播变量与累加器

分布式共享变量包括广播变量和累加器。

广播变量是缓存在集群每个节点而非和任务一起序列化的共享的、不可变变量，一种在集群共享不可变变量的高效的方式。在函数闭包中直接引用变量是低效的，函数闭包中引用的变量在工作节点每个任务都需要反序列化一次，且每个作业都需要重新发送到节点。使用广播变量需要先在Driver中通过`SparkContext#broadcast(value: T)`方法广播变量，然后在节点通过`Broadcast#value`方法引用该变量。

累加器是一种在多个转换中更新变量并将其传回driver节点的高效、容错的方式，可以用于debug、简单聚合。累加器仅动作执行时更新，Spark保证每个任务只会更新一次累加器。有名称的累加器才会在Spark UI中显示它们的值。使用累加器需要先在Driver中定义（`new XxxAccumulator`）并注册（`SparkContext#register(acc: Accumulator)`、`SparkContext#register(acc: Accumulator, name: String)`），然后在节点通过`Accumulator#add(v)`方法更新累加器，最后在driver节点通过`Accumulator#value`获取累加器的值。可以通过继承`AccumulatorV2`自定义累加器。

### 开发

#### 依赖

```xml
<!-- https://mvnrepository.com/artifact/org.apache.spark/spark-core -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_${scala_version}</artifactId>
    <version>${spark_version}</version>
</dependency>
```

如果需要访问HDFS集群则需要添加对应的hadoop-client依赖
```xml
<!-- https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>${hadoop_version}</version>
</dependency>
```

## Structured API

Structured API包含三种核心类型API：DataFrame、Dataset、SQL。对于Scala，DataFrames就是Dataset\[Row]，Row is Spark's internal representation of its optimized in-memory format for computation。Row表示一条记录，DataFrame中每条记录都是Row类型的，可以从SQL、RDD、数据源或代码生成Row。

Structured API（DataFrame/Dataset/SQL Code）执行过程：
1. 生成逻辑计划（Logical Planning）：将用户代码转换成逻辑计划（logical plan），逻辑计划仅表示一组抽象的转换，不涉及executor或driver，仅仅是将用户表达式转换成最优的版本。首先将用户代码转换成未解析的逻辑计划（unresolved logical plan，未解析是指引用的表或列可能不存在），然后在分析器（analyzer）中使用catalog（存储着所有表和DataFrame信息）解析（resolve）列和表。如果catalog中不包含需要的表或列，分析器将拒绝未解析的逻辑计划。解析成功后结果会传给Catalyst优化器（一组尝试通过谓词下推、投影优化逻辑计划的规则，可以通过继承Catalyst来增加领域特定优化规则）生成优化的逻辑计划

```bob-svg
                                                          
+------+      +--------------+           +--------------+  logical       +--------------+
| user |      | unresolved   | analysis  | resolved     |  optimization  | optimized    |
| code |----->| logical plan |---------->| logical plan |--------------->| logical plan |
+------+      +--------------+     ^     +--------------+       ^        +--------------+
                                   |                            |
                              +----+----+             +---------+----------+
                              | catalog |             | catalyst optimizer |
                              +---------+             +--------------------+
```

2. 生成物理计划（Physical Planning）：优化的逻辑计划会生成多个物理计划（Physical plan，也称Spark plan，指定逻辑计划如何在集群执行），通过成本模型（cost model）比较它们并筛选出最优的物理计划，最终结果是一系列RDD和转换（所以Spark有时也被视作编译器，将DataFrame、Dataset或SQL表达的查询转换成RDD转换）

```bob-svg

+--------------+         +----------+  cost    +---------------+
| Optimized    |         | Physical |  model   | Best          |
| logical plan |-------->| Plans    |--------->| Physical Plan |
+--------------+         +----------+          +---------------+
```
3. 执行（Execute）：Spark执行RDD代码，并在运行时进行一系列优化（生成本地Java字节码以在执行中移除整个任务或阶段），最终返回结果

Schema定义DataFrame包含列的名称和类型（什么位置存储了什么类型的数据），可以手动显式定义schema或从数据源读取schema(schema-on-read)。Schema表示为包含若干字段（表示为StructField对象，包含名称、类型、是否包含null值的标志以及可选的列元数据（metadata），元数据存储着列信息）的StructType对象。

Column是表示通过表达式逐行计算得到的值的逻辑结构。`col(columnName)`^[`col()`、`column()`、`expr()`方法均在org.apache.spark.sql.functions下]和`column(columnName)`方法（列名可以使用columnName.field来访问结构中的数据，列名中包含保留字符（如`.`）或关键字时需要使用反引号包围）用于构造和引用列（Spark提供了`$"columnName"`、`'columnName`语法糖来引用列），`expr(expr: String)`方法可以把字符串表达式转换为它所表示的列，也可以调用DataFrame对象的`col()`方法来引用它的列（或者`df(columnName)`）。列是未解析的直到和catalog中的列信息进行比较，列和表的解析发生在analyzer阶段。`printSchema()`方法用于输出DataFrame包含的列，`columns`属性用于获取DataFrame包含的列。

Expression是在DataFrame中一条记录的一个或多个值上的一组转换（类似一个函数，接收一个或多个列名称，解析它们，然后应用更多表达式为数据集中的每条记录创建一个值）。Column类提供了表达式功能的一个子集，可以直接对列对象应用转换，`expr()`方法用于从字符串解析转换和列引用，并将其传递到进一步的引用中。**列就是表达式**；**Columns and transformations of those columns compile to the same logical plan as parsed expressions**

Row用于表示DataFrame中的一条记录，在内部表示为字节数组，用户只能通过列表达式去操作Row，可以通过指定每列的值来手动创造Row对象（只有DataFrame有Schema，Row自身没有，所以创建Row时各列值的顺序必须和DataFrame的Schema一致），可以通过位置（`row(position)`）获取Row中的数据，对于Scala或Java，需要使用帮助方法(`row.getXxx(position)`)或显式转换（`row(position).asInstanceOf[ScalaType]`）将值转换为正确的类型。

### Spark类型

Spark使用Catalyst引擎在生成计划和作业运行过程中维护自己的类型信息，将开发语言中的表达式转化成相同类型的Spark内部Catalyst表示。

Scala类型与Spark类型对应关系

在Scala中，通过语句`import org.apache.spark.sql.types._`引入Spark类型。

|Spark类型|对应Scala类型|在Scala中创建对应Spark类型的API|
|---|---|---|
|ByteType|Byte|ByteType|
|ShortType|Short|ShortType|
|IntegerType|Int|IntegerType|
|LongType|Long|LongType|
|FloatType|Float|FloatType|
|DoubleType|Double|DoubleType|
|DecimalType|java.math.BigDecimal|DecimalType|
|StringType|String|StringType|
|BinaryType|Array\[Byte]|BinaryType|
|BooleanType|Boolean|BooleanType|
|TimestampType|java.sql.Timestamp|TimestampType|
|DateType|java.sql.Date|DateType|
|ArrayType|scala.collection.Seq|ArrayType(elementType)\[, containsNull])（containsNull默认为true）|
|MapType|scala.collection.Map|MapType(keyType, valueType\[, valueContainsNull])|
|StructType|org.apache.spark.sql.Row|StructType(fields)（fields是StructFields数组，StructField不允许重名）|
|StructField|该字段类型在Scala中对应的Spark类型|StructField(name, dataType\[, nullable])（nullable默认为true）|

在Java中，通过语句`import org.apache.spark.sql.types.DataTypes`引入Spark类型。

|Spark类型|对应Java类型|在Java中创建对应Spark类型的API|
|---|---|---|
|ByteType|byte或Byte|DataTypes.ByteType|
|ShortType|short或Short|DataTypes.ShortType|
|IntegerType|int或Integer|DataTypes.IntegerType|
|LongType|long或Long|DataTypes.LongType|
|FloatType|float或Float|DataTypes.FloatType|
|DoubleType|double或Double|DataTypes.DoubleType|
|DecimalType|java.math.BigDecimal|DataTypes.createDecimalType()/DataTypes.createDecimalType(precision, scale)|
|StringType|String|DataTypes.StringType|
|BinaryType|byte[]|DataTypes.BooleanType|
|BooleanType|boolean或Boolean|DataTypes.BooleanType|
|TimestampType|java.sql.Timestamp|DataTypes.TimestampType|
|DateType|java.util.List|java.sql.Date|DataTypes.DateType|
|ArrayType|java.util.List|DataTypes.createArrayType(elementType)|
|MapType|java.util.Map|DataTypes.createArrayType(keyType, valueType)|
|StructType|org.apache.spark.sql.Row|DataTypes.createStructType(fields)（fields是StructFields列表或数组，StructField不允许重名）|
|StructField|该字段类型在Java中对一个的Spark类型|DataTypes.createStructField(name, dataType, nullable)|

`lit()`方法（在org.apache.spark.sql.functions中）用于将其他语言中的类型转换为对应的Spark类型。

#### Boolean

Boolean语句由与（and）、或（or）、真（true）、假（false）四种元素组成，用于过滤数据。

比较运算符[^该部分中的强调方法均在org.apache.spark.functions下，其余在Column类中]：

小于：`<(other: Any)`、`lt(other: Any)`

小于等于：`<=(other: Any)`、`leq(other: Any)`

大于：`>(other: Any)`、`gt(other: Any)`

大于等于：`>=(other: Any)`、`geq(other: Any)`

相等：`<=>(other: Any)`（null安全）、`eqNullSafe(other: Any)`、`===(other: Any)`、`equalTo(other: Any)`

不等于：`=!=(other: Any)`、`notEqual(other: Any)`

逻辑运算符：

与：`&&(other: Any)`、`and(other: Column)`

或：`||(other: Any)`、`or(other: Column)`

Spark会将连续的Boolean语句展开成一个语句并同时执行它们（等价于and语句）。

#### Number

算术运算符：

加：`+(other: Any)`、`plus(other: Any)`

减：`-(other: Any)`、`minus(other: Any)`

乘：`*(other: Any)`、`multiply(other: Any)`

除：`/(other: Any)`、`divide(other: Any)`

取余：`%(other: Any)`、`mod(other: Any)`

乘方：**`pow(first: Any, second: Any)`**

取整：**`round(e: Column)`**（向上取整）、**`bround(e: Column)`**（向下取整）

#### String

正则表达式提取/替换：**`regexp_extract(e: Column, exp: String, groupIdx: Int)`**、**`regexp_replace(e: Column, pattern: String, replacement: String)`**、**`regexp_replace(e: Column, pattern: Column, replacement: Column)`**

判断是否包含子字符串：`contains(other: Any)`、**`instr(e: Column, substring: String)`**（返回子字符串第一次出现的位置）

大写化/小写化：**`initcap(e: Column)`**（每个单词首字母转大写）、**`lower(e: Column)`**（全部字符转小写）、**`upper(e: Column)`**（全部字符转大写）

添加/移除空格：**`lpad(e: Column, len: Int, pad: String)`**、**`rpad(e: Column, len: Int, pad: String)`**、**`trim(e: Column)`**、**`ltrim(e: Column)`**、**`rtrim(e: Column)`**

替换指定字符为其他字符：**`translate(e: Column, matchingString: String, replaceString: String)`**

#### Dates 和 Timestamp

Spark的TimestampType类精度为秒，处理毫秒或微秒时需要使用long。

获取当前日期：**`current_date()`**

获取当前时间戳：**`current_timestamp()`**

日期加减：**`date_add(e: Column, days: Int)`**、**`date_sub(e: Column, days: Int)`**

日期差值：**`datediff(end: Column, start: Column)`**、**`months_between(end: Column, start: Column)`**

字符串转日期：**`to_date(e: Column)`**、**`to_date(e: Column, fmt: String)`**（fmt为Java SimpleDateFormat标准）

字符串转时间戳：**`to_timestamp(e: Column)`**、**`to_timestamp(e: Column, fmt: String)`**

#### Null

Spark对null值有优化，推荐使用null表示DataFrame中缺失或空数据。操作null值最基本的方法是对DataFrame使用`.na`子包。对null值主要有两种处理方法：显式删除null值、使用指定值填充null值（全局或指定列）。

从多个列中选择第一个非null列：**`coalesce(e: Column*)`**

删除包含null值的行：**`drop`**

根据指定列的当前值替换成其他值：**`replace`**

指定null值在有序DataFrame中的顺序：`asc_nulls_first`、`desc_nulls_first`、`asc_nulls_last`、`desc_nulls_last`

#### 复杂类型

复杂类型用于以更有意义的方式组织数据，包括结构（struct）、数组（array）、映射（map）。

创建struct：**`struct(colName: String, colName: String*)`**、**`struct(cols: Column*)`**

获取struct中的数据：`getField(fieldName: String)`

创建array：**`array(cols: Column*)`**、**`array(colName: String, colNames: String*)`**、**`split(str: Column, pattern: String, limit: Int)`**、**`split(str: Column, pattern: String)`**

获取array元素数目：**`siz(e: Column)`**

判断数组是否包含某值：**`array_contains(column: Column, value: Any)`**

拆分数组（每行一个元素）：**`explode(e: Column)`**

移除数组中的重复元素：**`array_distinct(e: Column)`**

创建map：**`map(cols: Columns)`**

#### JSON

提取JSON中的数据：**`get_json_object(e: Column, path: String)`**、**`json_tuple(json: Column, fields: String*)`**

将StructType转换为JSON字符串：**`to_json(e: Column)`**

### DataFrame

选择列：`DataFrame#select(col: String, cols: String*)`[^对于Scala，DataFrame#表示DataSet类的untyped transformations]、`DataFrame#select(cols: Column*)`、`DataFrame#selectExpr(exprs: String*)`用于操作列（`select()`方法中字符串和列对象不能混用）。`DataFrame#selectExpr(exprs: String*)`方法用于表达语法select后跟着一组expr（`select expr[, expr]*），expr支持任何非聚合SQL语句（如`*`），但可以使用对整个DataFram的聚合函数。

添加列：`DataFrame#withColumn(colName: String, col: Column)`

重命名列：`DataFrame#withColumnRenamed(existingName: String, newName: String)`、`Column#alias(alias: String)`、`Column#as(alias: String)`

移除列：`DataFrame#drop(col: Column)`、`DataFrame#drop(colName: String)`、`DataFrame#drop(colNames: String*)`

强制类型转换：`Column#cast(to: String)`、`Column#cast(to: DataType)`

筛选/过滤行：`DataFrame#filter(func: (T) => Boolean)`、`DataFrame#filter(conditionExpr: String)`、`DataFrame#where(conditionExpr: String)`（需要指定多个AND过滤表达式时，没有必要把它们放在同一个表达式中，Spark会同时执行所有连续的过滤操作）

去重操作：`DataFrame#distinct()`

随机取样：`DataFrame#sample(withReplacement: Boolean, fraction: Double, seed: Long)`

随机分片：`DataFrame#randomSplit(weights: Array[Double], seed: Long)`

拼接追加行：`DataFrame#union(other: DataFrame)`（两个DataFrame必须有相同的Schema和列数，根据位置拼接而不是Schema）、`DataFrame#unionByName(other: DataFrame, allowMissingColumns: Boolean)`

排序：`DataFrame#sort(sortExprs: Column*)`、`DataFrame#sort(sortCol: String, sortCols: String*)`、`DataFrame#orderBy(sortExprs: Column*)`、`DataFrame#orderBy(sortCol: String, sortCols: String*)`，可以使用`asc()`、`desc()`方法指定列的排序顺序。`DataFrame#sortWithinPartitions(sortExprs: Column*)`、`DataFrame#sortWithinPartitions(sortCol: String, sortCols: String*)`方法用于对DataFrame每个分区排序

取部分行：`DataFrame#limit(n: Int)`

分区操作：`DataFrame#repartition(numPartitions: Int)`、`DataFrame#repartition(partitionExprs: Column*)`、`DataFrame#repartition(numPartitions: Int, partitionExprs: Column*)`、`DataFrame#coalesce(numPartitions: Int)`（不会触发全局shuffle而是尝试合并分区）

收集行到Driver：`DataFrame#take(n: Int)`、`DataFrame#show()`（输出前20行）、`DataFrame#show(numRows: Int)、`DataFrame#collect()`（收集所有行）、`DataFrame#toLocalIterator()`（用于顺序访问整个数据集）

#### 数据源

Spark有6种核心数据源（CSV、JSON、Parquet、ORC、JDBC/ODBC connections、Plain-text files）和社区开发的数百计外部数据源（Cassandra、HBase、MongoDB、XML等）。

读数据的基本结构：

```scala
DataFrameReader.format(...).option("key", "value").schema(...).load()
```

Spark中读取数据的基础是`DataFrameReader`，可以通过`SparkSession#read`属性获取，需要设置DataFrameReader：格式（format）、Schema、读取模式（read mode）以及其他选项。`format()`指定数据格式（可选，默认为Parquet）；`option()`设置读取数据时的配置；`schema()`指定Schema（数据源提供Schema时使用Schema接口时可选）。`format()`、`options()`、`schema()`都返回DataFrameReader。

读取模式指定遇到格式错误的记录时如何处理：

+ permissive：将格式错误记录的所有字段置为null，并将该记录放到字符串列`_corrupt_record`中，此为默认值

+ dropMalformed：删除格式错误的记录

+ failFast：遇到格式错误的记录时直接失败

写数据的基本结构：

```scala
DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()
```

Spark中写数据的基础是`DataFrameWriter`，可以通过`DataFrame#write`属性获取，`format()`指定数据格式（可选，默认是Parquet）；`option()`设置写数据时的配置；`partitionBy()`、`bucketBy()`、`sortBy()`仅支持文件类数据源，用于控制输出文件布局。

写数据模式指定目标目录已存在时如何处理：

+ append：将输出文件追加到目标目录

+ overwrite：覆盖目标目录中已存在的文件

+ errorIfExists：目标目录中文件已存在时抛出错误并写失败，默认值

+ ignore：目标目录中文件已存在时不再写入

选项`maxRecordsPerFile`设置Spark写文件时每个文件包含记录数的最大值。

##### JDBC/ODBC

|配置项|描述|
|---|---|
|url|JDBC url，可以在url中定义数据源特定连接属性|
|dbtable|JDBC表|
|driver|JDBC驱动的类名|
|partitionColumn, lowerBound, upperBound|必须同时指定这三个配置项，partitionColumn指定分区列，类型必须是数字（numeric），日期（date），或时间戳（timestamp），lowerBound和upperBound用于指定partition stride，而不是过滤表中的行|
|numPartitions|用于并行读写表的分区的最大数目，也指定了并发JDBC连接的最大数目，如果分区超过了该值，将调用coalesce(numPartitions)减少分区数目|
|fetchsize|JDBC拉取大小，决定一次拉取多少行，仅适用于读数据|
|batchsize|JDBC批大小，决定一些插入多少行，仅适用于血数据，默认是1000|
|isolationLevel|事务隔离级别，可以取值：`NONE`、`READ_COMMITTED`、`READ_UNCOMMITTED`、`REPEATABLE_READ`、`SERIALIZABLE`，默认是`READ_UNCOMMITTED`，仅适用于写数据|
|truncate|为true时，如果写模式为SaveMode.Overwirte，Spark将清空（truncate）存在的表而不是删除后再建，更高效，仅适用于写数据|
|createTableOptions||
|createTableColumnTypes||

Spark创建DataFrame前会尽力在数据库中过滤数据，会将对DataFrame的过滤下推到数据库（执行计划的PushedFilters部分可以看到），Spark并不能将它的所有含义翻译为SQL数据库中的函数，此时可以使用子查询指定`dbtable`（必须使用小括号包围子查询并重命名）。

#### 聚合操作

聚合操作需要指定分组操作（grouping）和聚合函数（aggregation function）。

Spark提供了以下几种分组类型：

+ DataFrame级别聚合（直接在select语句中使用聚合函数来聚合整个DataFrame）
+ group-by
+ window（窗口函数）
+ grouping sets/rollup/cube

分组操作返回RelationalGroupedDataset，之后对RelationalGroupedDataset通过`agg()`应用聚合函数（`RelationalGroupedDataset#agg(expr: Column, exprs: Column*)`、`RelationalGroupedDataset#agg(exprs: Map[String, String])`、`RelationalGroupedDataset#agg(aggExpr: (String, String), aggExprs: (String, String)*)`）。

窗口函数基于分组（frame）对表中的每一行生成一个结果，一行数据可以在一个或多个分组中，直接在select中以`windowFun().over(windowSpec)`的形式指定窗口函数列（支持`Column#alias()`等重命名操作）。Spark支持三种窗口函数：ranking函数（`row_number()`、`rank()`、`dense_rank()`、`percent_rank()`）、analytic函数（`lag()`、`lead()`、`nth_value()`、`ntile()`）和聚合函数。通过`Window`中的静态方法（`Window.partitionBy(cols: Column*)`、`Window.partitionBy(colName: String, colNames: String*)`、`Window.orderBy(cols: Column*)`、`Window.orderBy(colName: String, colNames: String*)`、`Window.rowsBetween(start: Long, end: Long)`、`Window.rangeBetween(start: Long, end: Long)`）创建WindowSpec对象，使用`WindowSpec#partitionBy(cols: Column*)`（或`WindowSpec#partitionBy(colName: String, colNames: String*)`）指定分组，使用`WindowSpec#orderBy(cols: Column*)`（或`WindowSpec#orderBy(colName: String, ColNames: String*)`）指定分组内排序，使用`WindowSpec#rowsBetween(start: Long, end: Long)`（或`WindwoSpec#rangeBetween(start: Long, end: Long)`）指定窗口边界。`rowsBetween()`/`rangeBetween()`中的`start`、`end`有3个特殊的取值：`Window.currentRow`（表示当前行）、`Window.unboundedFollowing`（表示分组中的最后一行，等价于SQL中的`UNBOUNDED FOLLOWING`）以及`Window.nboundedPreceding`（表示分组中的第一行，等价于SQL中的`UNBOUNDED PRECEDING`）。

grouping sets/rollup/cube是对group-by的增强，用于对多个分组方式进行聚合（仅SQL支持grouping sets）。grouping sets/rollup/cube依赖null值决定聚合层级，需要排除分组列中的null值以免出错。聚合函数`grouping(colName: String)`/`grouping(column: Column)`用于查询group by中的列是否被聚合（1表示是，0表示否），聚合函数`grouping_id(colName: String, colNames: String*)`/`grouping_id(cols: Column*)`（参数为空时表示所有分组列）用于获取分组级别（$$(grouping(c1) << (n-1)) + (grouping(c2) << (n-2)) + ... + grouping(cn)$$）。

##### 聚合函数

聚合函数一般在`org.apache.spark.sql.functions`包中。

统计分组内元素数目：`count(columnName: String)`（列名可以使用`*`或`1`来统计每1行，`count(*)`将包含所有值都为null的行，统计单独一列时则不会包含null值）、`count(column: Column)`

统计分组内去重元素数目：`countDistinct(columnName: String, columnNames: String*)`，`countDistinct(expr: Column, exprs: Column*)`、`count_distinct(expr: Column, exprs: Column*)`

统计分组内去重元素的近似数目：`approx_count_distinct(column: Column)`、`approx_count_distinct(columnName: String)`、`approx_count_distinct(column: Column, rsd: Double)`、`approx_count_distinct(columnName: String, rsd: Double)`

获取分组内指定列的第一个元素：`first(columnName: String)`、`first(column: Column)`、`first(columnName: String, ignoreNulls: Boolean)`、`first(column, Column, ignoreNulls: Boolean)`

获取分组内指定列的最后一个元素：`last(columnName: String)`、`last(column: Column)`、`last(columnName: String, ignoreNulls: Boolean)`、`last(column, Column, ignoreNulls: Boolean)`

统计分组内指定列的最大值：`max(columnName: String)`、`max(column: Column)`

统计分组内指定列的最小值：`max(columnName: String)`、`max(column: Column)`

计算分组内指定列的总和：`sum(columnName: String)`、`sum(column: Column)`

计算分组内指定列去重元素的总和：`sum_distinct(column: Column)`

计算分组内指定列的平均值：`avg(columnName: String)`、`avg(column：Column)`

收集分组内指定列的值：`collect_list(columnName: String)`、`collect_list(column: Column)`、`collect_set(columnName: String)`、`collect_set(column: Column)`

###### 自定义聚合函数

创建自定义函数，需要继承类`UserDefinedAggregateFunction`，并且实现以下方法：

+ `inputSchema`：指定输入参数的`StructType`
+ `bufferSchema`：指定UDAF中间结果的`StructType`
+ `dataType`：指定结果的`DataType`
+ `deterministic`：Boolean值，指定相同输入UDAF是否返回相同结果
+ `initialize`：初始化aggregation buffer的值
+ `update`：根据输入的行数据更新aggregation buffer
+ `merge`：合并两个aggregation buffer
+ `evaluate`：生成聚合的最终结果

#### 关联操作

join表达式决定两行数据是否关联，join类型决定结果集内容。

join类型：

+ inner join：保留左右两侧数据集中关联的上的行
+ outer join：保留左右两侧数据集中的所有行
+ left outer join：保留左侧数据集中的所有行
+ right outer join：保留右侧数据集中的所有行
+ left semi join：仅保留左侧数据集中在右侧数据集也出现的行
+ left anti join：仅保留左侧数据集中未在右侧数据集出现的行
+ natural join：根据两个数据集中的同名列进行关联
+ cross join：笛卡尔积（Cartesian join）（`DataFrame#corssJoin(right: DataFrame)`）

`DataFrame#join(right: DataFrame, usingColumn: String)`、`DataFrame#join(right: DataFrame, usingColumns: Seq[String], joinType: String)`、`DataFrame#join(right: DataFrame, joinExprs: Column, joinType: String`，joinType可选，支持`inner`, `cross`, `outer`, `full`, `fullouter`, `full_outer`, `left`, `leftouter`, `left_outer`, `right`, `rightouter`, `right_outer`, `semi`, `leftsemi`, `left_semi`, `anti`, `leftanti`, `left_anti`，默认为`inner`。

任何返回Boolean类型的表达式都是有效的join表达式。

### DataSet

通过DataSet可以定义每行数据的类型（对于Scala将是case class object，对于Java将是Java Bean），DataSet一般被视作有类型的API集合。Encoder用于将开发语言的类型映射到Spark内部类型系统，指示Spark在运行时生成代码来序列化开发语言中的对象为二进制结构。使用Dataset API时，Spark会将行数据从Spark Row格式转换为指定的对象（case class或Java class），这会降低操作的效率但是提高灵活度，建议在需要类型安全或所需操作不能表达为DataFrame操作时使用Dataset。

方法`Dataset#joinWith(other: Dataset, condition: Column, joinType: String)`（joinType可选）返回`Dataset[Tuple2]`，`Tuple2`为两个数据集中相互匹配的行。

### Spark SQL

SQL（Structured Query Language）是用来表达对数据进行关系操作的领域特定语言，Spark实现了ANSI SQL:2003的子集。

Spark SQL通过Hive Metastore获取hive表信息，可以通过以下方式执行Spark SQL：

+ Spark SQL CLI：在命令行以本地模式执行Spark SQL，不能连接SparkSQL Thrift JDBC Server，在spark目录中执行`./bin/spark-sql`来启动Spark SQL CLI，并将`hive-site.xml`、`core-site.xml`和`hdfs-site.xml`放置在`conf/`中来配置hive
+ Spark SQL接口：通过`SparkSession#sql()`方法以编程方式执行Spark SQL
+ SparkSQL Thrift JDBC/ODBC Server：Spark提供了JDBC接口用于远程执行Spark SQL，在spark目录中执行`./sbin/start-thriftserver.sh`来启动Spark JDBC/ODBC服务

## RDD、DataFrame、Dataset之间的转换

DataFrame -> Dataset：Scala中，`DataFrame#as[TargetClass]`；Java中，`DataFrame#as(Encoders.bean(TargetClass))`

Dataset -> DataFrame：`Dataset#toDF()`、`Dataset#toDF(colNames: String*)`

Dataset -> RDD：`Dataset#rdd`（`Dataset[T]`将转换为`RDD[T]`）

DataFrame -> RDD：`DataFrame#rdd`（结果为`RDD[Row]`，需要将Row对象转换为所需的数据类型或从Row对象中提取需要的值）

RDD -> DataFrame：`RDD#toDF()`[^ `RDD#toDF()`、`RDD#toDS()`需要引入`import sparkSession.implicits._`]、`RDD#toDF(colNames: String*)`、`SparkSession#createDataFrame(rdd: RDD[T], beanClass: Class[T])`、`SparkSession#createDataFrame(rdd: RDD[Row], schema: StructType)`

RDD -> Dataset：`RDD#toDS()`、`SparkSession#createDataset(rdd: RDD[T])`
