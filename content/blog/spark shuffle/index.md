---
title: Spark Shuffle
date: "2022-06-16"
description: 
tags: 
---

Spark Shuffle机制是指上游和下游Stage之间的数据传递过程，即运行在不同Stage、不同节点上的task间如何进行数据传递。Shuffle机制分为Shuffle Write和Shuffle Read，Shuffle Write主要解决上游Stage输出数据的分区问题，Shuffle Read主要解决下游Stage从上游Stage获取数据、重新组织、并为后续操作提供数据的问题。

Shuffle机制的设计与实现面临以下问题：

+ 计算的多样性：在进行Shuffle Write/Read时，可能需要对数据进行一定的计算（`groupByKey()`需要将Shuffle Read的\<K, V\>记录**聚合**为\<K, list\<V\>>记录，`reduceByKey()`需要在Shuffle Write端进行**combine**，`sortByKey()`需要对Shuffle Read的数据按照Key进行**排序**）。Shuffle机制需要根据不同数据操作的特点，灵活地构建Shuffle Write/Read过程，并合理安排聚合函数、数据分区、数据排序的执行顺序。
+ 计算的耦合性：用户自定义聚合函数的计算过程和数据的Shuffle Write/Read过程耦合在一起（`aggregateByKey(seqOp, combOp)`在Shuffle Write时需要调用seqOp进行`combine()`，在Shuffle Read时需要调用combOp进行聚合），需要考虑聚合函数的调用时机（先读取数据再进行聚合，边读取数据边进行聚合）
+ 中间数据存储问题：在Shuffle机制中需要对数据进行重新组织（分区、聚合、排序），也需要进行一些计算（执行聚合函数），需要考虑Shuffle Write/Read过程中中间数据的表示、组织以及存放方式。

## 设计思想

Shuffle机制中最基础的两个问题是：数据分区问题（Shuffle Write阶段）和数据聚合问题（Shuffle Read阶段）。

**数据分区** 在Shuffle Write阶段，根据下游stage的task个数确认分区个数（分区个数由用户通过方法中的可选参数`numPartitions`自定义，默认是parentRDD分区个数的最大值），对map task[^在ShuffleDependency情况下，将上游stage成为map stage，将下游stage成为reduce stage，map stage包含多个map task，reduce stage包含多个reduce task]输出的每一个\<K, V\>记录，根据Key计算partitionId，具有不同partitionId的记录被输出到不同的分区（文件）中

**数据聚合** 有两种方法在Shuffle Read阶段将相同Key的记录放在一起，并进行必要的计算。一是两步聚合（two-phase aggregation），先将不同task获取到的\<K, V\>记录存放到HashMap中，HashMap中的Key是K，Value是list(V)，然后对于HashMap中每一个\<K,list(V)\>记录，使用func计算得到\<K, func(list(V))\>记录，优点是逻辑清晰，容易实现，缺点是所有记录都会先存放在HashMap中，占用内存空间较大，另外对于包含聚合函数的操作，效率较低。一是在线聚合（online aggregation），在每个记录加入HashMap时，同时进行func()聚合操作，并更新相应的聚合结果，对于每一个新来的\<K, V\>记录，首先从HashMap中get出已经存在的结果`V'=HashMap.get(K)`，然后执行聚合函数得到新的中间结果`V''=func(V, V')`，最后将V''写入HashMap中，即HashMap.put(K, V'')，优点是减少内存消耗，将Shuffle Read和聚合函数计算耦合在一起，可以加速计算，但是对于不包含聚合函数的操作，在线聚合和两步聚合没有差别。

**combine操作** combine操作发生在Shuffle Write阶段，与Shuffle Read端的聚合过程相似。进行combine操作的目的是减少Shuffle的数据量，只有包含聚合函数的数据操作（如`reduceByKey()`、`foldByKey()`、`aggregateByKey()`、`combineByKey()`、`distinct()`）需要进行map()端combine，采用Shuffle Read端基于HashMap的解决方案，首先利用HashMap进行combine，然后对HashMap中每一个record进行分区，输出到对应的分区文件中

**数据排序** 对于`sortByKey()`、`sortBy()`等排序操作，在Shuffle Read阶段必须执行排序操作，理论上Shuffle Write阶段不需要排序，但是如果进行了排序，Shuffle Read阶段获取到的将是已经部分有序的数据，可以减少Shuffle Read阶段排序的复杂度。有三种排序方法：

+ 先排序再聚合：先使用线性数据结构（如Array），存储Shuffle Read的\<K, V\>记录，然后对Key进行排序，排序后的数据可以直接从前到后进行扫描聚合，不需要使用HashMap进行hash-based聚合，优点既可以满足排序要求又可以满足聚合要求，缺点是需要较大的内存空间来存储线性数据结构，同时排序和聚合过程不能同时进行，即不能使用在线聚合，效率较低，Hadoop MapReduce采用的是此中方案

+ 排序和聚合同时进行：使用带有排序功能的Map（如TreeMap）来对中间数据进行聚合，每次Shuffle Read获取到一个记录，就将其放入TreeMap中与现有的记录进行聚合，优点是排序和聚合可以同时进行；缺点是相比HashMap，TreeMap的排序复杂度较高

+ 先聚合再排序：在基于HashMap的聚合方案的基础上，将HashMap中的记录或记录的引用放入线性数据结构中进行排序，优点是聚合和排序过程独立，灵活性较高，缺点是需要复制数据或引用，空间占用较大

**内存不足问题** 使用HashMap对数据进行combine和聚合，在数据量大的时候，会出现内存溢出，即可能出现在Shuffle Write阶段，也可能出现在Shuffle Read阶段，使用内存+磁盘混合存储方案，先在内存（如HashMap）中进行数据聚合，如果内存空间不足，则将内存中的数据溢写（spill）到磁盘上，在进行下一步操作之前对磁盘上和内存中的数据进行再次聚合（全局聚合），为了加速全局聚合，需要将数据溢写到磁盘上时进行排序（全局聚合按顺序读取溢写到磁盘上的数据，减少磁盘I/O）。

## 实现

Spark可以根据不同数据操作的特点，灵活构建合适的Shuffle机制。在Shuffle机制中Spark典型数据操作的计算需求如下：

|包含ShuffleDependency的操作|Shuffle Write端combine|Shuffle Write端按Key排序|Shuffle Read端combine|Shuffle Read端按Key排序|
|---|---|---|---|---|
|`partitionBy()`|x|x|x|x|
|`groupByKey()`、`cogroup()`、`join()`、`coalesce()`、`intersection()`、`subtract()`、`subtractByKey()`|x|x|v|x|
|`reduceByKey()`、`aggregateByKey()`、`combineByKey()`、`foldByKey()`、`distinct()`|v|x|v|x|
|`sortByKey()`、`sortBy()`、`repartitionAndSortWithinPartitions()`|x|x|x|v|
|未来系统可能支持的或者用户自定义的数据操作|v|v|v|v|

### Shuffle Write

在Shuffle Write阶段，数据操作需要分区、聚合和排序3个功能，但每个数据操作只需要其中的部分功能，为了支持所有的情况，Shuffle Write框架的计算顺序为 **map()输出 -> 数据聚合 -> 排序 -> 分区输出。map task每计算出一条记录及其partitionId，就将记录放入类似HashMap的数据结构中进行聚合；聚合完成后，再将HashMap中的数据放入到类似Array的数据结构中进行排序（既可按照partitionId，也可按照partitionId+Key进行排序）；最后根据partitionId将数据写入不同的数据分区中，存放到本地磁盘上。其中，聚合（即combine）和排序过程是可选的。

Spark对不同的情况进行了分类，以及针对性的优化调整，形成了不同的Shuffle Write方式：

**BypassMergeSortShuffleWriter** map()依次输出\<K, V\>记录，并计算其partitionId，Spark根据partitionId将记录依次输出到不同的buffer^[分配buffer的原因是map()输出record的速度很快，需要进行缓冲来减少磁盘I/O]中，每当buffer填满就将记录溢写到磁盘上的分区文件中。优点是速度快，缺点是资源消耗过高，每个分区都需要一个buffer（大小由spark.Shuffle.file.buffer控制，默认为32KB），且同时需要建立多个分区文件进行溢写，当分区数过多（超过200）会出现buffer过大、建立和打开文件数过多的问题。适用于 *map()端不需要聚合（combine）、Key不需要排序* 且分区个数较少（<=spark.Shuffle.sort.bypassMergeThreshold，默认值为200）的数据操作，如`groupByKey(n)`、`partitionBy(n)`、`sortByKey(n)`（n <= 200）

**SortShuffleWriter(KeyOrdering=true)** 建立一个数组结构（PartitionedPairBuffer）来存放map()输出的记录，并将\<K,V\>记录转化为\<(partitionId,K),V\>记录存储，然后按照partitionId+Key对记录进行排序；最后将所有记录写入一个文件中，通过建立索引来标示每个分区。如果数组存放不下，则会先扩容，如果还存放不下，就将数组中的记录排序后溢写到磁盘上（此过程可重复多次），等待map()输出完以后，再将数组中的记录与磁盘上已排序的记录进行全局排序，得到最终有序的记录，并写入分区文件中。优点是只需要一个数组结构就可以支持按照partitionId+Key进行排序，数组大小可控^[同时具有扩容和溢写到磁盘上的功能，不受数据规模限制]，同时只需要一个分区文件就可以标示不同的分区数据^[输出的数据已经按照partitionId进行排序]，适用分区个数很大的情况，缺点是排序增加计算时延。适用于 *map()端不需要聚合（combine）、Key需要排序*、分区个数无限制的数据操作。SortShuffleWriter(KeyOrdering=false)（只按partitionId排序）可以用于解决BypassMergeSortShuffleWriter存在的buffer分配过多的问题，支持不需要map端聚合（combine）和排序、分区个数过大的操作，如`groupByKey(n)`、`partitionBy(n)`、`sortByKey(n)`（n > 200）

**SortShuffleWriterWithCombine** 建立一个HashMap结构（PartitionedAppendOnlyMap，同时支持聚合和排序操作）对map()输出的记录进行聚合，HashMap中的键是partitionId+Key，值是多条记录对应的Value combine后的结果，聚合完成后，Spark对HashMap中的记录进行排序（如果不需要按Key排序，只按partitionId进行排序；如果需要按Key排序，就按partitionId+Key进行排序），最后将排序后的记录写入一个分区文件中。如果HashMap存放不下，则会先扩容为两倍大小，如果还存放不下，就将HashMap中的记录排序后溢写到磁盘上（此过程可重复多次），当map()输出完成以后，再将HashMap中的记录与磁盘上已排序的记录进行全局排序，得到最终有序的记录，并写入分区文件中。优点是只需要一个HashMap结构就可以支持map()端聚合（combine）功能，HashMap大小可控^[同时具有扩容和溢写到磁盘上的功能，不受数据规模限制]，同时只需要一个分区文件就可以标示不同的分区数据，使用分区个数很大的情况，在聚合后使用数组结构排序，可以灵活支持不同的排序需求，缺点是在内存中进行聚合，内存消耗较大，需要额外的数组进行排序，而且如果有数据溢写到磁盘上，还需要再次进行聚合。适用于*map()端聚合（combine）*、需要或者不需要按Key排序、分区个数无限制的数据操作，如`reduceByKey()`、`aggregateByKey()`

BypassMergeSortShuffleWriter使用直接输出模式，适用于不需要map()端聚合（combine），也不需要按Key排序，而且分区个数很少的数据操作。SortShuffleWriter使用基于数组结构的方法来按partitionId或partitionId+Key排序，只输出单一的分区文件，克服了BypassMergeSortShuffleWriter打开文件过多、buffer分配过多的缺点，也支持按Key排序。SortShuffleWriterWithCombine使用基于Map结构的方法来支持map()端聚合（combine），在聚合后根据partitionId或partitionId+Key对记录进行排序，并输出分区文件。

### Shuffle Read

在Shuffle Read阶段，数据操作需要跨节点数据获取、聚合和排序3个功能，但每个数据操作只需要其中的部分功能，为了支持所有的情况，Spark Read框架的计算顺序为 数据获取 -> 聚合 -> 排序输出。reduce task不断从各个map task的分区文件中获取数据，然后使用HashMap结构来对数据进行聚合（aggregate），该过程是边获取数据边聚合，聚合完成后，将HashMap中的数据放入Array结构中按照Key进行排序，最后将排序结果输出或者传递给下一步操作，如果不需要聚合或者排序，则可以去掉相应的聚合或排序过程。

**不聚合、不按Key排序的Shuffle Read** 等待所有的map task结束后，reduce task开始不断从各个map task获取\<K,V\>记录，并将记录输出到一个buffer中（大小为spark.reducer.maxSizeInFlight=48MB），数据获取结束后将结果输出或者传递给下一步操作。优点是逻辑和实现简单，内存消耗很小，缺点是不支持聚合、排序等复杂功能。适用于不需要聚合也不需要排序的数据操作，如partitionBy()

**不聚合、按Key排序的Shuffle Read** 等待所有map task结束后，reduce task开始不断从各个map task获取\<K,V\>记录，并将记录输出到数组结构（ParitionedPairBuffer）中，然后，最数组中的记录按照Key进行排序，并将排序结果输出或者传给下一步操作。当内存无法存下所有的记录时，PartitionedPairBuffer将记录排序后溢写到磁盘上，最后将内存中和磁盘上的记录进行全局排序，得到最终排序后的记录。优点是只需要一个数组结构就可以支持按照Key进行排序，数组大小可控，而且具有扩容和溢写到磁盘上的功能，不受数据规模限制，缺点是排序增加计算时延。适用于不需要聚合，但需要按Key排序的数据操作，如sortByKey()、sortBy()

**支持聚合的Shuffle Read** 等待所有map task结束后，reduce task开始不断从各个map task获取\<K,V\>记录，通过HashMap结构（ExternalAppendOnlyMap，同时支持聚合和排序）对记录进行聚合，HahsMap中的键是记录中的Key，HashMap中的值是多条记录对应的Value聚合后的结果，聚合完成后，如果需要按照Key进行排序，则建立一个数组结构，读取HashMap中的记录，并对记录按Key进行排序，最后将结果输出或者传递给下一步操作。如果HahsMap存放不下，则会先扩容为两倍大小，如果还存放不下，就将HashMap中的记录排序后溢写到磁盘上（此过程可重复多次），当聚合完成以后，将HashMap中的记录和磁盘上已排序的记录进行全局排序，得到最终排序后的记录。优点是只需要一个HashMap结构和数组结构就可以支持聚合和排序功能，HashMap大小可控，而且具有扩容和溢写到磁盘上的功能，不受数据规模限制，缺点是需要在内存中进行聚合^[如果有数据溢写到磁盘上，还需要进行再次聚合]，而且经过HashMap聚合后的数据仍然需要复制到数组结构中进行排序，内存消耗极大。适用于需要聚合、不需要或需要按Key进行排序的数据操作，如reduceByKey()、aggregateByKey()

Shuffle Read使用的技术和数据结构与Shuffle Write过程类似，而且由于不需要分区，过程比Shuffle Write更为简单。如果应用中的数据操作不需要聚合，也不需要排序，那么获取数据后直接输出，对于需要按Key排序的操作，Spark使用基于数组结构的方法来对Key进行排序，对于需要聚合的操作，Spark提供了基于HashMap结构的聚合方法，同时可以再次使用数组结构来支持按照Key进行排序。

### 支持高效聚合和排序的数据结构

Spark为Shuffle Write/Read的聚合和排序过程设计了3中支持高效聚合和排序的数据结构：

+ PartitionedPairBuffer：用于map()和reduce()端数据排序
+ PartitionedAppendOnlyMap：用于map()端聚合及排序
+ ExternalAppendOnlyMap：用于reduce()端聚合及排序

基本思想是在内存中对记录进行聚合和排序，如果存放不下，则进行扩容，如果还存放不下，就将数据排序后溢写到磁盘上，最后将磁盘和内存中的数据进行聚合、排序，得到最终结果。Shuffle机制中使用的数据结构的两个特征：一是只需要支持记录的插入和更新操作，不需要支持删除操作；二是只有内存放不下时才需要溢写到磁盘上。

**PartitionedPairBuffer** 本质上是一个基于内存+磁盘的数组，随着数据添加，不断扩容，当到达内存限制时，就将数组中的数据按照partitionId或partitionId+Key进行排序，然后溢写到磁盘上，该过程可以进行多次，最后对内存中和磁盘上的数据进行全局排序，输出或者提供给下一个操作

**AppendOnlyMap** 是一个只支持记录添加和对值进行更新的HashMap。只使用数组来存储元素，根据元素的Hash值确定存储位置，使用二次地址探测法（Quadratic probing）来解决Hash冲突。

+ 添加：对于每个新来的\<K,V\>记录，先使用Hash(K)%Size计算其存放位置，如果存放位置为空，就把记录存放到该位置。如果该位置已经被占用，就使用二次探测法来找下一个空闲位置
+ 扩容：如果AppendOnlyMap的利用率达到70%，就扩张一倍^[需要对所有Key进行rehash，重新排列每个Key的位置]
+ 排序：先将数组中所有的\<K,V\>记录转移到数组前端，用begin和end来标示起始位置，然后调用排序算法对[begin,end]中的记录进行排序。对于需要按Key进行排序的操作直接按照Key值进行排序，对于其他操作，只按照Key的Hash值进行排序即可
+ 输出：迭代AppendOnlyMap数组中的记录，从前到后扫描输出即可

**ExternalAppendOnlyMap** Spark基于AppendOnlyMap设计实现了基于内存+磁盘的ExternalAppendOnlyMap，用于Shuffle Read端大规模数据聚合。工作原理是，先持有一个AppendOnlyMap来不断接收和聚合新来的记录，AppendOnlyMap快被装满时检查一下内存剩余空间是否可以扩展，可以的话直接在内存中扩展，否则对AppendOnlyMap中的记录进行排序，然后将记录都溢写到磁盘上^[溢写过程可以出现多次，最终形成多个溢写文件]。等记录都处理完后，将内存中AppendOnlyMap的数据与磁盘上溢写文件中的数据进行全局聚合（merge），得到最终结果。

+ AppendOnlyMap的大小预估：使用增量式的高效估算算法，在每个记录插入或更新时根据历史统计值和当前变化量直接估算当前AppendOnlyMap的大小，算法的复杂度是O(1)。在记录插入和聚合过程中定期对当前AppendOnlyMap中的记录进行抽样，然后精确计算这些记录的总大小、总个数、更新个数及平均值等，并作为历史统计值，之后，每当有记录插入或更新时，会根据历史统计值和历史平均的变化值，增量估算AppendOnlyMap的总大小（`SizeTracker.estimateSize()`方法）。抽样也会定期进行，更新统计值以获得更高的精度

+ 溢写过程与排序：溢写时会根据记录的Key进行排序（没有定义Key的排序方法时按照Key的Hash值进行排序，为了解决Hash冲突问题，在merge-sort的同时会比较Key的Hash值是否相等，以及Key的实际值是否相等），排序是为了方便下一步全局聚合（聚合内存和磁盘上的记录）时可以采用更高效的merge-sort（外部排序+聚合）。

+ 全局聚合：建立一个最小堆或最大堆，每次从各个溢写文件中读取前几个具有相同Key（或相同Key的Hash值）的记录，然后与AppendOnlyMap中的记录进行聚合，并输出聚合后的结果。

**PartitionedAppendOnlyMap** 带有partitionId的ExternalAppendOnlyMap，用于Shuffle Write端对记录进行聚合（combine）。与ExternalAppendOnlyMap的区别是PartitionedAppendOnlyMap中的Key是partitionId+Key，既可以根据partitionId进行排序，也可以根据partitionId+Key进行排序，从而可以在Shuffle Write阶段进行聚合、排序和分区

## 与Hadoop MapReduce的Shuffle机制对比


