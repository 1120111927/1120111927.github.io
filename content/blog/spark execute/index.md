---
title: Spark应用执行
date: "2021-04-19"
description: Spark应用执行时，先建立DAG型的逻辑处理流程（Logical Plan），然后根据数据依赖关系将逻辑处理流程转化为物理执行计划（Physical Plan），最后将任务调度到Executor执行
tags: 逻辑处理流程，物理执行计划，数据依赖关系
---

```toc
ordered: true
class-name: "table-of-contents"
```

在Spark 2.0版本以前，Spark的核心是Core模块。在Spark3.0时，Spark的核心变成了SQL模块。

Spark将一个应用的逻辑处理流程划分为多个作业，每个作业又可以划分为多个调度阶段，每个调度阶段可以生成多个计算任务，而同一阶段中的任务可以同时分发到不同的机器上并行执行。

在非AQE的情况下，Spark作业被提交给DAGScheduler并转换为DAG图。在DAGScheduler通过调用createResultStage方法，依次从下游到上游遍历RDD寻找ShuffleDependency划分父Stage。在Spark中Stage有两种类型：ResultStage是执行pipeline中用于执行操作的最后阶段，而 ShuffleMapStage是中间阶段，它为shuffle写入mapper输出文件。当一个ShuffleMapStage创建时，它会在shuffledIdToMapStage哈希映射中注册，该哈希映射从shuffle依赖ID映射到创建的ShuffleMapStage。当createResultStage方法创建并返回最终结果阶段时，提交最终阶段运行。在提交当前阶段之前，需要先提交所有的父阶段。通过getMissingParentStages方法获取父阶段，该方法首先找到当前阶段的shuffle依赖项，并在shuffledIdToMapStage哈希映射中查找为shuffle依赖项创建的ShuffleMapStage。

在开启AQE的情况下，在prepared Physical stage阶段，将应用InsertAdaptiveSparkPlan规则。当AdaptiveSparkPlanExec被执行时，它会调用getFinalPhysicalPlan方法来启动执行流程。在AQE也会通过递归调用方法来创建Stage, 不过该方法是AdaptiveSparkPlanExec中的createQueryStages方法。如果当前节点是Exchange节点并且其所有子阶段都已实现，则新的QueryStage。然后物化createQueryStages方法返回的新阶段，该阶段在内部将阶段提交给DAGScheduler以独立运行并返回阶段执行的map输出统计信息。然后根据新的统计信息重新优化和重新规划查询计划。然后评估新规划的物理计划的成本并与旧物理计划的成本进行比较。如果新的物理计划比旧的运行成本低，则使用新的物理计划进行剩余处理。

## 系统架构

Spark采用Master-Worker结构：

```bob-svg
               .-----------------------------------------------------.
               |                                                     |
.--------------+----.                                    NodeManager v
| Driver       |    |                                    .-----------------------.
|              v    |                                    | Worker                |
| .--------------.  |       .-----------------.          | .------------+------. |
| | SparkContext |<-+------>| Cluster Manager |<-------->| | Executor   |Cache | |
| '--------------'  |       '----------------+'          | |            '------| |
|              ^    |        Master、        ^           | | .------. .------. | |
|              |    |        ResourceManager |           | | | Task | | Task | | |
'--------------+----'                        |           | | '------' '------' | |
 Client、      |                             |           | '-------------------' |
 Worker、      |                             |           '-----------------------'
 AppMaster     |                             |                        ^
               |                             |                        |
               |                             |                        v
               |                             |                    .--------.
               |                             '------------------->|        |
               |                                                  | Worker |
               '------------------------------------------------->|        |
                                                                  '--------'
```

**集群资源管理器（Cluster Manager）** 是指在集群上获取资源的外部服务，目前有以下几种

+ Standalone：Spark原生的资源管理，由Master负责资源的管理
+ Hadoop Yarn：由YARN中的ResourceManager负责资源的管理
+ Mesos：由Mesos中的Mesos Master负责资源的管理

**应用程序（Application）** 指用户编写的Spark应用程序，包含驱动程序和分布在多个节点上运行的Executor代码，在执行过程中由一个或多个作业组成

**驱动程序（Driver）** 指运行Spark应用的main()函数并且创建SparkContext的进程。创建SparkContext的目的是为了准备Spark应用程序的运行环境。在Spark中由SparkContext负责与ClusterManager通信，进行资源的申请、任务的分配和监控等。通常用SparkContext代表Driver^[Driver独立于Master进程，在client模式下，Driver就是提交应用的client，在cluster模式下，对于Standalone，Driver是某个worker，对于Spark on Yarn，Driver是application node]

**总控节点（Mater）** Master节点^[在Standalone模式中是指执行./sbin/start-master.sh的节点，在Spark on Yarn模式中指的是ResourceManager]上常驻Master进程，负责管理全部Worker节点，如将Spark任务分配给Worker节点，收集Worker节点上任务的运行信息，监控Worker节点的存活状态等

**工作节点（Worker）** 集群中任何可以运行Application代码的节点^[在Standalone模式中指的就是通过Slave文件配置的Worker节点，在Spark on Yarn模式中指的就是NodeManager节点]，Worker节点上常驻Worker进程，负责启停和观察任务的执行情况，如与Master节点通信，启动Executor来执行具体的Spark任务，监控任务运行状态

**执行进程（Executor）** 是Spark计算资源单位，Spark先以Executor为单位占用集群资源，然后可以将具体的计算任务分配给Executor执行。Executor是Worker节点上的一个JVM进程，负责运行Task，并负责将数据存在内存或者磁盘上。在Spark on Yarn模式下，Executror为CoarseGrainedExecutorBackend^[类似于Hadoop MapReduce中的YarnChild]，一个CoarseGrainedExecutorBackend进程有且仅有一个executor对象，负责将Task包装成taskRunner，并从线程池中抽取出一个空闲线程运行Task，每个CoarseGrainedExecutorBackend能并行运行Task的数量取决于分配给它的CPU的个数。Executor内存大小由用户配置，而且Executor的内存空间由多个Task共享

## RDD执行原理

Spark应用执行时，先建立DAG型的逻辑处理流程（Logical plan）^[toDebugString()方法用于输出逻辑处理流程]，逻辑处理流程只是表示输入/输出、中间数据，以及它们之间的依赖关系，并不涉及具体的计算任务。然后根据数据依赖关系将逻辑处理流程转化为物理执行计划（Physical plan），包括调度阶段（Stage）和任务（Task），最后Spark将任务调度到Executor执行，同一个调度阶段的任务可以并行执行，同一个任务的操作可以进行串行、流水线式的处理。

作业（Job）：RDD中由行动操作所生成的一个或多个调度阶段

调度阶段（Stage）：每个作业会因为RDD之间的依赖关系拆分成多组任务集合，称为调度阶段，也叫做任务集（TaskSet）。调度阶段的划分是由DAGScheduler来划分的，调度阶段分为Shuffle Map Stage和Result Stage两种

任务（Task）：分发到Executor上的计算任务，是Spark中最小的计算单位，以线程方式^[为了数据共享和提高执行效率，Spark采用了以线程为最小的执行单位，缺点是线程间会有资源竞争]运行在Executor进程中，执行具体的计算任务

DAGScheduler：是面向调度阶段的调度器，负责任务的逻辑调度，接收Spark应用提交的作业，根据RDD的依赖关系划分成不同阶段的具有依赖关系的调度阶段，并提交调度阶段给TaskScheduler。另外，DAGScheduler记录了哪些RDD被存入磁盘等物化动作，寻求任务的最优化调度^[如数据本地性]，监控运行调度阶段过程，如果某个调度阶段运行失败，则需要重新提交该调度阶段

TaskScheduler：是面向任务的调度器，负责具体任务的调度执行，接收DAGScheduler提交过来的任务集，然后以把任务分发到Work节点运行，由Worker节点的Executor来运行该任务

### 逻辑处理流程

将Spark应用程序转化为逻辑处理流程（RDD及其之间的数据依赖关系），需要解决以下3个问题：

**根据应用程序如何产生RDD，产生什么样的RDD？** Spark用转换方法（transformation）返回（创建）一个新的RDD^[只适用于逻辑比较简单的转换操作，如map()，对于join()、distinct()等复杂的转换操作，Spark依据其子操作的顺序，将生成的多个RDD串联在一起，只返回给用户最后生成的RDD]，并且使用不同类型的RDD（如ParallelCollectionRDD，MapPartitionsRDD，ShuffledRDD等）来表示不同的数据类型、不同的计算逻辑，以及不同的数据依赖。

**如何建立RDD之间的数据依赖关系？** RDD之间的数据依赖关系包括RDD之间的依赖关系和RDD自身分区之间的关联关系两方面。

建立RDD之间的数据依赖关系，需要解决以下3个问题：

1. 如何建立RDD之间的数据依赖关系？对于一元操作，如 rdd2 = rdd1.transformation()，关联关系是 `rdd1 => rdd2`；对于二元操作，如rdd3 = rdd1.join(rdd2)，关联关系是`(rdd1, rdd2) => rdd3`，依此类推

2. 新生成的RDD应该包含多少个分区？新生成RDD的分区个数由用户和parentRDD共同决定，对于join()等转换操作，可以指定生成分区的个数，如果不指定，则取其parentRDD分区个数的最大值；对于map()等转换操作，其生成RDD的分区个数与数据源的分区个数相同

3. 新生成的RDD与其parentRDD中的分区间是什么依赖关系？是依赖parentRDD中的一个分区还是多个分区？分区之间的依赖关系与转换操作的语义有关，也与RDD分区个数有关。根据childRDD的各个分区是否完全依赖parentRDD的一个或者多个分区，Spark将常见数据操作的数据依赖关系分为两大类：窄依赖（Narrow Dependency）和宽依赖（Shuffle Dependency）。

    窄依赖（NarrowDependency）：childRDD中每个分区都依赖parentRDD中的一部分分区。窄依赖可以进一步细分为4种依赖^[Spark代码中没有ManyToOneDependency和ManyToManyDependency，统称为NarrowDependency]：

    + 一对一依赖（OneToOneDependency）：表示childRDD和parentRDD中分区个数相同，并存在一一映射关系，如`map()`、`filter()`
    + 区域依赖（RangeDependency）：表示childRDD和parentRDD的分区经过区域化后存在一一映射关系，如`union()`
    + 多对一依赖（ManyToOneDependency）：表示childRDD中的一个分区同时依赖多个parentRDD中的分区，如具有特殊性质的`cogroup()`、`join()`
    + 多对多依赖（ManyToManyDependency）：表示childRDD中的一个分区依赖parentRDD中的多个分区，同时parentRDD中的一个分区被childRDD中的多个分区依赖，如`catesian()`

    宽依赖（ShuffleDependency）：新生成的childRDD中的分区依赖parentRDD中的每个分区的一部分。

    窄依赖、宽依赖的区别是childRDD的各个分区是否完全依赖parentRDD的一个或者多个分区。对于窄依赖，parentRDD中的一个或者多个分区中的数据需要全部流入childRDD的某一个或者多个分区；对于宽依赖，parentRDD分区中的数据需要一部分流入childRDD的某一个分区，另外一部分流入childRDD的其他分区。

    对数据依赖进行分类有以下用途：一是明确RDD分区之间的数据依赖关系；二是对数据依赖进行分类有利于生成物理执行计划，窄依赖在执行时可以在同一个调度阶段进行流水线（pipeline）操作，而宽依赖则需要进行Shuffle；三是有利于代码实现，OneToOneDependency可以采用一种实现方式，ShuffleDenpendency采用另外一种实现方式。

**如何计算RDD中的数据?** 根据数据依赖关系，对childRDD中每个分区的输入数据应用`transformation(func)`处理，并将生成的数据推送到childRDD中对应的分区即可。不同的转换操作有不同的计算方式（控制流），如`map(func)`每读入一个record就进行处理，然后输出一个record，`recuceByKey(func)`操作对中间结果和下一个record进行聚合计算并输出结果，`mapPartitions()`操作可以对分区中的数据进行多次操作后再输出结果。

### 物理执行计划

Spark生成物理执行计划主要思想是将窄依赖（ShuffleDependency）前后的计算逻辑分开形成不同的调度阶段和任务，具体分为3个步骤：

1. 根据行动（action）操作顺序将应用划分为作业。这一步主要解决何时生成作业以及如何生成作业逻辑处理流程的问题，当应用出现行动操作时，表示应用会生成一个作业，该作业的逻辑处理流程为从输入数据到其最后RDD的逻辑处理流程

2. 根据ShuffleDependency依赖关系将作业划分为调度阶段。对于每个作业，从其最后的RDD往前回溯整个逻辑处理流程，如果遇到NarrowDependency，则将当前RDD的parentRDD纳入，并继续往前回溯。当遇到ShuffleDependency时，停止回溯，将当前已经纳入的所有RDD按照其依赖关系建立一个调度阶段，命名为stage i

3. 在每个调度阶段中根据最后生成的RDD分区个数生成多个计算任务（Task）

**job、stage、task的计算顺序** 作业的提交时间与动作操作被调用的时间有关，当应用程序执行到动作操作时，会立即将作业提交给Spark。stage计算顺序从包涵输入数据的stage开始，从前往后依次执行，仅当上游的stage都执行完成后，再执行下游的stage。stage中每个task都是独立而且同构的，可以并行运行没有先后之分。

**task内部数据的存储与计算问题** 对于读取一个record，计算并生成新record的操作（`map()`、`filter()`等），以流水线计算（pipeline）方式处理连续的这类操作，即读取一个record，按顺序逐个执行这些操作，输出record，再读取record，以此类推。对于需要一次性读取上游分区中所有的record再计算并生成新record的操作（`mapPartitions(func)`、`zipPartitions(func)`等），以计算-回收模式执行，每执行完一个操作，输出新record并回收之前的中间计算数据（上游分区record或中间聚合结果），对于有些函数（如`max()`）来说不需要保存上游所有记录仅需保存中间聚合结果即可

**task间的数据传递与计算问题** 不同stage的task之间通过Shuffle Write + Shuffle Read传递数据。Shuffle Write是指上游task预先将输出数据进行划分（Hash、Range等分区方式），按照分区存放，分区个数与下游task个数一致。Shuffle Read是指上游task输出数据按照分区存放完成后，下游task将属于自己分区的数据通过网络传输获取，然后将来自上游不同分区的数据聚合在一起进行处理

**stage和task命名方式** 在Spark中，stage使用stage i来命名，对于task，如果其输出结果需要进行Shuffle Write，以便传输给下一个stage，那么被称为ShuffleMapTask，而如果其输出结果被汇总到Driver端或者直接写入分布式文件系统，那么被称为ResultTask

### 作业和任务调度流程

1. 提交作业。Spark应用程序进行各种转换操作，通过行动操作触发作业运行，提交作业后根据RDD之间的依赖关系构建DAG图，DAG图提交给DAGScheduler进行解析。行动操作内部调用SparkContext的`runJob()`方法来提交作业，SparkContext的`runJob()`方法经过几次调用后进入DAGScheduler的`runJob()`方法。在DAGScheduler类内部会进行一系列的方法调用，首先是在`runJob()`方法里，调用`submitJob()`方法来继续提交作业，这里会发生阻塞，直到返回作业完成或失败的结果；然后在`submitJob()`方法里，创建一个JobWaiter对象，并借助内部消息处理进行把这个对象发送给DAGScheduler的内嵌类DAGSchedulerEventProcessLoop进行处理；最后在DAGSchedulerEventProcessLoop消息接收方法`OnReceive()`中，接收到JobSubmitted样例类完成模式匹配后，继续调用DAGScheduler的handleJobSubmitted方法来提交作业，在该方法中划分阶段。
2. 划分调度阶段。DAGScheduler从最后一个RDD出发使用广度优先遍历整个依赖树，把DAG拆分成相互依赖的调度阶段，拆分调度阶段是依据RDD的依赖是否为宽依赖，当遇到宽依赖就划分为新的调度阶段^[当某个RDD的操作是Shuffle时，以该Shuffle操作为界限划分成前后两个调度阶段]。每个调度阶段包含一个或多个任务，这些任务形成任务集，提交给TaskScheduler进行调度运行。在SparkContext中提交运行时，会调用DAGScheduler的`handleJobSubmitted()`方法进行处理，在该方法中会先找到最后一个finalRDD，并调用`getParentStage()`方法，`getParentStage()`方法判断指定RDD的祖先RDD是否存在Shuffle操作，如果没有存在shuffle操作，则本次作业仅有一个ResultStage；如果存在Shuffle操作，则需要从发生shuffle操作的RDD往前遍历，找出所有ShuffleMapStage，在`getAncestorShuffleDependencies()`方法中实现，`getAncestorShuffleDependencies()`方法找出所有操作类型是宽依赖的RDD，然后通过`registerShuffleDependencies()`和`newOrUsedShuffleStage()`两个方法划分所有ShuffleMapStage，最后生成finalRDD的ResultStage。当所有调度阶段划分完毕时，这些调度阶段建立起依赖关系，依赖关系是通过调度阶段属性parents[Stage]定义，通过该属性可以获取当前阶段所有祖先阶段，可以根据这些信息按顺序提交调度阶段进行。
    + `getParentStages()`方法实现

    ```Scala
    def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
        val parents = new HashSet[Stage]
        val visited = new HashSet[RDD[_]]

        // 存放等待访问的堆栈，存放的是非ShuffleDependency的RDD
        val waitingForVisit = new Stack[RDD[_]]

        // 定义遍历处理方法，先对访问过的RDD标记，然后根据当前RDD所依赖的RDD操作类型进行不同处理
        def visit(r: RDD[_]) {
            if (!visited(r)) {
                visited += r
                for (dep <- r.dependencies) {
                    dep match {
                        // 所依赖RDD操作类型是ShuffleDependency，需要划分ShuffleMap调度阶段，以调度getShuffleMapStage方法为入口，向前遍历划分调度阶段
                        case shufDep: ShuffleDependenty[_, _, _] => parents += getShuffleMapStage(shufDep, firstJobId)
                        // 所依赖RDD操作类型是非ShuffleDependency，把该RDD压入等待访问的堆栈
                        case _ => waitingForVisit.push(dep.rdd)
                    }
                }
            }
        }

        // 以最后一个RDD开始向前遍历整个依赖树，如果该RDD依赖树存在ShuffleDependency的RDD，则父调度阶段存在，反之不存在
        waitingForVisit.push(rdd)
        while (waitingForVisit.nonEmpty) {
            visit(waitingForVisit.pop())
        }
        parents.toList
    }
    ```

    + `getAncestoreShuffleDependencies()`方法实现

    ```scala
    def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
        val parents = new Stack[ShuffleDependency[_, _, _]]
        val visited = new HashSet[RDD[_]]

        val waitingForVisit = new Stack[RDD[_]]
        def visit(r: RDD[_]) {
            if (!visited(r)) {
                visited += r
                for (dep <- r.dependencies) {
                    dep match {
                        // 所依赖RDD操作类型是ShuffleDependency，作为划分ShuffleMap调度阶段界限
                        case shufDep: ShuffleDependency[_, _, _] =>
                          if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                              parents.push(shufDep)
                          }
                        case _ =>
                    }
                    waitingForVisit.push(dep.rdd)
                }
            }
        }

        // 向前遍历依赖树，获取所有操作类型是ShuffleDependency的RDD，作为划分阶段依据
        waitingForVisit.push(rdd)
        while (waitingForVisit.nonEmpty) {
            visit(waitingForVisit.pop())
        }
        parents
    }
    ```

3. 提交调度阶段。在DAGScheduler的`handleJobSubmitted()`方法中，生成finalStage的同时建立起所有调度阶段的依赖关系，然后通过finalStage生成一个作业实例，在该作业实例中按照顺序提交调度阶段进行执行，在执行过程中通过监听总线获取作业、阶段执行情况。在作业提交阶段调度开始时，在`submitStage()`方法中调用`getMissingParentStage()`方法获取finalStage父调度阶段，如果不存在父调度阶段，则使用`submitMissingTasks()`方法提交执行；如果存在父调度阶段，则把该调度阶段存放到waitingStages列表（等待运行调度阶段列表）中，同时递归调用`submitStage()`。当入口的调度阶段运行完成后相继提交后续调度阶段，在调度前先判断该调度阶段所依赖的父调度阶段的结果是否可用（即运行是否成功）。如果结果都可用，则提交该调度阶段；如果结果存在不可用的情况，则尝试提交结果不可用的父调度阶段。对于调度阶段是否可用的判断是在ShuffleMapTask完成时进行，DAGScheduler会检查调度阶段的所有任务是否都完成了。如果存在执行失败的任务，则重新提交该调度阶段；如果所有任务完成，则扫描waitingStages列表，检查它们的父调度阶段是否存在未完成，如果不存在则表明该调度阶段准备就绪，生成实例并提交运行

    + `submitStage()`方法实现：

    ```scala
    private def submitStage(stage: Stage) {
        val jobId = activeJobForStage(stage)
        if (jobId.isDefined) {
            logDebug("submitStage(" + stage + ")")
            if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
                // 在该方法中，获取调度阶段的父调度阶段，获取的方法是通过RDD的依赖关系向前遍历看是否存在Shuffle操作
                val missing = getMissingParentStages(stage).sortBy(_.id)
                if (missing.isEmpty) {
                    // 如果不存在父调度阶段，直接把该调度阶段提交执行
                    logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
                    submitMissingTasks(stage, jobId.get)
                } else {
                    // 如果存在父调度阶段，把该调度阶段加入到等待运行调度阶段列表中，同时递归调用submitStage方法，直到找到开始的调度阶段，即该调度阶段没有父调度阶段
                    for (parent <- missing) {
                        submitStage += stage
                    }
                    waitingStages += stage
                }
            }
        } else {
            abortStage(stage, "No active job for stage " + stage.id, None)
        }
    }
    ```

    + 检查调度阶段是否可用

    ```scala
    case smt: ShuffleMapTask =>
        val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
        val status = event.result.asInstanceOf[MapStatus]
        val execId = status.location.executorId
        ...

        // 如果当前调度阶段处在运行调度阶段列表中，并且没有任务处于挂起状态，则标记该调度阶段已经完成并注册输出结果的位置
        if (runningStages.contains(shuffleStage) && shuffleStage.pendingTasks.isEmpty) {
            markStageAsFinished(shuffleStage)
            mapOutputTracker.registerMapOutputs(shuffleStage.shuffleDep.shuffleId, shuffleStage.outputLocs.map(list => if (list.isEmpty) null else list.head), changeEpoch = true)
            clearCacheLocs()

            if (shuffleStage.outputLocs.contains(Nil)) {
                // 当调度阶段中存在部分任务执行失败，则重新提交执行
                submitStage(shuffleStage)
            } else {
                // 当该调度阶段没有等待运行的任务，则设置该调度阶段状态为完成
                if (shuffleStage.mapStageJobs.nonEmpty) {
                    val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
                    for (job <- shuffleStage.mapStageJobs) {
                        markMapStageJobAsFinished(job, stats)
                    }
                }
            }

        }
    ```

4. 提交任务。当调度阶段提交运行后，在DAGScheduler的`submitMissingTasks()`方法中，会根据调度阶段Partition个数拆分对应个数任务，这些任务组成一个任务集提交到TaskScheduler进行处理。对于ResultStage生成TesultTask，对于ShuffleMapStage生成ShuffleMapTask。当TaskScheduler收到发送过来的任务集时，在`submitTasks()`方法中（TaskSchedulerImpl类中实现）构建一个TaskSetManager的实例，用于管理这个任务集的生命周期，而该TaskSetManager会放入系统的调度池中，根据系统设置的算法进行调度，最后调用调度器后台进程SparkDeploySchedulerBackend的`reviveOffers()`方法（继承自父类CoarseGrainedSchedulerBackend）分配资源并运行。`reviviOffers()`方法会向DriverEndPoint终端点发送消息，调用其`makeOffers()`方法，在该方法中先会获取集群中可用的Executor，然后发送到TaskSchedulerImpl的`resourceOffers()`方法中进行对任务集的任务分配运行资源，最后提交到`launchTask()`方法中。`resourceOffers()`方法负责资源分配，在分配的过程中会根据调度策略对TaskSetManager进行排序，然后依次对这些TaskSetManager按照就近原则分配资源，按照顺序为PROCESS_LOCAL、NODE_LOCAL、NO_PREF、RACK_LOCAL和ANY。分配好资源的任务提交到CoarseGrainedSchedulerBackend的`launchTasks()`方法中，在该方法中会把任务一个个发送到worker节点上的CoarseGrainedExecutorBackend，然后通过其内部的Executor来执行任务

    + `submitMissingTasks()`方法实现：

    ```scala
    private def submitMissingTasks(stage: Stage, jobId: Int) {
        // ...
        val tasks: Seq[Task[_]] = try {
            stage match {
                // 对于ShuffleMapStage生成ShuffleMapTask任务
                case stage: ShuffleMapStage =>
                  partitionsToCompute.map { id =>
                      val locs = taskIdToLocations(id)
                      val part = stage.rdd.partitions(id)
                      new ShuffleMapTask(stage.id, stage.latestInfo.attemptId, taskBinary, part, locs, stage.internalAccumulators)
                  }
                // 对于ResultStage生成ResultTask任务
                case stage: ResultStage =>
                  val job = stage.resultOfJob.get
                  partitionsToCompute.map { id =>
                      val p: Int = job.partitions(id)
                      val part = stage.rdd.partitions(p)
                      val locs = taskIdToLocations(id)
                      new ResultTask(stage.id, stage.latestInfo.attemptId, taskBinary, part, locs, id, stage.internalAccumulators)
                  }
            }
        } catch {//...}

        if (tasks.size > 0) {
            // 把这些任务以任务集的方式提交到taskScheduler
            stage.pendingPartitions ++= tasks.map(_.partitionId)
            taskScheduler.submitTasks(new TaskSet(tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
            stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
        } else {
            // 如果调度阶段中不存在任务标记，则表示该调度阶段已经完成
            markStageAsFinished(stage, None)
            // ...
        }
    }
    ```

    + `submitTasks()`方法实现：

    ```scala
    overrider def submitTasks(taskSet: TaskSet) {
        val tasks = taskSet.tasks
        this.synchronized {
            // 创建任务集的管理器，用于管理这个任务集的生命周期
            val manager = createTaskSetManager(taskSet, maxTaskFailures)
            val stage = taskSet.stageId
            val stageTaskSets = taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
            stageTaskSets(taskSet.stageAttemptId) = manager
        
            val conflictingTaskSet = stageTaskSets.exists { case (_, ts) => ts.taskSet != taskSet && !ts.isZombie }
            // 将该任务集的管理器加入到系统调度池中，由系统统一调配，该调度器属于应用级别，支持FIFO和FAIR两种
            SchedulabelBuilder.addTaskSetManager(manager, manager.taskSet.properties)
            // ...
        }

        // 调用调度器后台进程SparkDeploySchedulerBackend的reviveOffers方法分配资源并运行
        backend.reviveOffers()
    }
    ```

    + `makeOffers()`方法实现：

    ```scala
    private def makeOffers() {
        // 获取集群中可用的Executor列表
        val activeExecutors = executorDataMap.filterKeys(!executorsPendingToRemove.contains(_))
        val workOffers = activeExecutors.map { case (id, executorData) =>
            new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
        }.toSeq
        // 对任务集的任务分配运行资源，并把这些任务提交运行
        launchTasks(scheduler.resourceOffers(workOffers))
    }
    ```

    + `resourceOffers`方法实现：

    ```scala
    def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
        // 对传入的可用Executor列表进行处理，记录其信息，如有新的Executor加入，则进行标记
        var newExecAvail = false
        for (o <- offers) {
            executorIdToHost(o.executorId) = o.host
            executorToTaskCount.getOrElseUpdate(o.executorId, 0)
            if (!executorsByHost.contains(o.host)) {
                executorsByHost(o.host) = new HashSet[String]()
                executorAdded(o.executorId, o.host)
                newExecAvail = true
            }
            for (rack <- getRackForHost(o.host)) {
                hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
            }
        }

        // 为任务随机分配Executor，避免将任务集中分配到Worker上
        val shuffledOffers = Random.shuffle(offers)

        // 用于存储分配好资源的任务
        val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
        val availableCpus = shuffledOffers.map(o => o.cores).toArray

        // 获取按照调度策略排序好的TaskSetManager
        val sortedTaskSets = rootPool.getSortedTaskSetQueue

        // 如果有新加入的Executor，需要重新计算数据本地性
        for (taskSet <- sortedTaskSets) {
            if (newExecAvail) {
                taskSet.executorAdded()
            }
        }

        // 为排好序的TaskSetManager列表进行分配资源，分配的原则是就近原则，按照顺序为PROCESS_LOCAL、NODE_LOCAL、NO_PREF、RACK_LOCAL、ANY
        var launchedTask = false
        for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
            do {
                launchedTask = resourceOfferSingleTaskSet(taskSet, maxLocality, shuffleOffers, availabelCpus, tasks)
            } while (launchedTask)
        }

        if (task.size > 0) {
            hasLaunchedTask = true
        }
        return tasks
    }
    ```

    + `launchTasks()`方法实现：

    ```scala
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
        for (task <- tasks.flatten) {
            // 序列化每一个task
            value serializedTask = ser.serialize(task)
            if (serializedTask.limit >= maxRpcMessageSize) {
                scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr => 
                    try {
                        taskSetMgr.abort(msg)
                    } catch {
                        case e: Exception => logError("Exception in error callback", e)
                    }
                }
            } else {
                val executorData = executorDataMap(task.executorId)
                executorData.freeCores -= scheduler.CPUS_PER_TASK
                // 向Worker节点的CoarseGrainedExecutorBackend发送消息执行Task
                executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
            }
        }
    }
    ```

5. 执行任务。Worker中的Executor受到TaskScheduler发送过来的任务后，以多线程方式运行，每一个线程负责一个任务。当CoarseGrainedExecutorBackend接收到LaunchTask消息时，会调用Executor的`launchTask()`方法进行处理，在Executor的`launchTask()`方法中，初始化一个TaskRunner来封装任务，它用于管理任务运行时的细节，再把TaskRunner对象放入到ThreadPool中去执行。在TaskRunner的`run()`方法里，首先会对发送过来的Task本身以及它所依赖的Jar等文件反序列化，然后对反序列化的任务调用Task的`runTask()`方法，Task本身是一个抽象类，具体的`runTask()`方法由它的两个子类ShuffleMapTask和ResultTask来实现。对于ShuffleMapTask，计算结果会写到BlockManager中，最终返回一个MapStatus对象给DAGScheduler，该对象中管理了ShuffleMapTask的运算结果存储到BlockManager里的相关存储信息，而不是计算结果本身，这些存储信息将会成为下一阶段的任务需要获得的输入数据时的依据。对于ResultTask，最终返回的是func函数的计算结果

    + `TaskRunner#run()`方法实现：

    ```scala
    override def run(): Unit = {
        // 生成内存管理taskMemoryManager实例，用于任务运行期间内存管理
        val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
        val deserializeStartTime = System.currentTimeMillis()
        Thread.currentThread.setContextClassLoader(replClassLoader)
        val ser = env.closureSerializer.newInstance()

        // 向Driver终端点发送任务运行开始消息
        execBackend.statusUpdate(taskId, TaskStage.RUNNING, EMPTY_BYTE_BUFFER)
        var taskStart: Long = 0
        startGCTime = computeTotalGcTime()
        try {
            // 对任务运行时所需要的文件、Jar包、代码等反序列化
            val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
            updateDependencies(taskFiles, taskJars)
            task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
            task.setTaskMemoryManager(taskMemoryManager)

            // 任务在反序列化之前被杀死，则抛出异常并退出
            if (killed) {
                throw new TaskKilledException
            }
            env.mapOutputTracker.updateEpoch(task.epoch)

            // 调用Task的runTask方法，Task本身是一个抽象类，具体的runTask方法是由它的两个子类ShuffleMapTask和ResultTask实现的
            taskStart = System.currentTimeMillis()
            var threwException = true
            val value = try {
                val res = task.run(taskAttemptId = taskId, attemptNumger = attemptNumber, metricsSystem = env.metricsSystem)
                res
            } finally {//...}
            // ...
        }
    }
    ```

    + `ShuffleMap#runTask()`方法实现：

    ```scala
    override def runTask(context: TaskContext): MapStatus = {
        val deserializeStartTime = System.currentTimeMillis()

        // 反序列化获取RDD和RDD的依赖
        var ser = SparkEnv.get.closureSerializer.newInstance()
        val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
        _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
        metrics = Some(context.taskMetrics)
        var writer: ShuffleWriter[Any, Any] = null
        try {
            val manager = SparkEnv.get.shuffleManager
            writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
            // 首先调用rdd.iterator，如果该RDD已经Cache或Checkpoint，那么直接读取结果，否则计算，计算结果会保存在本地系统的BlockManager中
            writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
            // 关闭writer，返回计算结果，返回包含了数据的location和size等元数据信息的MapStatus信息
            writer.stop(success = true).get
        } catch {//...}
    }
    ```

    + `ResultTask#runTask()`方法实现：

    ```scala
    override def runTask(context: TaskContext): U = {
        // 反序列化广播变量得到RDD
        val deserializeStartTime = System.currentTimeMillis()
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
        _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

        metrics = Some(context.taskMetrics)
        // ResultTask的runTask方法返回的是计算结果
        func(context, rdd.iterator(partition, context  ))
    }
    ```

6. 获取执行结果。不同类型的任务，返回的方式也不同，ShuffleMapTask返回一个MapStatus对象，ResultTask会根据结果的大小有不同的策略。
    + 生成结果大于1GB，结果直接丢弃，可通过spark.driver.maxResultSize进行设置
    + 生成结果大小在\[128MB - 200KB, 1GB\]，会把该结果以taskId为编号存入到BlockManager中，然后把该编号通过Netty发送给Driver终端点，该阈值是Netty框架传输的最大值spark.akka.frameSize(默认为128M)和Netty的预留空间reservedSizeBytes(200KB)差值
    + 生成结果大小在(0, 128MB - 200KB)，通过Netty直接发送到Driver终端点。

## Spark shuffle

Spark Shuffle机制是指上游和下游Stage之间的数据传递过程，即运行在不同Stage、不同节点上的task间如何进行数据传递。Shuffle机制分为Shuffle Write和Shuffle Read，Shuffle Write主要解决上游Stage输出数据的分区问题，Shuffle Read主要解决下游Stage从上游Stage获取数据、重新组织、并为后续操作提供数据的问题。

Spark中Shuffle实现方式经历了Hash、Sort和Tungsten-Sort三个重要阶段：

+ 在1.1之前的版本中，采用基于Hash方式的Shuffle实现，从1.1版本开始Spark参考MapReduce的实现，引入了Sort Shuffle机制，自此Hash Shuffle与Sort Shuffle共同存在，并在1.2版本时将Sort Shuffle设置为默认的Shuffle方式
+ 1.4版本时，Tungsten引入了UnSafe Shuffle机制来优化内存和CPU的使用，并在1.6版本中进行代码优化，统一实现到Sort Shuffle中，通过Shuffle管理器自动选择最佳Shuffle方式
+ 到2.0版本时，Hash Shuffle的实现从Spark中删除了，所有Shuffle方式全部统一到Sort Shuffle一个实现中

Shuffle机制的设计与实现面临以下问题：

+ 计算的多样性：在进行Shuffle Write/Read时，可能需要对数据进行一定的计算（`groupByKey()`需要将Shuffle Read的\<K, V\>记录聚合为\<K, list\<V\>>记录，`reduceByKey()`需要在Shuffle Write端进行combine，`sortByKey()`需要对Shuffle Read的数据按照Key进行排序）。Shuffle机制需要根据不同数据操作的特点，灵活地构建Shuffle Write/Read过程，并合理安排聚合函数、数据分区、数据排序的执行顺序。
+ 计算的耦合性：用户自定义聚合函数的计算过程和数据的Shuffle Write/Read过程耦合在一起（`aggregateByKey(seqOp, combOp)`在Shuffle Write时需要调用seqOp进行combine，在Shuffle Read时需要调用combOp进行聚合），需要考虑聚合函数的调用时机（先读取数据再进行聚合，边读取数据边进行聚合）
+ 中间数据存储问题：在Shuffle机制中需要对数据进行重新组织（分区、聚合、排序），也需要进行一些计算（执行聚合函数），需要考虑Shuffle Write/Read过程中中间数据的表示、组织以及存放方式。

### 设计思想

Shuffle机制中最基础的两个问题是：数据分区问题（Shuffle Write）和数据聚合问题（Shuffle Read）。

**数据分区** 在Shuffle Write阶段，根据下游stage的task个数确认分区个数（分区个数由用户通过方法中的可选参数`numPartitions`自定义，默认是parentRDD分区个数的最大值），对map task^[在ShuffleDependency情况下，将上游stage成为map stage，将下游stage成为reduce stage，map stage包含多个map task，reduce stage包含多个reduce task]输出的每一个\<K, V\>记录，根据Key计算partitionId，具有不同partitionId的记录被输出到不同的分区（文件）中

**数据聚合** 有两种方法在Shuffle Read阶段将相同Key的记录放在一起，并进行必要的计算。一是两步聚合（two-phase aggregation），先将不同task获取到的\<K, V\>记录存放到HashMap中，HashMap中的Key是K，Value是list(V)，然后对于HashMap中每一个\<K,list(V)\>记录，使用func计算得到\<K, func(list(V))\>记录，优点是逻辑清晰，容易实现，缺点是所有记录都会先存放在HashMap中，占用内存空间较大，另外对于包含聚合函数的操作，效率较低。一是在线聚合（online aggregation），在每个记录加入HashMap时，同时进行func()聚合操作，并更新相应的聚合结果，对于每一个新来的\<K, V\>记录，首先从HashMap中get出已经存在的结果`V'=HashMap.get(K)`，然后执行聚合函数得到新的中间结果`V''=func(V, V')`，最后将V''写入HashMap中，即HashMap.put(K, V'')，优点是减少内存消耗，将Shuffle Read和聚合函数计算耦合在一起，可以加速计算，但是对于不包含聚合函数的操作，在线聚合和两步聚合没有差别。

**combine操作** combine操作发生在Shuffle Write阶段，与Shuffle Read端的聚合过程相似。进行combine操作的目的是减少Shuffle的数据量，只有包含聚合函数的数据操作（如`reduceByKey()`、`foldByKey()`、`aggregateByKey()`、`combineByKey()`、`distinct()`）需要进行map()端combine，采用Shuffle Read端基于HashMap的解决方案，首先利用HashMap进行combine，然后对HashMap中每一个record进行分区，输出到对应的分区文件中

**数据排序** 对于`sortByKey()`、`sortBy()`等排序操作，在Shuffle Read阶段必须执行排序操作，理论上Shuffle Write阶段不需要排序，但是如果进行了排序，Shuffle Read阶段获取到的将是已经部分有序的数据，可以减少Shuffle Read阶段排序的复杂度。有三种排序方法：

+ 先排序再聚合：先使用线性数据结构（如Array），存储Shuffle Read的\<K, V\>记录，然后对Key进行排序，排序后的数据可以直接从前到后进行扫描聚合，不需要使用HashMap进行hash-based聚合，优点既可以满足排序要求又可以满足聚合要求，缺点是需要较大的内存空间来存储线性数据结构，同时排序和聚合过程不能同时进行，即不能使用在线聚合，效率较低，Hadoop MapReduce采用的是此中方案

+ 排序和聚合同时进行：使用带有排序功能的Map（如TreeMap）来对中间数据进行聚合，每次Shuffle Read获取到一个记录，就将其放入TreeMap中与现有的记录进行聚合，优点是排序和聚合可以同时进行；缺点是相比HashMap，TreeMap的排序复杂度较高

+ 先聚合再排序：在基于HashMap的聚合方案的基础上，将HashMap中的记录或记录的引用放入线性数据结构中进行排序，优点是聚合和排序过程独立，灵活性较高，缺点是需要复制数据或引用，空间占用较大

**内存不足问题** 使用HashMap对数据进行combine和聚合，在数据量大的时候，会出现内存溢出，即可能出现在Shuffle Write阶段，也可能出现在Shuffle Read阶段。使用内存+磁盘混合存储方案，先在内存（如HashMap）中进行数据聚合，如果内存空间不足，则将内存中的数据溢写（spill）到磁盘上，在进行下一步操作之前对磁盘上和内存中的数据进行再次聚合（全局聚合），为了加速全局聚合，需要将数据溢写到磁盘上时进行排序（全局聚合按顺序读取溢写到磁盘上的数据，减少磁盘I/O）。

### 实现

Spark可以根据不同数据操作的特点，灵活构建合适的Shuffle机制。在Shuffle机制中Spark典型数据操作的计算需求如下：

<div class="wrapper" markdown="block">

|包含ShuffleDependency的操作|Shuffle Write端combine|Shuffle Write端按Key排序|Shuffle Read端combine|Shuffle Read端按Key排序|
|---|---|---|---|---|
|`partitionBy()`|x|x|x|x|
|`groupByKey()`、`cogroup()`、`join()`、<br>`coalesce()`、`intersection()`、<br>`subtract()`、`subtractByKey()`|x|x|v|x|
|`reduceByKey()`、`aggregateByKey()`、<br>`combineByKey()`、`foldByKey()`、`distinct()`|v|x|v|x|
|`sortByKey()`、`sortBy()`、<br>`repartitionAndSortWithinPartitions()`|x|x|x|v|
|未来系统可能支持的或者用户自定义的数据操作|v|v|v|v|

</div>

#### Shuffle Write

在Shuffle Write阶段，数据操作需要分区、聚合和排序3个功能，但每个数据操作只需要其中的部分功能，为了支持所有的情况，Shuffle Write的计算顺序为 map()输出 -> 数据聚合 -> 排序 -> 分区输出。map task每计算出一条记录及其partitionId，就将记录放入类似HashMap的数据结构中进行聚合；聚合完成后，再将HashMap中的数据放入到类似Array的数据结构中进行排序（既可按照partitionId，也可按照partitionId+Key进行排序）；最后根据partitionId将数据写入不同的数据分区中，存放到本地磁盘上。其中，聚合（即combine）和排序过程是可选的。

Spark对不同的情况进行了分类，以及针对性的优化调整，形成了不同的Shuffle Write方式：

**BypassMergeSortShuffleWriter** map()依次输出\<K, V\>记录，并计算其partitionId，Spark根据partitionId将记录依次输出到不同的buffer^[分配buffer的原因是map()输出record的速度很快，需要进行缓冲来减少磁盘I/O]中，每当buffer填满就将记录溢写到磁盘上的分区文件中。优点是速度快，缺点是资源消耗过高，每个分区都需要一个buffer（大小由spark.Shuffle.file.buffer控制，默认为32KB），且同时需要建立多个分区文件进行溢写，当分区数过多（超过200）会出现buffer过大、建立和打开文件数过多的问题。适用于 *map()端不需要聚合（combine）、Key不需要排序* 且分区个数较少（<=spark.Shuffle.sort.bypassMergeThreshold，默认值为200）的数据操作，如`groupByKey(n)`、`partitionBy(n)`、`sortByKey(n)`（n <= 200）

**SortShuffleWriter(KeyOrdering=true)** 建立一个数组结构（PartitionedPairBuffer）来存放map()输出的记录，并将\<K,V\>记录转化为\<(partitionId,K),V\>记录存储，然后按照partitionId+Key对记录进行排序；最后将所有记录写入一个文件中，通过建立索引来标示每个分区。如果数组存放不下，则会先扩容，如果还存放不下，就将数组中的记录排序后溢写到磁盘上（此过程可重复多次），等待map()输出完以后，再将数组中的记录与磁盘上已排序的记录进行全局排序，得到最终有序的记录，并写入分区文件中。优点是只需要一个数组结构就可以支持按照partitionId+Key进行排序，数组大小可控^[同时具有扩容和溢写到磁盘上的功能，不受数据规模限制]，同时只需要一个分区文件就可以标示不同的分区数据^[输出的数据已经按照partitionId进行排序]，适用分区个数很大的情况，缺点是排序增加计算时延。适用于 *map()端不需要聚合（combine）、Key需要排序*、分区个数无限制的数据操作。SortShuffleWriter(KeyOrdering=false)（只按partitionId排序）可以用于解决BypassMergeSortShuffleWriter存在的buffer分配过多的问题，支持不需要map端聚合（combine）和排序、分区个数过大的操作，如`groupByKey(n)`、`partitionBy(n)`、`sortByKey(n)`（n > 200）

**SortShuffleWriterWithCombine** 建立一个HashMap结构（PartitionedAppendOnlyMap，同时支持聚合和排序操作）对map()输出的记录进行聚合，HashMap中的键是partitionId+Key，值是多条记录对应的Value combine后的结果，聚合完成后，Spark对HashMap中的记录进行排序（如果不需要按Key排序，只按partitionId进行排序；如果需要按Key排序，就按partitionId+Key进行排序），最后将排序后的记录写入一个分区文件中。如果HashMap存放不下，则会先扩容为两倍大小，如果还存放不下，就将HashMap中的记录排序后溢写到磁盘上（此过程可重复多次），当map()输出完成以后，再将HashMap中的记录与磁盘上已排序的记录进行全局排序，得到最终有序的记录，并写入分区文件中。优点是只需要一个HashMap结构就可以支持map()端聚合（combine）功能，HashMap大小可控^[同时具有扩容和溢写到磁盘上的功能，不受数据规模限制]，同时只需要一个分区文件就可以标示不同的分区数据，使用分区个数很大的情况，在聚合后使用数组结构排序，可以灵活支持不同的排序需求，缺点是在内存中进行聚合，内存消耗较大，需要额外的数组进行排序，而且如果有数据溢写到磁盘上，还需要再次进行聚合。适用于*map()端聚合（combine）*、需要或者不需要按Key排序、分区个数无限制的数据操作，如`reduceByKey()`、`aggregateByKey()`

BypassMergeSortShuffleWriter使用直接输出模式，适用于不需要map()端聚合（combine），也不需要按Key排序，而且分区个数很少的数据操作。SortShuffleWriter使用基于数组结构的方法来按partitionId或partitionId+Key排序，只输出单一的分区文件，克服了BypassMergeSortShuffleWriter打开文件过多、buffer分配过多的缺点，也支持按Key排序。SortShuffleWriterWithCombine使用基于Map结构的方法来支持map()端聚合（combine），在聚合后根据partitionId或partitionId+Key对记录进行排序，并输出分区文件。

#### Shuffle Read

在Shuffle Read阶段，数据操作需要跨节点数据获取、聚合和排序3个功能，但每个数据操作只需要其中的部分功能，为了支持所有的情况，Spark Read框架的计算顺序为 数据获取 -> 聚合 -> 排序输出。reduce task不断从各个map task的分区文件中获取数据，然后使用HashMap结构来对数据进行聚合（aggregate），该过程是边获取数据边聚合，聚合完成后，将HashMap中的数据放入Array结构中按照Key进行排序，最后将排序结果输出或者传递给下一步操作，如果不需要聚合或者排序，则可以去掉相应的聚合或排序过程。

**不聚合、不按Key排序的Shuffle Read** 等待所有的map task结束后，reduce task开始不断从各个map task获取\<K,V\>记录，并将记录输出到一个buffer中（大小为spark.reducer.maxSizeInFlight=48MB），数据获取结束后将结果输出或者传递给下一步操作。优点是逻辑和实现简单，内存消耗很小，缺点是不支持聚合、排序等复杂功能。适用于不需要聚合也不需要排序的数据操作，如partitionBy()

**不聚合、按Key排序的Shuffle Read** 等待所有map task结束后，reduce task开始不断从各个map task获取\<K,V\>记录，并将记录输出到数组结构（ParitionedPairBuffer）中，然后，最数组中的记录按照Key进行排序，并将排序结果输出或者传给下一步操作。当内存无法存下所有的记录时，PartitionedPairBuffer将记录排序后溢写到磁盘上，最后将内存中和磁盘上的记录进行全局排序，得到最终排序后的记录。优点是只需要一个数组结构就可以支持按照Key进行排序，数组大小可控，而且具有扩容和溢写到磁盘上的功能，不受数据规模限制，缺点是排序增加计算时延。适用于不需要聚合，但需要按Key排序的数据操作，如sortByKey()、sortBy()

**支持聚合的Shuffle Read** 等待所有map task结束后，reduce task开始不断从各个map task获取\<K,V\>记录，通过HashMap结构（ExternalAppendOnlyMap，同时支持聚合和排序）对记录进行聚合，HahsMap中的键是记录中的Key，HashMap中的值是多条记录对应的Value聚合后的结果，聚合完成后，如果需要按照Key进行排序，则建立一个数组结构，读取HashMap中的记录，并对记录按Key进行排序，最后将结果输出或者传递给下一步操作。如果HahsMap存放不下，则会先扩容为两倍大小，如果还存放不下，就将HashMap中的记录排序后溢写到磁盘上（此过程可重复多次），当聚合完成以后，将HashMap中的记录和磁盘上已排序的记录进行全局排序，得到最终排序后的记录。优点是只需要一个HashMap结构和数组结构就可以支持聚合和排序功能，HashMap大小可控，而且具有扩容和溢写到磁盘上的功能，不受数据规模限制，缺点是需要在内存中进行聚合^[如果有数据溢写到磁盘上，还需要进行再次聚合]，而且经过HashMap聚合后的数据仍然需要复制到数组结构中进行排序，内存消耗极大。适用于需要聚合、不需要或需要按Key进行排序的数据操作，如reduceByKey()、aggregateByKey()

Shuffle Read使用的技术和数据结构与Shuffle Write过程类似，而且由于不需要分区，过程比Shuffle Write更为简单。如果应用中的数据操作不需要聚合，也不需要排序，那么获取数据后直接输出，对于需要按Key排序的操作，Spark使用基于数组结构的方法来对Key进行排序，对于需要聚合的操作，Spark提供了基于HashMap结构的聚合方法，同时可以再次使用数组结构来支持按照Key进行排序。

#### 支持高效聚合和排序的数据结构

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

### 与Hadoop MapReduce的Shuffle机制对比

## Spark SQL执行原理

一般来讲，对于Spark SQL系统，从SQL到Spark中RDD的执行需要经过两个大的阶段，分别是逻辑计划（LogicalPlan）和物理计划（PhysicalPlan）。

```bob-svg
    .----------------.     .----------.        .-----------.
    | SparkSqlParser |     | Analyzer |        | Optimizer |
    '-------+--------'     '-----+----'        '-----+-----'
            | parse              | apply             | apply
            | .------------------+-------------------+--------------------.
            | |  LogicalPlan     |                   |                    |
+--------+  | |  +------------+  |  +-------------+  |  +-------------+   |
|  SQL   |  v |  |Unresolved  |  v  | Analyzed    |  v  | Optimized   |   |
|  Query +--+-+->+LogicalPlan +--+->+ LogicalPlan +--+->+ LogicalPlan +---+--+
+--------+    |  +------------+     +-------------+     +-------------+   |  |
              '-----------------------------------------------------------'  |   plan   .--------------.
              .-----------------------------------------------------------.  +<---------| SparkPlanner |
              |  PhysicalPlan                                             |  |          '--------------'
              |  +-----------+      +-----------+      +---------------+  |  |
      show    |  | Prepared  |      | SparkPlan | next |Iterator       |  |  |
   <----------+--+ SparkPlan +<--+--+           +<-----+[PhysicalPlan] +<-+--+
              |  +-----------+   ^  +-----------+      +---------------+  |
              |                  |                                        |
              '------------------+----------------------------------------'
            Seq[Rule[SparkPlan]] | apply
                      .----------+-----------.
                      | prepareForExecution  |
                      '----------------------'
```

逻辑计划阶段会将用户所写的SQL语句转换成树型数据结构（逻辑算子树），SQL语句中蕴含的逻辑映射到逻辑算子树的不同节点。逻辑计划阶段生成的逻辑算子树并不会直接提交执行，仅作为中间阶段，最终逻辑算子树的生成过程经历了3个子阶段，分别对应未解析的逻辑算子树（Unresolved LogicalPlan，仅仅是数据结构，不包含任何数据信息等）、解析后的逻辑算子树（Analyzed LogicalPlan，节点中绑定各种信息）和优化后的逻辑算子树（Optimized LogicalPlan，应用各种优化规则对一些低效的逻辑计划进行替换）。

物理计划阶段将上一步逻辑计划阶段生成的逻辑算子树进行进一步转换，生成物理算子树，物理算子树的节点会直接生成RDD或对RDD进行transformation操作^[每个物理计划节点中都实现了对RDD进行转换的execute方法]。物理计划阶段也包含3个子阶段，首先根据逻辑算子树，生成物理算子树的列表`Iterator[PhysicalPlan]`（同样的逻辑算子树可能对应多个物理算子树），然后从列表中按照一定的策略选取最优的物理算子树（SparkPlan），最后，对选取的物理算子树进行提交前的准备工作（如 确保分区操作正确、物理算子树节点重用、执行代码生成等），得到准备后的物理算子树（Prepared SparkPlan）。

经过上述步骤后，物理算子树生成的RDD执行action操作，即可提交执行。从SQL语句的解析一直到提交之前，上述整个转换过程都在Spark集群的Driver端进行，不涉及分布式环境。SparkSession类的sql方法调用SessionState中的各种对象，包括上述不同阶段对应的SparkSqlParser类、Optimizer类和SparkPlanner类等，最后封装成一个QueryExecution对象。

Spark SQL内部实现上述流程中平台无关部分的基础框架称为Catalyst，Catalyst中涉及的重要概念和数据结构主要包括InternalRow体系、TreeNode体系和Expression体系。

<div class="wrapper" markdown="block">

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
abstract class InternalRow {
    numFields: Int
    setNull(i: Int): Unit
    update(i: Int, value: Any): Unit
    setBoolean(i: Int, value: Boolean): Unit
    ...
    toSeq(fieldTypes: Seq[DataType]): Seq[Any]
    toSeq(schema: StructType): Seq[Any]
}

class UnsafeRow
class BaseGenericInternalRow
class JoinedRow
class GenericInternalRow
class SpecificInternalRow
class MutableUnsafeRow

InternalRow <|-- UnsafeRow
InternalRow <|-- BaseGenericInternalRow
InternalRow <|-- JoinedRow
BaseGenericInternalRow <|-- GenericInternalRow
BaseGenericInternalRow <|-- SpecificInternalRow
BaseGenericInternalRow <|-- MutableUnsafeRow
@enduml
```

</div>

在Spark SQL内部实现中，InternalRow是用来表示一行行数据的类^[物理算子树节点产生和转换的RDD类型即为`RDD[InternalRow]`]。InternalRow中的每一列都是Catalyst内部定义的数据类型。InternalRow作为一个抽象类，包含numFields和`update()`方法，以及各列数据对应的get与set方法^[InternalRow中都是根据下标来访问和操作列元素的]，但具体的实现逻辑体现在不同的子类中：

+ BaseGenericInternalRow：实现了InternalRow中定义的所有get类型方法，这些方法的实现都通过调用类中定义的GenericGet虚函数进行，该函数的实现在下一级子类中
+ JoinedRow：主要用于Join操作，将两个InternalRow放在一起形成新的InternalRow
+ UnsafeRow：不采用Java对象存储的方式，避免了JVM中垃圾回收的代价，对行数据进行了特定的编码，使得存储更加高效。MutableUnsafeRow和UnsafeRow相关，用来支持对特定的列数据进行修改

GenericInternalRow构造参数是`Array[Any]`类型，采用对象数据进行底层存储，`genericGet()`也是直接根据下标访问的^[数组是非修改的，一旦创建就不允许通过set操作进行改变]，SpecificInternalRow是以`Array[MutableValue]`为构造参数的，允许通过set操作进行修改。

```scala
class GenericInternalRow(val values: Array[Any]) extends BaseGenericInternalRow {
    def this() = this(null)
    def this(size: Int) = this(new Array[Any](size))
    def genericGet(ordinal: Int) = values(oridinal)
    def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = values.clone()
    def numFields: Int = values.length
    def setNullAt(i: Int): Unit = { values(i) = value }
    def copy(): GenericInternalRow = this
}
```

<div class="wrapper" markdown="block">

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
abstract class TreeNode {
    children: Seq[BaseType]
    foreach(f: BaseType => Unit): Unit
    foreachUp(f: BaseType => Unit): Unit
}
class Expression
class QueryPlan
class LogicalPlan
class SparkPlan

TreeNode <|-- Expression
TreeNode <|-- QueryPlan
QueryPlan <|-- LogicalPlan
QueryPlan <|-- SparkPlan
@enduml
```

</div>

TreeNode体系是Catalyst中的中间数据结构，TreeNode类是Spark SQL中所有树结构的基类，定义了一系列通用的集合操作和树遍历操作接口，TreeNode内部包含一个`Seq[BaseType]`类型的变量children来表示孩子节点，定义了foreach、map、collect等针对节点操作的方法，以及transformUp和transformDown等遍历节点并对匹配节点进行相应转换的方法。TreeNode一直在内存里维护，不会dump到磁盘以文件形式存储，且树的修改都是以替换已有节点的方式进行的。

TreeNode包含两个子类继承体系，QueryPlan和Expression体系，QueryPlan类下又包含逻辑算子树（LogicalPlan）和物理执行算子树（SparkPlan）两个重要子类，其中，逻辑算子树在Catalyst中内置实现，可以剥离出来直接应用到其他系统中；而物理算子树SparkPlan和Spark执行层紧密相关，当Catalyst应用到其他计算模型时，可以进行相应的适配修改。

TreeNode本身提供了最简单和最基本的操作：
+ `collectLeaves`：获取当前TreeNode所有叶子节点
+ `collectFirst`：先序遍历所有节点并返回第一个满足条件的节点
+ `withNewChildren`：将当前节点的子节点替换为新的子节点
+ `transformDown`：用先序遍历方式将规则作用于所有节点
+ `transformUp`：用后序遍历方式将规则作用于所有节点
+ `transformChildren`：递归地将规则作用到所有子节点
+ `treeString`：将TreeNode以树型结构展示

表达式一般指的是不需要触发执行引擎就能够直接进行计算的单元（四则运算、逻辑操作、转换操作、过滤操作等），Expression是Catalyst中的表达式体系，算子执行前通常都会进行绑定操作，将表达式与输入的属性对应起来，同时算子也能够调用各种表达式处理相应的逻辑。Expression类中定义了5个方面的操作，包括基本属性、核心操作、输入输出、字符串表示和等价性判断。核心操作中的`eval`函数实现了表达式对应的处理逻辑，`genCode`和`doGenCode`用于生成表达式对应的Java代码，字符串表示用于查看该Expression的具体内容。

### Spark SQL解析

<div class="wrapper" markdown="block">

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members

interface ParseInterface {
    parsePlan(sqlText: String): Logical Plan
    parseExpression(sqlText: String): Expression
    parseTableIdentifier(sqlText: String): TableIdentifier
}

abstract class AbstractSqlParser {
    parseDataType(sqlText: String): DataType
    astBuilder: AstBuilder
}

class CatalystSqlParser {
    astBuilder: AstBuilder
}

class SparkSqlParser {
    astBuilder: SparkSqlAstBuilder
}

class SqlBaseBaseVisitor
class AstBuilder
class SparkSqlAstBuilder

ParseInterface <|.. AbstractSqlParser
AbstractSqlParser <|-- SparkSqlParser
AbstractSqlParser <|-- CatalystSqlParser

SqlBaseBaseVisitor <|-- AstBuilder
AstBuilder <|-- SparkSqlAstBuilder

AstBuilder <- AbstractSqlParser
SparkSqlAstBuilder <- SparkSqlParser

@enduml
```

</div>

Catalyst中提供了直接面向用户的ParseInterface接口，该接口中包含了对SQL语句、Expression和TableIdentifier（数据表标识符）的解析方法，AbstractSqlParser是实现了ParseInterface的虚类，其中定义了返回AstBuilder的方法。CatalystSqlParser仅用于Catalyst内部，而SparkSqlParser用于外部调用。

AstBuilder继承了ANTLR 4生成的默认SqlBaseBaseVisitor，用于生成SQL对应的抽象语法树AST（Unresolved LogicalPlan）。SparkSqlAstBuilder继承AstBuilder，并在其基础上定义了一些DDL语句的访问操作，主要在SparkSqlParser中调用。

QuerySpecificationContext为根节点所代表的子树中包含了对数据的查询，其子节点包括NamedExpressionSqlContext、FromClauseContext、BooleanDefaultContext、AggregationContext等，NamedExpressionSqlContext为根节点的系列节点代表select表达式中选择的列，FromClauseContext节点为根节点的系列节点对应数据表，BooleanDefaultContext为根节点的系列节点代表where条件中的表达式，AggregationContext为根节点的系列节点代表聚合操作（group by、cube、grouping sets和roll up）的字段。

QueryOrganizationContext为根节点所代表的子树中包含了各种对数据的操作，如SortItemContext节点代表数据查询之后所进行的排序操作。

FunctionCallContext为根节点的系列节点代表函数，其子节点QualifiedNameContext代表函数名，ExpressionContext表示函数的参数表达式。

当开发新的语法支持时，首先需要改动ANTLR 4语法文件（在SqlBase.g4中添加文法），重新生成词法分析器（SqlBaseLexer）、语法分析器（SqlBaseParser）和访问者类（SqlBaseVisitor接口与SqlBaseVisitor类），然后在AstBuilder等类中添加相应的访问逻辑，最后添加执行逻辑。

### Spark SQL逻辑计划（Logical Plan）

逻辑计划阶段字符串形态的SQL语句转换为树结构形态的逻辑算子树，SQL中包含的各种处理逻辑（过滤、剪裁等）和数据信息都会被整合在逻辑算子树的不同节点中。逻辑计划本质上是一种中间过程表示，与Spark平台无关，后续阶段会进一步将其映射为可执行的物理计划。

Spark SQL逻辑计划在实现层面被定义为LogicalPlan类，从SQL语句经过SparkSqlParser解析生成Unresolved LogicalPlan到最终优化成Optimized LogicalPlan，这个流程主要经过3个阶段：

1. 由SparkSqlParser中的AstBuilder执行节点访问，将语法树的各种Context节点转换成对应的LogicalPlan节点，从而成为一棵未解析的逻辑算子树（Unresolved LogicalPlan），此时逻辑算子树是最初形态，不包含数据信息和列信息等
2. 由Analyzer将一系列规则作用在Unresolved LogicalPlan上，对树上的节点绑定各种数据信息，生成解析后的逻辑算子树（Analyzed LogicalPlan）
3. 由Spark SQL中的优化器（Optimizer）将一系列优化规则作用到上一步生成的逻辑算子树中，在确保结果正确的前提下改写其中的低效结构，生成优化后的逻辑算子树（Optimized LogicalPlan）。Optimized LogicalPlan传递到下一个阶段用于物理执行计划的生成

LogicalPlan属于TreeNode体系，继承自QueryPlan父类，作为数据结构记录了对应逻辑算子树节点的基本信息和基本操作，包括输入输出和各种处理逻辑等。

QueryPlan的主要操作分为6个模块：
+ 输入输出：定义了5个输入输出方法
    + `output`是返回值为`Seq[Attribute]`的虚函数，具体内容由不同子节点实现
    + `outputSet`是将output返回值进行封装，得到`AttributeSet`集合类型的结果
    + `inputSet`是用于获取输入属性的方法，返回值也是`AttributeSet`，节点的输入属性对应所有子节点的输出
    + `producedAttributes`表示该节点所产生的属性
    + `missingInput`表示该节点表达式中涉及的但是其子节点输出中并不包含的属性
+ 基本属性：表示QueryPlan节点中的一些基本信息
    + `schema`对应output输出属性的schema信息
    + `allAttributes`记录节点所涉及的所有属性（Attribute）列表
    + `aliasMap`记录节点与子节点表达式中所有的别名信息
    + `references`表示节点表达式中所涉及的所有属性集合
    + `subqueries`/`innerChildren`默认实现该QueryPlan节点中包含的所有子查询
+ 字符串：这部分方法主要用于输出打印QueryPlan树型结构信息，其中schema信息也会以树状展示。`statePrefix`方法用来表示节点对应计划状态的前缀字符串^[在QueryPlan的默认实现中，如果该计划不可用，则前缀会用感叹号标记]
+ 规范化：在QueryPlan的默认实现中，`canonicalized`直接赋值为当前的QueryPlan类，在`sameResult`方法中会利用`canonicalized`来判断两个QueryPlan的输出数据是否相同
+ 表达式操作：在QueryPlan各个节点中，包含了各种表达式对象，各种逻辑操作一般也都是通过表达式来执行的
    + `expressions`方法能够得到该节点中的所有表达式列表
+ 约束（Constraints）：本质上属于数据过滤条件（Filter）的一种，同样是表达式类型。相对于显式的过滤条件，约束信息可以推导出来^[对于过滤条件a > 5，显然a的属性不能为null，这样就可以对应的构造`isNotNull(a)`约束]。在实际情况下，SQL语句中可能会涉及很复杂的约束条件处理，如约束合并、等价性判断等，在QueryPlan类中，提供了大量方法用于辅助生成constraints表达式集合以支持后续优化操作
    + `validConstraints`方法返回该QueryPlan所有可用的约束条件
    + `constructIsNotNullConstraints`方法会针对特定的列构造isNotNull约束条件

#### Unresolved LogicalPlan生成

Spark SQL首先会在ParserDriver中通过调用语法分析器中的`singleStatement()`方法构建整棵语法树，然后通过AstBuilder访问者类对语法树进行访问。AstBuilder访问入口是`visitSingleStatement()`方法，该方法也是访问整棵抽象语法树的启动接口。

```Java
override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
}
```

对根节点的访问操作会递归访问其子节点，这样逐步向下递归调用，直到访问某个子节点时能够构造LogicalPlan，然后传递给父节点。

QuerySpecificationContext节点的执行逻辑可以看作两部分：首先访问FromClauseContext子树，生成名为from的LogicalPlan，接着调用withQuerySpecification方法在from的基础上完成后续扩展。

```Java
override def visitQuerySpecification(ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation.optional(ctx.fromClause) {
        visitFromClause(ctx.fromClause)
    }
    withQuerySpecification(ctx, from)
}
```

生成UnresolvedLogicalPlan的过程，从访问QuerySpecificationContext节点开始，分为以下3个步骤：
1. 生成数据表对应的LogicalPlan：访问fromClauseContext并递归访问，一直到匹配TableNameContext节点（visitTableName）时，直接根据TableNameContext中的数据信息生成UnresolvedRelation，此时不再递归访问子节点，构造名为from的LogicalPlan并返回
2. 生成加入了过滤逻辑的LogicalPlan：过滤逻辑对应SQL中的where语句，在QuerySpecificationContext中包含了名称为where的BooleanExpressionContext类型，AstBuilder会对该子树进行递归访问，生成expression并返回作为过滤条件，然后基于此过滤条件表达式生成Filter LogicalPlan节点。最后，由此LogicalPlan和1中的UnresolveRelation构造名称为withFilter的LogicalPlan，其中Filter节点为根节点
3. 生成加入列剪裁后的LogicalPlan：列剪裁逻辑对应SQL中select语句对name列的选在操作，AstBuilder在访问过程中会获取QuerySpecificationContext节点所包含的NamedExpressionSeqContext成员，并对其所有子节点对应的表达式进行转换，生成NameExpression列表（namedExpressions）,然后基于namedExpressions生成Project LogicalPlan，最后，由此LogicalPlan和2中的withFilter构造名称为withProject的LogicalPlan，其中Project最终成为整棵逻辑算子树的根节点

#### Analyzed LogicalPlan生成

Analyzed LogicalPlan基本上是根据Unresolved LogicalPlan一对一转换过来的。

Analyzer的主要作用就是根据Catalog中的信息将Unresolved LogicalPlan中未被解析的UnresolvedRelation和UnresolvedAttribute两种对象解析成有类型的（Typed）对象。

在Spark SQL系统中，Catalog主要用于各种函数资源信息和原数据信息（数据库、数据表、数据视图、数据分区与函数等）的统一管理。Saprk SQL中的Catalog体系实现以SessionCatalog为主体，通过SparkSession（Spark程序入口）提供给外部调用。一般一个SparkSession对应一个SessionCatalog。本质上，SessionCatalog起到一个代理的作用，对底层的元数据信息、临时表信息、视图信息和函数信息进行了封装。SessionCatalog主要包含以下部分：

+ GlobalTempViewManager（全局的临时视图管理）：是一个线程安全的类，提供了对全局视图的原子操作，包括创建、更新、删除和重命名等，进行跨Session的视图管理。内部实现中主要功能依赖一个mutable类型的HashMap来对视图名和数据源进行映射，其中key为视图名，value为视图所对应的LogicalPlan（一般在创建该视图时生成）
+ FunctionResourceLoader（函数资源加载器）：主要用于加载Jar包、文件两种类型的资源以提供函数^[Spark SQL内置实现的函数、用户自定义函数和Hive中的函数]的调用
+ FunctionRegistry（函数注册接口）：用来实现对函数的注册（Register）、查找（Lookup）和删除（Drop）等功能。在Spark SQl中默认实现是SimpleFunctionRegistry，其中采用Map数据结构注册了各种内置的函数
+ ExternalCatalog（外部系统Catalog）：用来管理数据库（Databases）、数据表（Tables）、数据分区（Partitions）和函数（Functions）的接口。目标是与外部系统交互，并做到上述内容的非临时性存储。ExternalCatalog是一个抽象类，定义了上述4个方面的功能，在Spark SQL中，具体实现有InMemoryCatalog和HiveExternalCatalog两种，InMemoryCatalog将上述信息存储在内存中，HiveExternalCatalog利用Hive元数据库来实现持久化管理

另外，SessionCatalog内部还包含一个mutable类型的HashMap用来管理临时表信息，以及currentDb成员变量用来指代当前操作所对应的数据库名称。

对Unresolved LogicalPlan的操作（绑定、解析、优化等）都是基于规则（Rule）的，通过Scala语言模式匹配机制（Pattern-match）进行树结构的转换或节点改写。Rule是一个抽象类，子类需要复写`apply(plan: TreeType)`方法来制定特定的处理逻辑。在Catalyst中RuleExecutor用来调用这些规则，涉及树型结构的转换过程^[Analyzer逻辑算子树的分析过程，Optimizer逻辑算子树的优化过程，物理算子树的生成过程]都要实施规则匹配和节点处理，都继承自`RuleExecutor[TreeType]`抽象类。RuleExecutor内部提供了一个`Seq[Batch]`，里面定义了该RuleExecutor的处理步骤。每个Batch代表一套规则，配备一个策略，该策略说明了迭代次数（一次还是多次），RuleExecutor的`apple(plan: TreeType)`方法会按照batches顺序和batch内的Rules顺序，对传入的plan里的节点进行迭代处理，处理逻辑由具体Rule子类实现。

```Scala
abstract class Rule[TreeType <: TreeNode[_]] extends Logging {
    val ruleName: String = {
        val className = getClass.getName
        if (className endsWith "$") className.dropRigh(1) else className
    }
    def apply(plan: TreeType): TreeType
    def execute(plan: TreeType): TreeType = {
        var curPlan = plan
        batches.foreach { batch =>
            val batchStartPlan = curPlan
            var iteration = 1
            var lastPlan = curPlan
            var continue = true
            while (continue) {
                curPlan = batch.rules.foldLeft(curPlan) {
                    case (plan, rule) => rule(plan)
                }
                iteration += 1
                if (iteration > batch.strategy.maxIterations) {
                    continue = false
                }
                if (curPlan.fastEquals(lastPlan)) {
                    continue = false
                }
                lastPlan = curPlan
            }
        }
        curPlan
    }
}
```

Analyzer继承自ruleExecutor类，执行过程会调用其父类RuleExecutor中实现的execute方法，Analyzer中重新定义了一系列规则，即RuleExecutor类中的成员变量batches。Analyzer默认定义了6个Batch，共有34条内置的规则外加额外实现的扩展规则。

<div class="wrapper" markdown="block">

|组|组描述|规则|规则描述|
|---|---|---|---|
|Batch Substitution|对节点进行替换操作|CTESubstitution|用来处理with语句^[CTE对应with语句，在SQL中主要用于子查询模块化]，在遍历逻辑算子树的过程中，当匹配到with(child, relation)节点时，将子LogicalPlan替换成解析后的CTE。<br>由于CTE的存在，SparkSqlParser对SQL语句从左向右解析后会产生多个LogicalPlan，这条规则的作用是将多个LogicalPlan合并成一个LogicalPlan|
| | |WindowsSubstitution|对当前的逻辑算子树进行查找，当匹配到WithWindowDefinition(windowDefinitions, child)表达式时，<br>将其子节点中未解析的窗口表达式（Unresolved-WindowExpression）转换成窗口函数表达式（WindowExpression）|
| | |EliminateUnions|在遍历逻辑算子树过程中，匹配到Union(children)且children的数目只有1个时，将Union(children)替换为children.head节点|
| | |SubstituteUnresolvedOrdinals|根据配置项spark.sql.orderByOridinal和spark.sql.groupByOrdinal将下标替换成UnresolvedOriginal表达式，以映射到对应的列|
|Batch Resolution|解析|ResolveTableValuedFunctions|解析可以作为数据表的函数|
| | |ResolveRelations|解析数据表，当遍历逻辑算子树的过程中匹配到UnresolvedRelation节点时，调用lookupTableFromCatalog方法从SessionCatalog中查表，<br>在Catalog查表后，Relation节点上会插入一个别名节点|
| | |ResolveRederrences|解析列，当遍历逻辑算子树的过程中匹配到UnresolvedAttribute时，会调用LogicalPlan中定义的resolveChildren方法对该表达式进行分析。<br>resolveChildren并不能确保一次分析成功，在分析对应表达式时，需要根据该表达式所处LogicalPlan节点的子节点输出信息进行判断，<br>如果存在处于unresolved状态的子节点，解析操作无法成功，留待下一轮规则调用时再进行解析|
| | |ResolveCreateNamedStruct|解析结构体创建|
| | |ResolveDeserializer|解析反序列化操作类|
| | |ResolveNewInstance|解析新的实例|
| | |ResolveUpCast|解析类型转换|
| | |ResolveGroupingAnalytics|解析多维分析|
| | |ResolvePivot|解析Pivot|
| | |ResolveOrdinalInOrderByAndGroupBy|解析下标聚合|
| | |ResolveMissingReferences|解析新的列|
| | |ExtractGenerator|解析生成器|
| | |ResolveGenerate|解析生成过程|
| | |ResolveFunctions|解析函数|
| | |ResolveAliases|解析别名|
| | |ResolveSubquery|解析子查询|
| | |ResolveWindowOrder|解析窗口函数排序|
| | |ResolveWindowFrame|解析窗口函数|
| | |ResolveNaturalAndUsingJoin|解析自然join|
| | |ExtractWindowExpressions|提取窗口函数表达式|
| | |GlobalAggregates|解析全局聚合|
| | |ResolveAggregateFunctions|解析聚合函数|
| | |TimeWindowing|解析时间窗口|
| | |ResolveInlineTables|解析内联表|
| | |TypeCoercion.typeCoercionRules|解析强制类型转换<br>ImplicitTypeCasts规则对逻辑算子树中的BinaryOperator表达式调用findTightestCommonTypeOfTwo找到对于左右表达式节点来讲最佳的共同数据类型|
| | |extendedResolutionRules|扩展规则，用来支持Analyzer子类在扩展规则列表中添加新的分析规则|
|Batch Nondeterministic| |PullOutNondeterministic|用来将LogicalPlan中非Project或非Filter算子的nondeterministic（不确定的）表达式提取出来，<br>然后将这些表达式放在内层的Project算子中或最终的Project算子中|
|Batch UDF|对用户自定义函数进行一些特别的处理|HandleNullInputForUDF|用来处理输入数据为Null的情形，主要思想是从上至下进行表达式的遍历，<br>当匹配到ScalaUDF类型的表达式时，会创建If表达式来进行Null值的检查|
|Batch FixNullability| |FixNullability|用来统一设定LogicaPlan中表达式的nullable属性。在DataFrame或Dataset等编程接口中，<br>用户代码对于某些列可能会改变其nullability属性，导致后续的判断逻辑中出现异常结果，<br>在FixNullability规则中，对解析后的LogicalPlan执行transformExpressions操作，<br>如果某列来自于其子节点，则其nullability值根据子节点对应的输出信息进行设置|
|Batch Cleanup| |CleanupAliases|用来删除LogicalPlan中无用的别名信息。一般情况下，逻辑算子树中仅Project、Aggregate或Window算子的最高一层表达式才需要别名，<br>CleanupAliases通过trimAliases方法对表达式执行中的别名进行删除|

</div>

Unresolved LogicalPlan的解析是一个不断的迭代过程，用户可以通过参数（spark.sql.optimizer.maxIterations）设定RuleExecutor迭代的轮数，默认配置为50轮，对于某些嵌套较深的特殊SQL，可以适当地增加轮数。

QueryExecution类中触发Analyzer执行的是execute方法，即RuleExecutor中的execute方法，该方法会循环地调用规则对逻辑算子树进行分析。

#### Optimized LogicalPlan生成

Optimizer继承自RuleExecutor类，执行过程是调用父类RuleExecutor中实现的executor方法，Optimizer中重新定义了一系列规则，即RuleExecutor类中的成员变量batches。在QueryExecution中，Optimizer会对传入的Analyzed LogicalPlan执行execute方法，启动优化过程。

```scala
val optimizedPlan: LogicalPlan = optimizer.execute(analyzed)
```

SparkOptimizer继承自Optimizer，实现了16个Batch（Optimizer自身定义了12个规则Batch，SparkOptimizer类又添加了4个Batch）：

<div class="wrapper" markdown="block">

|组|组描述|规则|规则描述|
|---|---|---|---|
|Batch Finish Analysis| |EliminateSubqueryAliases|消除子查询别名，直接将SubqueryAlias节点替换为其子节点^[subqueries仅用于提供查询的视角范围信息，一旦Analyzer阶段结束，该节点就可以被移除]|
| | |ReplaceExpressions|表达式替换，在逻辑算子树中查找匹配RuntimeReplaceable的表达式并将其替换为能够执行的正常表达式，<br>通常用来对其他类型的数据库提供兼容能力，如用coalesce替换nvl表达式|
| | |ComputeCurrentTime|计算与当前时间相关的表达式，在同一条SQL语句中可能包含多个计算时间的表达式，即CurrentDate和CurrentTimestamp，<br>且该表达式出现在多个语句中，未避免不一致，ComputeCurrentTime对逻辑算子树中的时间函数计算一次后，将其他同样的函数替换成该计算结果|
| | |GetCurrentDatabase|执行CurrentDatabase并得到结果，然后用此结果替换所有CurrentDatabase表达式|
| | |RewriteDistinctAggregates|重写Distinct聚合操作，将包含Distinct算子的聚合语句转换为两个常规的聚合表达式|
|Batch Union|针对Union操作的规则|CombineUnions|在逻辑算子树中当相邻的节点都是Union算子时，将这些相邻的Union节点合并为一个Union节点|
|Batch Subquery| |OptimizeSubqueries|当SQL语句包含子查询时，会在逻辑算子树上生成SubqueryExpression表达式，<br>OptimizeSubqueries优化规则在遇到SubqueryExpression表达式时，进一步递归调用Optimizer对该表达式的子计划并进行优化|
|Batch Replace Operators|主要用来执行算子的替换操作^[在SQL语句中，某些查询算子可以直接改写为已有的算子]|ReplaceIntersectWithSemiJoin|将Intersect操作算子替换为Left-Semi Join操作算子，仅适用于INTERSECT DISTINCT类型的语句，<br>而不适用于INTERSECT ALL语句，该优化规则执行前必须消除重复的属性，避免生成的Join条件不正确|
| | |ReplaceExceptWithAntiJoin|将Except操作算子替换为Left-Anti Join操作算子，仅适用于EXCEPT DISTINCT类型的语句，而不适用于EXCEPT ALL语句，<br>该优化规则执行之前必须消除重复的属性，避免生成的Join条件不正确|
| | |ReplaceDistinctWithAggregate|将Distinct算子转换为Aggregate语句，将SQL语句中直接进行Distinct操作的Select语句替换为对应的Group By语句|
|Batch Aggregate|处理聚合算子中的逻辑|RemoveLiteralFromGroupExpressions|用来删除Group By语句中的常数。如果Group By语句中全部是常数，则会将其替换为一个简单的常数0表达式|
| | |RemoveRepetitionFromGroupExpressions|将重复的表达式从Group By语句中删除|
|Batch Operator Optimizations|算子下推（Operator Push Down）^[主要是将逻辑算子树中上层的算子节点尽量下推，使其靠近叶子节点<br>能够在不同程度上减少后续处理的数据量甚至简化后续的处理逻辑]|PushProjectionThroughUnion|列剪裁下推|
| | |ReorderJoin|Join顺序优化|
| | |EliminateOuterJoin|OuterJoin消除|
| | |PushPredicateThroughJoin|谓词下推到Join算子|
| | |PushDownPredicate|谓词下推|
| | |LimitPushDown|Limit算子下推，将LocalLimit算子下推到Union All和Outer Join操作算子的下方，减少这两种算子在实际计算过程中需要处理的数据量|
| | |ColumnPruning|列剪裁|
| | |InferFiltersFromConstraints|约束条件提取|
| |算子组合（Operator Combine）^[将逻辑算子树中能够进行组合的算子尽量整合在一起，避免多次计算，以提高性能，主要针对的是重分区（repartition）算子、投影（Project）算子、过滤（Filter）算子、Window算子、Limit算子和Union算子]|CollapseRepartition|重分区组合|
| | |CollapseProject|投影算子组合|
| | |CollapseWindow|Window组合|
| | |CollapseFilters|过滤条件组合|
| | |CollapseLimits|Limit操作组合|
| | |CollapseUnions|Union算子组合|
| |常量折叠与长度消减<br>（Constant Folding and <br>Strength Reduction）^[对于逻辑算子树中涉及某些常量的节点，可以在实际执行之前就完成静态处理]|NullPropagation|Null提取|
| | |FoldablePropagation|可折叠算子提取|
| | |OptimizeIn|In操作优化|
| | |ConstantFolding|常数折叠，对于能够可折叠（foldable）的表达式会直接在EmptyRow上执行evaluate操作，从而构造新的Literal表达式|
| | |ReorderAssociativeOperator|重排序关联算子优化|
| | |LikeSimplification|Like算子简化|
| | |BooleanSimplification|Boolean算子简化|
| | |SimplifyConditionals|条件简化|
| | |RemoveDispensableExpressions|Dispensable表达式消除|
| | |SimplifyBinaryComparison|比较算子优化|
| | |PruneFilters|过滤条件裁剪，会详细的分析过滤条件，对总是能够返回true或false的过滤条件进行特别的处理|
| | |EliminateSorts|排序算子消除|
| | |SimplifyCasts|Cast算子简化|
| | |SimplifyCaseConversionExpressions|case表达式简化|
| | |RewriteCorrelatedScalarSubquery|依赖子查询重写|
| | |EliminateSerialization|序列化消除|
| | |RemoveAliasOnlyProject|消除别名|
|Batch Check Cartesian Products| |CheckCartesianProducts|检测逻辑算子树中是否存在笛卡尔积类型的Join操作。必须在ReorderJoin规则执行之后才能执行，<br>确保所有的Join条件收集完毕，当配置项spark.sql.crossJoin.enabled为true时，该规则会被忽略|
|Batch Decimal Optimizations| |DicimalAggregates|处理聚合操作中与Decimal类型相关的问题。如果聚合查询中涉及浮点数的精度处理，<br>性能就会受到很大的影响，对于固定精度的Decimal类型，DecimalAggregates规则将其当作unscaled Long类型来执行，这样可以加速聚合操作的速度|
|Batch Typed Filter Optimization| |CombineTypedFilters|对特定情况下的过滤条件进行合并，当逻辑算子树中存在两个TypedFilter过滤条件且针对同类型的对象条件时，<br>CombineTypedFilters优化规则会将它们合并到同一个过滤函数中|
|Batch LocalRelation|优化与LocalRelation相关的逻辑算子树|ConvertToLocalRelation|将LocalRelation上的本地操作转换为另一个LocalRelation，当前仅处理Project投影操作|
| | |PropagateEmptyRelation|将包含空的LocalRelation进行中折叠|
|Batch OptimizeCodegen| |OptimizeCodegen|用来对生成的代码进行优化，主要针对的是case when语句，当case when语句中的分支数目不超过配置中的最大数据时，该表达式才能执行代码生成|
|Batch RewriteSubquery|优化子查询|RewritePredicateSubquery|将特定的子查询谓词逻辑转换为left-semi/anti join操作，其中EXISTS和NOT EXISTS算子分别对应semi和anti类型的join，<br>过滤条件会被当作Join的条件，IN和NOT IN也分别对应semi和anti类型的Join，过滤条件和选择的列都会被当作join的条件|
| | |CollapseProject|会将两个相邻的Project算子组合在一起并执行别名替换，整合成一个统一的表达式|
|Batch Optimize Metadata Only Query| |OptimizeMetadataOnlyQuery|优化执行过程中只需查找分区级别元数据的语句，适用于扫描的所有列都是分区列且包含聚合算子的情形，并且聚合算子需要满足以下情况之一：<br>聚合表达式是分区列；分区列的聚合函数有DISTINCT算子；分区列的聚合函数中是否有DISTINCT算子不影响结果
|Batch Extract Python UDF from Aggregate|仅执行一次|ExtractPythonUDFFromAggregate|提取出聚合操作中的Python UDF函数。主要针对采用PySpark提交查询的情形，将参与聚合的Python自定义函数提取出来，在聚合操作完成之后执行|
|Batch Prune File Source Table Partitions|仅执行一次|PruneFileSourcePartitions|对数据文件中的分区进行裁剪操作，当数据文件中定义了分区信息且逻辑算子树中的LogicalRelation节点上方存在过滤算子时，<br>PruneFileSourcePartitions优化规则会尽可能地将过滤算子下推到存储层，这样可以避免读入无关的数据分区|
|Batch User Provided Optimizers|支持用户自定义的优化规则^[ExperimentalMethods的extraOptimizations队列默认为空，用户只需要继承Rule`[LogicalPlan]`虚类，实现相应的转化逻辑就可以注册到优化规则队列中应用执行]| | |

</div>

### Spark SQL物理计划（PhysicalPlan）

物理计划是与底层平台紧密相关的，在此阶段，Spark SQL会对生成的逻辑算子树进行进一步处理，得到物理算子树，并将LogicalPlan节点及其所包含的各种信息映射成Spark Core计算模型的元素，如RDD、Transformation和Action等，以支持其提交执行。

在Spark SQL中，物理计划用SparkPlan表示，从Optimized LogicalPlan传入到Spark SQL物理计划提交并执行，主要经过3个阶段：
1. 由SparkPlanner将各种物理计划策略（Strategy）作用于对应的LogicalPan节点上，生成SparkPlan列表^[一个LogicalPlan可能产生多种SparkPlan]
2. 选取最佳的SparkPlan，Spark 2.1版本中直接在候选列表中直接用`next()`方法获取第一个
3. 提交前进行准备工作，进行一些分区排序方面的处理，确保SparkPlan各节点能够正确执行，这一步通过`prepareForExecution()`方法调用若干规则（Rule）进行转换

在Spark SQL中，当逻辑计划处理完毕后，会构造SparkPlanner并执行`plan()`方法对LogicalPlan进行处理，得到对应的物理计划（`Iterator[SparkPlan]`）^[一个逻辑计划可能会对应多个物理计划]。

SparkPlanner继承自SparkStrategies类，SparkStrategies类继承自QueryPlanner基类，`plan()`方法是现在QueryPlanner类中，SparkStrategies类本身不提供任何方法，而是在内部提供一批SparkPlanner会用到的各种策略（Strategy）实现，最后，在SparkPlanner层面将这些策略整合在一起，通过`plan()`方法进行逐个应用。SparkPlanner本身只是一个逻辑的驱动，各种策略的`apply`方法把逻辑执行计划算子映射成物理执行计划算子。SparkLater是一种特殊的SparkPlan，不支持执行（`doExecute()`方法没有实现），仅用于占位，等待后续步骤处理。

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members

class QueryPlanner {
    strategies: Seq[GenericStrategy[PhysicalPlan]]
    plan(plan: LogicalPlan): Iterator[PhysicalPlan]
    collectPlaceholders(plan: PhysicalPlan)：Seq[(PhysicalPlan, LogicalPlan)]
    prunePlans(plans: Iterator[PhysicalPlan])：Iterator[PhysicalPlan]
}

class SparkStrategies

class SparkPlanner {
    numPartitions: Int
    strategies: Seq[String]
    collectPlaceholders(plan: SparkPlan): Seq[(SparkPlan, LogicalPlan)]
    prunePlans(plans: Iterator[SparkPlan]): Iterator[SparkPlan]
    pruneFilterProject(...): SparkPlan
}

QueryPlanner <|-- SparkStrategies
SparkStrategies <|-- SparkPlanner

@enduml
```

生成物理计划的过程：`plan()`方法传入LogicalPlan作为参数，将strategies应用到LogicalPlan，生成物理计划候选集合（Candidates）。如果该集合中存在PlanLater类型的SparkPlan，则通过placeholder中间变量取出对应的LogicalPlan后，递归调用`plan()`方法，将PlanLater替换为子节点的物理计划，最后，对物理计划列表进行过滤，去掉一些不够搞笑的物理计划。

```Scala
def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    val candidates = strategies.iterator.flatMap(_(plan))
    val plans = candidates.flatMap { candidate =>
        val placeholders = collectPlaceholders(candidate)
        if (placeholders.isEmpty) {
            Iterator(candidate)
        } else {
            placeholders.iterator.foldLeft(Iterator(candidate)) {
                case (candidatesWithPlaceholders, (placeholder, LogicalPlan)) => 
                    val childPlans = this.plan(logicalPlan)
                    candidatesWithPlaceholders.flatMap { candidateWithPlaceholders => 
                        childPlans.map { childPlan => 
                            candidateWithPlaceholders.transformUp {
                                case p if p == placeholder => childPlan
                            }
                        }
                    }
            }
        }
    }
    val pruned = prunePlans(plans)
    assert(pruned.hashNext, s"No plan for $plan")
    pruned
}
```

物理执行计划策略都继承自GenericStrategy类，其中定义了`planLater`和`apply`方法，SparkStrategy类继承自GenericStrategy类，对其中的planLater进行了实现，根据传入的LogicalPlan直接生成PlanLater节点。各种具体的Strategy都实现了`apply`方法，将传入的LogicalPlan转换为SparkPlan的列表，如果当前的执行策略无法应用于该LogicalPlan节点，则返回的物理执行计划列表为空。在实现上，各种Strategy会匹配传入的LogicalPlan节点，根据节点或节点组合的不同情形实行一对一的映射或多对一的映射。如BasicOperators中实现了各种基本操作的转换，其中列出了大量的映射关系。多对一的情况涉及对多个LogicalPlan节点进行组合转换，称为逻辑算子树的模式匹配，目前逻辑算子树的节点模式共有4种：

+ ExtractEquiJoinKeys：针对具有相等条件的join操作的算子集合，提取出其中的Join条件、左子节点和右子节点等信息
+ ExtractFiltersAndInnerJoins：收集Inner类型Join操作中的过滤条件，目前仅支持对左子树进行处理
+ PhysicalAggregation：针对聚合操作，提取出聚合算子中的各个部分，并对一些表达式进行初步的转换
+ PhysicalOperation：匹配逻辑算子树中的Project和Filter等节点，返回投影列、过滤条件集合和子节点

在SparkPlanner中默认添加了8种Strategy来生成物理计划：
+ 文件数据源策略（FileSourceStrategy）：面向的是来自文件的数据源，能够匹配PhysicalOperation模式^[Project节点加上Filter节点]加上LogicalRelation节点，在这种情况下，该策略会根据数据文件信息构建FileSourceScanExec物理执行计划，并在此物理执行计划后添加过滤（FilterExec）与列剪裁（ProjectExec）物理计划
+ DataSourcesStrategy：
+ DDL操作策略（DDLStrategy）：仅针对CreateTable和CreateTempViewUsing这两种类型的节点，这两种情况都直接生成ExecutedCommandExec类型的物理计划
+ SpecialLimits：
+ Aggregation：
+ JoinSelection：
+ 内存数据表扫描策略（InMemoryScans）：主要针对InMemoryRelation LogicalPlan节点，能够匹配PhysicalOperation模式加上InMemoryRelation节点，生成InMemoryTableScanExec，并调用SparkPlanner中的pruneFilterProject方法对其进行过滤和列剪裁
+ 基本操作策略（BasicOperators）：专门针对各种基本操作类型的LogicalPlan节点，如排序、过滤等，一般一对一地进行映射即可，如Sort对应SortExec、Union对应UnionExec等

在物理算子树中，叶子类型的SparkPlan节点负责从无到有地创建RDD，每个非叶子类型的SparkPlan节点等价于在RDD上进行一次Transformation，即通过调用`execute()`函数转换成新的RDD，最终执行`collect()`操作触发计算，返回结果给用户。SparkPlan在对RDD做Transformation的过程中除对数据进行操作外，还可能对RDD的分区做调整，另外，SparkPlan除实现execute方法外，还支持直接执行executeBroadcast将数据广播到集群。SparkPlan的主要功能可以划分为3大块：
+ Metadata与Metric体系：将节点元数据（Metadata）与指标（Metric）信息以Key-Value的形式保存在Map数据结构中。元数据和指标信息是性能优化的基础，SparkPlan提供了Map类型的数据结构来存储相关信息，元数据信息Metadata对应Map中的key和value都是字符串类型，一般情况下，元数据主要用于描述数据源的一些基本信息（如数据文件的格式、存储路径等），指标信息Metrics对应Map中的key为字符串类型，而value部分是SQLMetrics类型，在Spark执行过程中，Metrics能够记录各种信息，为应用的诊断和优化提供基础。在对Spark SQL进行定制时，用户可以自定义一些指标，并将这些指标显示在UI上，一方面，定义越多的指标会得到越详细的信息，另一方面，指标信息要随着执行过程而不断更新，会导致额外的计算，在一定程度上影响性能
+ Partitioning与Ordering体系：进行数据分区（Partitioning）与排序（Ordering）处理。requiredChildChildDistribution和requiredChildOrdering分别规定了当前SparkPlan所需的数据分布和数据排序方式列表，本质上是对所有子节点输出数据（RDD）的约束。outputPartitioning定义了当前SparkPlan对输出数据（RDD）的分区操作，outputOrdering则定义了每个数据分区的排序方式
+ 执行操作部分：以execute和executeBroadcast方法为主，支持提交到Spark Core去执行

根据SparkPlan的子节点数目，可以将其大致分为4类：LeafExecNode、UnaryExecNode、BinaryExecNode和其他。

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members

class SparkPlan {
    medadata: Map[String, String]
    metrics: Map[String, SQL Metric]
    resetMetrics: Unit
    longMetric(name: String): SQLMetric
    ---
    outputPartitioning: Partitioning
    requiredChildDistribution: Seq[Distribution]
    outputOrdering: Seq[SortOrder]
    requiredChildOrdering: Seq[Seq[SortOrder]]
    ---
    doExecuteBroadcast[T](): Broadcast[T]
    executeBroadcast[T](): Broadcast[T]
    doExecute(): RDD[InternalRow]
    execute(): RDD[InternalRow]
}
@enduml
```

<div class="wrapper" markdown="block">

|分组|分组描述|SparkPlan|描述|
|---|---|---|---|
|LeafExecNode类型|创建初始RDD|RangeExec|利用SparkContext中的parallelize方法生成给定范围内的64位数据的RDD|
| | |LocalTableScanExec| |
| | |ExternalRDDScanExec| |
| | |HiveTableScanExec|会根据Hive数据表存储的HDFS信息直接生成HadoopRDD|
| | |InMemoryTableScanExec| |
| | |DataSourceScanExec|FileSourceScanExec：根据数据表所在的源文件生成FileScanRDD<br>RawDataSourceScanExec|
| | |RDDScanExec| |
| | |StreamingRelationExec| |
| | |ReusedExchangeExec| |
| | |PlanLater| |
| | |MyPlan| |
|UnaryExecNode类型|负责RDD的转换操作|InputAdapter| |
| | |Exchange|对子节点产生的RDD进行重分区，包括BroadcastExchange、ExecShuffleExchange|
| | |InsertIntoHiveTable| |
| | |MapGroupsExec| |
| | |ProjectExec|对子节点产生的RDD进行列剪裁|
| | |ReferenceSort| |
| | |SampleExec|对子节点产生的RDD进行采样|
| | |ScriptTransformation| |
| | |SortAggregateExec| |
| | |SortExec|按照一定条件对子节点产生的RDD进行排序|
| | |StateStoreSaveExec| |
| | |SubqueryExec| |
| | |GenerateExec| |
| | |AppendColumnsExec| |
| | |BaseLimitExec|LocalLimitExec<br>GlobalLiimitExec|
| | |ExpandExec| |
| | |CoalesceExec| |
| | |CollectLimitExec| |
| | |DebugExec| |
| | |ObjectConsumerExec|AppendColumnsWithObjectExec<br>MapElementsExec<br>MapPartitionsExec<br>SerializeFromObjectExec|
| | |DeserializeToObjectExec| |
| | |ExceptionInjectingOpeartor| |
| | |FilterExec|对子节点产生的RDD进行行过滤操作|
| | |FlatMapGroupsInRExec| |
| | |WholeStageCodegenExec|将生成的代码整合成单个Java函数|
| | |HashAggregateExec| |
| | |TakeOrderedAndProjectExec| |
| | |WindowExec| |
|BinaryExecNode类型|除CoGroupExec外都是不同类型的Join执行计划|BroadcastHashJoinExec| |
| | |BroadcastNestedLoopJoinExec| |
| | |CartesianProductExec| |
| | |CoGroupExec|处理逻辑类似Spark Core中CoGroup操作，将两个要进行合并的左、右子SparkSplan所产生的RDD，按照相同的key值组合到一起，<br>返回的结果中包含两个Iterator，分别代表左子树中的值与右子树中的值|
| | |ShuffledHashJoinExec| |
| | |SortMergeJoinExec| |
|其他类型的SparkPlan| |CodegenSupport| |
| | |UnionExec| |
| | |DummySparkPlan|对每个成员赋予默认值|
| | |FastOperator| |
| | |MyPlan|用于在Driver端更新Metric信息|
| | |EventTimeWatermarkExec| |
| | |BatchEvalPythonExec| |
| | |ExecutedCommandExec| |
| | |OutputFakerExec| |
| | |StatefulOperator| |
| | |ObjectProducerExec| |

</div>

在SparkPlan分区体系实现中，Partitioning表示对数据进行分区的操作，Distribution则表示数据的分布，在Spark SQL中，Distribution和Partitioning均被定义为接口，具体实现有多个类。

Distribution定义了查询执行时，同一个表达式下的不同数据元组在集群各个节点上的分布情况，可以用来描述以下两种不同粒度的数据特征：一是节点间（Inner-node）分区信息，即数据元组在集群不同的物理节点上是如何分区的，用来判断某些算子（如Aggregate）能否进行局部计算（Partial Operation），避免全局操作的代价；一是分区数据内（Inner-partition）排序信息，即单个分区内数据是如何分布的。在Spark 2.1中包括以下5种Distribution实现：
+ UnspecifiedDistribution：未指定分布，无需确定数据元组之间的位置关系
+ AllTuples：只有一个分区，所有的数据元组存放在一起（Co-located）
+ BroadcastDistribution：广播分布，数据会被广播到所有节点上，构造参数mode为广播模式（BroadcastMode），广播模式可以为原始数据（IdentityBroadcastMode）或转换为HashedRelation对象（HashedRelationBroadcastMode）
+ ClusteredDistribution：构造参数clustering是`Seq[Expression]`类型，起到了哈希函数的效果，数据经过clustering计算后，相同value的数据元组会被存放在一起（Co-located），如果有多个分区的情况，则相同数据会被存放在同一个分区中；如果只能是单个分区，则相同的数据会在分区内连续存放
+ OrderedDistribution：构造参数ordering是`Seq[SortOrder]`类型，该分布意味着数据元组会根据ordering计算后的结果排序

Partitioning定义了一个物理算子输出数据的分区方式，具体包括子Partitionging之间、目标Partitioning和Distribution之间的关系，描述了SparkPlan中进行分区的操作，类似直接采用API进行RDD的repartition操作。Partitioning接口中包括1个成员变量和3个函数来进行分区操作：
+ `numPartitions`：指定该SparkPlan输出RDD的分区数目
+ `satisfies(required: Distribution)`：当前的Partitioning操作能否得到所需的数据分布，当不满足时一般需要进行repartition操作，对数据进行重新组织
+ `compatibleWith(other：Partitioning)`：当存在多个子节点时，需要判断不同的子节点的分区操作是否兼容，只有当两个Partitioning能够将相同key的数据分发到相同的分区时，才能够兼容
+ `guarantees(other: Partitioning)`：如果`A.gurantees(B)`为真，那么任何A进行分区操作所产生的数据行业能够被B产生，这样，B就不需要再进行重分区操作，该方法主要用来避免冗余的重分区操作带来的性能代价。在默认情况下，一个Partitioning仅能够gurantee（保证）等于它本身的Partitioning（相同的分区数目和相同的分区策略等）

Partitioning接口的具体实现有以下几种：
+ UnknownPartitioning：不进行分区
+ RoundRobinPartitioning：在1-numPartitions范围内轮询式分区
+ HashPartitioning：基于哈希的分区方式
+ RangePartitioning：基于范围的分区方式
+ PartitioningCollection：分区方式的集合，描述物理算子的输出

```scala
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int) extends Expression with Partitioning with Unevaluable {
    override def satisfies(required: Distribution): Boolean = required match {
        case UnspecifiedDistribution => true
        case ClusteredDistribution(requiredClustering) => expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
    }
    override def comparitableWith(other: Partitioning): Boolean = other match {
        case o: HashPartitioning => this.semanticEquals(o)
        case _ => false
    }
    override def guarantees(other: Partitioning): Boolean = other match {
        case o: HashPartitioning => this.semanticEquals(o)
        case _ => false
    }
}
```

在SparkPlan默认实现中，`outputPartitioning`设置为`UnknownPartitioning(0)`，`requiredChildDistribution`设置为`Seq[UnspecifiedDistribution]`，且在数据有序性和排序操作方面不涉及任何动作。FileSourceScanExec中的分区排序信息会根据数据文件构造的初始的RDD进行设置，如果没有bucket信息，则分区与排序操作将分别为最简单的UnknownPartitioning与Nil；当且仅当输入文件信息中满足特定的条件时，才会构造HashPartitioning与SortOrder类。FilterExec（过滤执行算子）与ProjectExec（列剪裁执行算子）中分区与排序方式仍然沿用其子节点的方式，即不对RDD的分区与排序进行任何的重新操作。通常情况下，LeafExecNode类型的SparkPlan会根据数据源本身的特点（包括分块信息和有序性特征）构造RDD与对应的Partitioning和Ordering方式，UnaryExecNode类型的SparkPlan大部分会沿用其子节点的Partitioning和Ordering方式（SortExec等本身具有排序操作的执行算子例外），而BinaryExecNode往往会根据两个子节点的情况综合考虑。

得到SparkPlan后，还需对树型结构的物理计划进行全局的整合处理和优化。在QueryExecution中，最后阶段由`prepareforExecution`方法对传入的SparkPlan进行处理而生成executedPlan，处理过程仍然基于若干规则，主要包括对Python中UDF的提取、子查询的计划生成等。
+ python.ExtractPythonUDFs：提取Python中的UDF函数
+ PlanSubqueries：处理物理计划中的ScalarSubquery和PredicateSubquery这两种特殊的子查询。Spark 2.0版本及以上支持两种特殊情形的子查询，即Scalar类型和Predicate类型。Scalar类型子查询返回单个值，又分为相关的（Correlated）和不相关的（Uncorrelated）类型，Uncorrelated子查询和主查询不存在相关性，对于所有的数据行都返回相同的值，在主查询执行之前，会首先执行，Correlated子查询包含了外层主查询中的相关属性，会等价转换为Left Join算子。Predicate类型子查询作为过滤谓词使用，可以出现在EXISTS和IN语句中。PlanSubqueries处理这两种特殊子查询的过程为，遍历物理算子树中的所有表达式，碰到ScalarSubquery或PredicateSubquery表达式时，进入子查询中的逻辑，递归得到子查询的物理执行计划（executedPlan），然后封装为ScalarSubquery和InSubquery表达式
    ```scala
    case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
        def apply(plan: SparkPlan): SparkPlan = {
            plan.transformAllExpressions {
                case subquery: expressions.ScalarSubquery =>
                    val executedPlan = new QueryExecution(sparkSession, subquery.plan).executedPlan
                    ScalarSubquery(SubqueryExec(s"subquery${subquery.exprId.id}", executePlan), subquery.exprId)
                case expressions.PredicateSubquery(query, Seq(e: Expression), _, exprId) =>
                    val executePlan = new QueryExecution(sparkSession, query).executedPlan
                    InSubquery(e, SubqueryExec(s"subquery${exprId.id}", executedPlan), exprId)
            }
        }
    }
    ```
+ EnsureRequirements：用来确保物理计划能够执行所需要的前提条件，包括对分区和排序逻辑的处理
+ CollapseCodegenStages：代码生成相关
+ ReuseExchange：Exchange节点重用
+ ReuseSubquery：子查询重用

```scala
lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

protected def prepareForexecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apple(sp) }
}
```

EnsureRequirements在遍历SparkPlan的过程中，当匹配到Exchange节点（ShuffleExchange）且其子节点也是Exchange类型时，会检查两者的Partitioning方法，判断能否消除多余的Exchange节点，另外，遍历过程中会逐个调用`ensureDistributionAndOrdering`方法来确保每个节点的分区与排序需求，核心逻辑大致分为以下3步：
1. 添加Exchange节点：Exchange节点是实现数据并行化的重要算子，用于解决数据分布（Distribution）相关问题，有BroadcastExchangeExec和ShuffleExechange两种子类，ShuffleExchange会通过Shuffle操作进行重分区处理，BroadcastExchangeExec则对应广播操作。有两种情形需要添加Exchange节点：数据分布不满足，子节点的物理计划输出数据无法满足（Satisfies）当前物理计划处理逻辑中对数据分布的要求；数据分布不兼容，当前物理计划为BinaryExecNode类型，即存在两个子物理计划时，两个子物理计划的输出数据可能不兼容（Compatile）。在`ensureDistributionAndOrdering`方法中，添加Exchange节点的过程可以细分为两个阶段，分别针对单个节点和多个节点，第一个阶段是判断每个子节点的分区方式是否可以满足（Satisfies）对应所需的数据分布，如果满足，则不需要创建Exchange节点；否则根据是否广播来决定添加何种类型的Exchange节点^[例如，SortMerge类型的Join节点中requiredChildDistribution列表为`[ClusteredDistribution(leftKeys), ClusteredDistribution(rightKeys)]`，假设两个子节点的Partitioning都无法输出该数据分布，那么就会添加两个ShuffleExchange节点]。第二个阶段专门针对多个子节点的情形，如果当前SparkPlan节点需要所有子节点分区方式兼容但并不能满足时，就需要创建ShuffleExchange节点^[例如，SortMerge类型的Join节点就需要两个子节点的Hash计算方式相同，如果所有的子节点outputPartitioning能够保证由最大分区数目创建新的Partitioning，则子节点输出的数据并不需要重新Shuffle，只需要使用已有的outputPartitioning方式即可，没有必要创建新的Exchange节点；否则，至少有一个子节点的输出数据需要重新进行Shuffle操作，重分区的数目（NumPartitions）根据是否所有的子节点输出都需要Shuffle来判断，若是，则采用默认的Shuffle分区配置数目；否则，取子节点中最大的分区数目]。ShuffleExchange执行得到的RDD称为ShuffledRowRDD，ShuffleExchange执行`doExecute`时，首先会创建ShuffleDependency，然后根据ShuffleDependency构造ShuffleRowRDD,ShuffleDependency的创建分为以下两种情况，一是包含ExchangeCoordinator协调器，如果需要ExchangeCoordinator协调ShuffleRowRDD的分区，则需要先提交该ShuffleExchange之前的Stage到Spark集群执行，完成之后确定ShuffleRowRDD的分区索引信息，一是直接创建，直接执行`prepareShuffleDependency`方法来创建RDD的依赖，然后根据ShuffleDependency创建ShuffledRowRDD对象，在这种情况下，ShuffleRowRDD中每个分区的ID与Exchange节点进行shuffle操作后的数据分区是一对一的映射关系。直接创建ShuffleDependency的过程中，首先会根据传入的newPartitioning信息构造对应的partiitoner，然后基于该partitioner生成ShuffleDependency最重要的构造参数`rddWithPartitionIds(RDD[Product2[Int, InternalRow]])`类型，其中`[Int, InternalRow]`代表某行数据及其所在分区的partitionId，其取值范围为`[0, numPartitions-1]`，最终在rddWithPartitionIds基础上创建ShuffleDependency对象
    ```scala
    // 第一阶段
    // defaultNumPreShufflePartitions决定了Shuffle操作过程中分区的数目，是一个用户可配置的参数（spark.sql.shuffle.partitions），默认为200
    // createPartitioning方法会根据数据分布与分区数目创建对应的分区方式，具体对应关系是：AllTuples对应SinglePartition，ClusteredDistribution对应HashPartitioning，OrderedDistribution对应RangePartitioning
    children = children.zip(requiredChildDistributions).map {
        case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
            child
        case (child, BroadcastDistribution(mode)) =>
            BroadcastExchangeExec(mode, child)
        case (child, distribution) =>
            ShuffleExchange(createPartitioning(distribution, defaultNumPreShufflePartitions), child)
    }

    // 第二阶段
    if (children.length > 1 && requiredChildDistributions.exists(requireCompatiblePartitioning) && !Partitioning.allCompatible(children.map(_.outputPartitioning))) {
        val maxChildrenNumPartitions = children.map(_.outputPartitioning.numPartitions).max
        val useExistingPartitioning = children.zip(requiredChildDistributions).forall {..}
        children = if (useExistingPartitioning) {
            children
        } else {
            val numPartitions = {...}
            children.zip(requiredChildDistributions).map {
                case (child, distribution) =>
                    val targetPartitioning = createPartitioning(distribution, numPartitions)
                    if (child.outputPartitioning.guarantees(targetPartitioning)) {
                        child
                    } else {
                        child match {
                            case ShuffleExchange(_, c, _) => ShuffleExchange(targetPartitioning, c)
                            case _ => ShuffleExchange(targetPartitioning, child)
                        }
                    }
            }
        }
    }
    ```
2. 应用ExchangeCoordinator协调分区：ExchangeCoordinator用来确定物理计划生成的Stage之间如何进行Shuffle行为，其作用在于协助ShuffleExchange节点更好地执行。ExchangeCoordinator针对的是一批SparkPlan节点，这些节点需要满足两个条件，一个是Spark SQ的自适应机制开启（`spark.sql.adaptive.enabled`为`true`），一个是这批节点支持协调器（一种情况是至少存在一个ShuffleExchange类型的节点且所有节点的输出分区方式都是HashPartitioning，另一种情况是节点数目大于1且每个节点输出数据的分布都是ClusteredDistribution类型）。当ShuffleExchange中加入了ExchangeCoordinator来协调分区数目时，需要知道子物理计划输出的数据统计信息，因此在协调之前需要将ShuffleExchange之前的Stage提交到集群执行来获取相关信息
3. 添加SortExec节点：排序的处理在分区处理（创建完Exchange）之后，只需要对每个节点单独处理。当且仅当所有子节点的输出数据的排序信息满足当前节点所需时，才不需要添加SortExec节点，否则，需要在当前节点上添加SortExec为父节点。
    ```scala
    children = children.zip(requiredChildOrderings).map {case (child, requiredOrdering) =>
        if (requiredOrdering.nonEmpty) {
            val orderingMatched = if (requiredOrdering.length > child.outputOrdering.length) {false} else {
                requiredOrdering.zip(child.outputOrdering).forall {
                    case (requiredOrder, childOutputOrder) => requiredOrder.semanticEquals(childOutputOrder)
                }
            }
            if (!orderingMatched) {SortExec(requiredOrdering, global = false, child = child)} else {child}
        } else {
            child
        }
    }
    ```

Ensurerequirements规则的处理逻辑结束后，调用TreeNode中的`withNewChildren`将SparkPlan中原有的子节点替换为新的子节点。

## Spark Join

Spark提供了5种Join策略：
+ Broadcast Hash Join：即mapjoin，将小表的数据广播到所有Executor（利用collect算子将小表的数据从Executor拉到Driver，在Driver调用sparkContext.broadcast广播到所有 Executor），在Executor上进行Hash Join。Hash Join指将小表的数据构建为hash table，大表的分区数据匹配Hash Table中的数据。需满足以下条件，等值连接，小表数据大小小于spark.sql.autoBroadcastJoinThreshold。支持全外连接以外的所有连接类型。另外，外连接中基表不能被广播，当小表数据较大时会造成Driver OOM
+ Shuffle Hash Join：把大表和小表按照关联键进行shuffle，保证关联键相同的数据被分发到同一个分区，然后在Executor上进行Hash Join。Hash Join指将小表的分区数据构建为hash table，大表的分区数据匹配Hash Table中的数据。需满足以下条件，等值连接、spark.sql.join.preferSortMergeJoin为false、小表数据大小必须小于spark.sql.autoBroadcastJoinThreshold * spark.sql.shuffle.partitions、小表数据大小的3倍必须小于大表数据大小。支持全外连接以外的所有连接类型
+ Sort Merge Join：将两张表按照关联键进行shuffle，保证关联键相同的数据被分发到同一个分区，对每个分区内的数据按关联键进行排序，排序后再对相应分区内的数据进行Join。合并连接指从头遍历两个分区，碰到key相同的就输出，如果不同，左边小就继续取左边，反之取右边。需满足以下条件，等值Join、关联键可排序。支持所有连接类型
+ Shuffle-And-Replicate Nested Loop Join：即Cartesian Product Join，仅支持内连接，支持等值和不等值连接，设置spark.sql.crossJoin.enabled=true
+ Broadcast Nested Loop Join：广播小表数据，和大表每个分区的数据进行Nested Loop Join。支持等值连接和非等值连接，支持所有连接类型

Join基本方法有Hash Join、Sort Merge Join、Nested Loop Join。Broadcast Hash Join和Shuffle Hash Join都是Hash Join，区别在于Hash Join前是Shuffle还是Broadcast。

**spark如何选择关联策略** Join Hints按BROADCAST > MERGE > SHUFFLE_HASH > SHUFFLE_REPLICATE_NL顺序。没有Join Hints时，分以下情形

+ 等值连接时，按以下规则：

    1. 如果连接类型支持并且一张表能被广播，则选择Broadcast Hash Join
    2. 如果配置项spark.sql.join.preferSortMergeJoin设定为false，且一张表足够小，则选择Shuffle Hash Join
    3. 如果关联键是排序的，则选择Shuffle Sort Merge Join
    4. 如果是内连接，则选择Cartesian Product Join
	5. 选择Broadcast Nested Loop Join

+ 非等值连接时，按以下规则：

	1. 如果一张表能被广播，则选择Broadcast Nested Loop Join
	2. 如果是内连接，则选择Cartesian Product Join
	3. 选择Broadcast Nested Loop Join

<div class="wrapper" markdown="block">

|配置项|描述|默认值|
|---|---|---|
|spark.sql.autoBroadcastJoinThreshold|Broadcast Hash Join小表数据阈值，-1表示禁用|10M|
|spark.sql.join.preferSortMergeJoin|是否优先Sort Merge Join|true|
|spark.sql.crossJoin.enabled|是否开启Cross Join|false|

</div>

<div class="wrapper" markdown="block">

|Join Hints类型|语法|描述|
|:---:|---|---|
|BROADCAST|/\*+ BROADCAST(t) */ <br> /\*+ BROADCASTJOIN(t) */ <br> /\*+ MAPJOIN(t) */|指定Broadcast Join，无视autoBroadcastJoinThreshold广播指定的表|
|MERGE|/\*+ MERGE(t) */ <br> /\*+ MERGEJOIN(t) */ <br> /\*+ SHUFFLE_MERGE(t) */|指定Shuffle Sort Merge Join|
|SHUFFLE_HASH|/*+ SHUFFLE_HASH(t) */|指定Shuffle Hash Join，如果两个表都有SHUFFLE_HASH提示，则基于统计信息广播较小表|
|SHUFFLE_REPLICATE_NL|/*+ SHUFFLE_REPLICATE_NL(t) */|指定Cartesian Product Join|

</div>

### AQE

概括来说，Spark SQL使用基于规则的优化技术来对处理逻辑流程进行优化，使用基于性能模型的优化技术选择最优的物理执行计划，使用基于自适应的优化执行技术根据应用运行时的信息来动态调整执行计划。

在2.0版本之前，Spark SQL仅仅支持RBO（Rule Based Optimization，基于规则的优化），即启发式、静态的优化过程，如谓词下推、算子组合、常量折叠。

spark在2.2版本推出了CBO（Cost Based Optimization，基于性能模型的优化），即基于数据表的统计信息（如表大小、数据列分布、空值数、最大值、最小值等）来选择优化策略。因为有统计数据支持，所以CBO选择的策略往往优于RBO选择的优化规则。但CBO使用面太窄，仅支持注册到Hive Metastore的数据表，统计信息的搜集效率比较低，需要调用ANALYZE TABLE COMPUTE STATISTICS语句收集统计信息，执行计划运行时，如果数据分布发生动态变化，执行计划并不会跟着调整、适配。

spark在3.0版本推出了AQE（Adaptive Query Execution，自适应查询执行），AQE是Spark SQL的一种动态优化机制，在运行时，每当Shuffle Map阶段执行完毕，AQE都会结合这个阶段的统计信息，基于既定的规则动态地调整、修正尚未执行的逻辑计划和物理计划，来完成对原始查询语句的运行时优化。

AQE优化机制触发的时机是Shuffle Map阶段执行完毕，即 AQE优化的频次与执行计划中Shuffle的次数一致。AQE的统计信息基于Shuffle Map阶段输出的中间文件。每个Map Task都会输出以data为后缀的数据文件和以index为结尾的索引文件，统称为中间文件。每个data文件的大小、空文件数量与占比、每个Reduce Task对应的分区大小，这些基于中间文件的统计值构成了AQE进行优化的信息来源。AQE从运行时获取统计信息，在条件允许的情况下，优化决策会分别作用到逻辑计划和物理计划。

AQE实现基于DAGScheduler支持提交单个mapStage。启用AQE后，Spark会将逻辑执行计划拆分为多个QueryStage, 在执行QueryStage时先提交其子QueryStage，在所有子QueryStage完成后，收集到shuffle数据统计信息，并根据这些信息重新启动逻辑优化阶段和物理计划阶段，动态更新查询计划。AQE定义了ShuffleQueryStage（将其输出物化为Shuffle文件）和BroadcastQueryStage（将其输出物化到Driver内存中的数组）两种QueryStage，分别对应Shuffle和Broadcast。对于物理执行计划，AQE会在原执行计划中查找Exchange并添加两个新的操作节点QueryStage（是一个阶段的根节点，负责运行时决策）和QueryStageInput（是一个阶段的叶子节点，负责在物理计划更新后将子阶段的结果提供给其父阶段）。

AQE主要有三大特性：

+ Join策略调整：包括规则DemoteBroadcastHashJoin和规则OptimizeLocalShuffleReader，统计Map阶段中间文件总大小、中间文件空文件占比，存在小表时自动将Shuffle Sort Merge Join降级为Broadcast Hash Join
+ 自动分区合并：包括规则CoalesceShufflePartitions，在Shuffle Read阶段，当Shuffle Read Task把数据分片从map端拉回时，AQE按照分区编号的顺序，依次把小于目标尺寸的分区合并在一起
+ 自动倾斜处理：包括规则OptimizeSkewedJoin，统计每个Reduce Task分区大小，自动拆分过大的Reduce Task分区，降低单个Reduce Task的工作负载

**DemoteBroadcastHashJoin** 逻辑优化规则，对于参与join^[仅适用于Shuffle Sort Merge Join]的两张表，如果在它们分别完成Shuffle Map阶段的计算之后，存在一张表同时满足中间文件尺寸总和小于广播阈值（配置项spark.sql.autoBroadcastJoinThreshold设置）和空文件占比小于配置项（配置项spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin设置），Shuffle Sort Merge Join就会降级为Broadcast Hash Join

**OptimizeLocalShuffleReader** 物理优化规则，用于避免Shuffle Sort Merge Join降级为Broadcast Hash Join时大表的Shuffle操作，Reduce Task就地读取本地节点的中间文件，完成与广播小表的关联操作，由spark.sql.adaptive.localShuffleReader.enabled设置，默认值为true

**CoalesceShufflePartitions** 物理优化规则，在Shuffle Read阶段，当Reduce Task拉回数据分片时，AQE按照分区编号的顺序，依次把小于目标分区大小的分区合并在一起，目标分区尺寸由spark.sql.adaptive.advisoryPartitionSizeInBytes和spark.sql.adaptive.coalescePartitions.minPartitionNum控制

**OptimizeSkewedJoin** 物理优化规则，在Shuffle Read阶段，当Reduce Task所需处理的分区大小大于一定阈值时，AQE会把大分区拆分成多个小分区，在同一个Executor内部，本该由一个Task去处理的大分区，被拆分成多个小分区并交由多个Task去计算。倾斜分区和拆分粒度由spark.sql.adaptive.skewJoin.skewedPartitionFactor、spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes和spark.sql.adaptive.advisoryPartitionSizeInBytes控制。自动倾斜处理只能解决Task之间的计算负载平衡，不能解决不同Executor之间的负载均衡问题。当应用场景中的数据倾斜比较简单，如虽然有倾斜但是数据分布相对均匀或是关联计算中只有一边倾斜，完全可以依赖AQE的自动倾斜处理机制，当时当应用场景中的数据倾斜很复杂，如数据中不同key的分布悬殊，或是参与关联的两表都存在大量的倾斜，就需要衡量AQE的自动化机制与手工处理倾斜之间的利害关系。

<div class="wrapper" markdown="block">

|配置项|描述|默认值|
|---|---|---|
|spark.sql.adaptive.enabled|是否开启自适应查询|false|
|spark.sql.adaptive.logLevel|自适应执行的计划改变日志的日志级别|debug|
|spark.sql.autoBroadcastJoinThreshold|转为Broadcast Hash Join的小表阈值|10 MB|
|spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin|转为Broadcast Hash Join的非空分区比例阈值|0.2|
|spark.sql.adaptive.localShuffleReader.enabled|是否开启本地Shuffle读取|true|
|spark.sql.adaptive.coalescePartitions.enabled|是否开启分区合并|true|
|spark.sql.adaptive.advisoryPartitionSizeInBytes|分区建议大小，合并分区和优化倾斜时使用|64MB|
|spark.sql.adaptive.coalescePartitions.minPartitionNum|分区合并后的最小分区数|spark集群的默认并行度|
|spark.sql.adaptive.skewJoin.enabled|是否开启数据倾斜自适应优化|true|
|spark.sql.adaptive.skewJoin.skewedPartitionFactor|判断倾斜的膨胀系数|5|
|spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes|判断倾斜的分区大小阈值|256MB|

</div>

首先，Spark SQL经过catalyst转换为物理执行计划。接着，在AQE开启后，AdaptiveSparkPlanExec被执行时，它会调用`getFinalPhysicalPlan()`方法来启动执行流程，通过递归地自上而下地调用`createQueryStages()`方法，将物理执行计划转换为包含QeuryStageExec的树（主要是将broadcast和shuffle转换为其对应的QueryStageExec），它将物化其输出，并将输出的统计信息用于优化后续查询阶段。在第一次转换之后，会记录所有新的和未解决的QueryStage，这些阶段稍后会提交执行。在newQueryStage中会调用AQE物理计划优化。然后，将当前逻辑执行计划中的所有querystages替换为LogicalQueryStage，转换后的逻辑计划有新的统计数据可用，再调用其`reOptimize()`方法产生全新的逻辑执行计划和物理执行计划，并比较新旧计划的成本，如果新的计划比前一个更优，则将其提升为新的currentPhysicalPlan和currentLogicalPlan。一旦执行了所有查询阶段，就会生成最终的物理计划，在`reOptimize()`中会调用AQE逻辑优化规则。最后，会再次应用AQE物理优化规则，并返回最终的物理计划。

```scala
// 物理优化规则InsertAdaptiveSparkPlan是AQE开启入口，在QeuryExecution#preparations()中被调用（Spark SQL从逻辑计划转换为物理计划后，还需要经过preparations阶段才能执行）
class QueryExecution(
    val sparkSession: SparkSession,
    val logical: LogicalPlan,
    val tracker: QueryPlanningTracker = new QueryPlanningTracker) {

    def preparations: Seq[Rule[SparkPlan]] = {
        QueryExecution.preparations(
            sparkSession,
            Option(InsertAdaptiveSparkPlan(AdaptiveExecutionContext(sparkSession, this))))
    }

    def preparations(sparkSession: SparkSession, adaptiveExecutionRule: Option[InsertAdaptiveSparkPlan] = None): Seq[Rule[SparkPlan]] = {
        adaptiveExecutionRule.toSeq ++
            Seq(
                CoalesceBucketsInJoin,
                PlanDynamicPruningFilters(sparkSession),
                PlanSubqueries(sparkSession),
                RemoveRedundantProjects,
                EnsureRequirements,
                RemoveRedundantSorts,
                DisableUnnecessaryBucketedScan,
                ApplyColumnarRulesAndInsertTransitions(sparkSession.sessionState.columnarRules),
                CollapseCodegenStages(),
                ReuseExchange,
                ReuseSubquery
            )
    }
}


// 物理优化规则InsertAdaptiveSparkPlan用AdaptiveSparkPlanExec包装query plan，AdaptiveSparkPlanExec用于执行query plan并在运行时基于运行时统计信息进行再次优化
class InsertAdaptiveSparkPlan(adaptiveExecutionContext: AdaptiveExecutionContext) extends Rule[SparkPlan] {

    def apply(plan: SparkPlan): SparkPlan = applyInternal(plan, false)

    def applyInternal(plan: SparkPlan, isSubquery: Boolean): SparkPlan = plan match {
        case _ if !conf.adaptiveExecutionEnabled => plan
        // 和数据写入相关的命令算子不会应用AQE
        case _: ExecutedCommandExec => plan
        case c: DataWritingCommandExec => c.copy(child = apply(c.child))
        case c: V2CommandExec => c.withNewChildren(c.children.map(apply))
        // 检查是否应用AQE
        case _ if shouldApplyAQE(plan, isSubquery) =>
            // 检查是否支持AQE
            if (supportAdaptive(plan)) {
                // 递归处理子查询，并传入共享阶段缓存以供Exchange重用，如果任一子查询不支持AQE则退回非AQE模式
                // 构建一个子查询map
                val subqueryMap = buildSubqueryMap(plan)
                // 获取PlanAdaptiveSubqueries预处理规则
                val planSubqueriesRule = PlanAdaptiveSubqueries(subqueryMap)
                val preprocessingRules = Seq(planSubqueriesRule)
                // 应用预处理规则
                val newPlan = AdaptiveSparkPlanExec.applyPhysicalRules(plan, preprocessingRules)
                // 返回物理执行计划AdaptiveSpakPlanExec
                AdaptiveSparkPlanExec(newPlan, adaptiveExecutionContext, preprocessingRules, isSubquery)
            } else {
                plan
            }
    }

    /**
      * 检查是否对物理执行计划应用AQE
      * AQE只对包含Exchange算子或子查询的查询有效
      * 查询为子查询或者至少满足以下一个条件时才应用AQE
      * + 开启强制应用AQE（配置项spark.sql.adaptive.forceApply为true）
      * + 物理执行计划中包含Exchange算子或者需要添加Exchange算子（SparkPlan.requiredChildDistribution）
      * + 查询中包含子查询
      */
    def shouldApplyAQE(plan: SparkPlan, isSubquery: Boolean): Boolean = {
        conf.getConf(SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY)
        || isSubquery
        || {
            plan.find {
                case _: Exchange => true
                case p if !p.requiredChildDistribution.forall(_ == UnspecifiedDistribution) => true
                case p => p.expressions.exists(_.find {
                    case _: SubqueryExpression => true
                    case _ => false
                }.isDefined)
        }.isDefined}
    }

    /**
      * 检查物理执行计划是否支持AQE，必须满足以下四个条件
      * + 物理执行计划需要关联一个逻辑执行计划
      * + 物理执行计划关联的逻辑执行计划不包含流式数据源
      * + 物理执行计划不包含DynamicPruningSubquery
      * + 物理执行计划的所有子节点都必须支持AQE
      */
    def supportAdaptive(plan: SparkPlan): Boolean = {
        plan.logicalLink.isDefined    // logicalLink()返回物理执行计划关联的逻辑执行计划
        && !plan.logicalLink.exists(_.isStreaming)
        && !plan.expressions.exists(_.find(_.isInstanceOf[DynamicPruningSubquery]).isDefined)
        && plan.children.forall(supportAdaptive)
    }
    /** 为所有子查询返回表达式id到执行计划的map
      */
    def buildSubqueryMap(plan: SparkPlan): Map[Long, SubqueryExec] = {
        val subqueryMap = mutable.HashMap.empty[Long, SubqueryExec]
        plan.foreach(_.expressions.foreach(_.foreach {
            case expressions.ScalarSubquery(p, _, exprId) if !subqueryMap.contains(exprId.id) =>
                val executedPlan = compileSubquery(p)
                verifyAdaptivePlan(executedPlan, p)
                val subquery = SubqueryExec.createForScalarSubquery(s"subquery#${exprId.id}", executedPlan)
                subqueryMap.put(exprId.id, subquery)
            case expressions.InSubquery(_, ListQuery(query, _, exprId, _)) if !subqueryMap.contains(exprId.id) =>
                val executedPlan = compileSubquery(query)
                verifyAdaptivePlan(executedPlan, query)
                val subquery = SubqueryExec(s"subquery#${exprId.id}", executedPlan)
                subqueryMap.put(exprId.id, subquery)
            case _ =>
        }))
        subqueryMap.toMap
    }
}

/**
  * PlanAdaptiveSubqueries规则递归处理物理执行计划中的ScalarSubquery、InSubquery
  */
class PlanAdaptiveSubqueries(subqueryMap: Map[Long, SubqueryExec]) extends Rule[SparkPlan] {

    def apply(plan: SparkPlan): SparkPlan = {
        plan.transformAllExpressions {
            case expressions.ScalarSubquery(_, _, exprId) =>
                execution.ScalarSubquery(subqueryMap(exprId.id), exprId)
            case expressions.InSubquery(values, ListQuery(_, _, exprId, _)) =>
                val expr = if (values.length == 1) {
                    values.head
                } else {
                    CreateNamedStruct(values.zipWithIndex.flatMap { case (v, index) => Seq(Literal(s"col_$index"), v) })
                }
                InSubqueryExec(expr, subqueryMap(exprId.id), exprId)
        }
    }
}

class AdaptiveSparkPlanExec(inputPlan: SparkPlan, context: AdaptiveExecutionContext, preprocessingRules: Seq[Rule[SparkPlan]], isSubquery: Boolean) {

    def doExecute(): RDD[InternalRow] = {
        // 获取最终的物理执行计划并执行
        val rdd = getFinalPhysicalPlan().execute()
        finalPlanUpdate
        rdd
    }

    /**
      * 将Broadcast或Shuffle作为QeuryStage的划分，当一个QueryStageExec完成后就会更新CreateStageResult，同时统计数据，然后创建一个更优的物理执行计划来更新当前的物理执行计划
      */
    def getFinalPhysicalPlan(): SparkPlan = lock.synchronized {
        if (isFinalPlan) return currentPhysicalPlan

        val executionId = getExecutionId
        // 通过物理执行计划的logicalLink获取其关联的逻辑执行计划
        var currentLogicalPlan = currentPhysicalPlan.logicalLink.get
        // 将Exchange节点替换为QueryStage
        var result = createQueryStages(currentPhysicalPlan)
        val events = new LinkedBlockingQueue[StageMaterializationEvent]()
        val errors = new mutable.ArrayBuffer[Throwable]()
        var stagesToReplace = Seq.empty[QueryStageExec]
        // 检查是否所有子阶段都已物化
        while (!result.allChildStagesMaterialized) {
            currentPhysicalPlan = result.newPlan
            if (result.newStages.nonEmpty) {
                // 通知监听器物理计划已经变更
                stagesToReplace = result.newStages ++ stagesToReplace
                executionId.foreach(onUpdatePlan(_, result.newStages.map(_.plan)))

                // 开始物化所有新QueryStage
                result.newStages.foreach { stage =>
                    stage.materialize().onComplete { res =>
                        if (res.isSuccess) {
                            events.offer(StageSuccess(stage, res.get))
                        } else {
                            events.offer(StageFailure(stage, res.failed.get))
                        }
                    }(AdaptiveSparkPlanExec.executionContext)
                }
            }

            // 等待下一个完成的QueryStage，此时将有新的统计数据可用并可能创建新阶段
            val nextMsg = events.take()
            val rem = new util.ArrayList[StageMaterializationEvent]()
            events.drainTo(rem)
            (Seq(nextMsg) ++ rem.asScala).foreach {
                case StageSuccess(stage, res) =>
                    stage.resultOption.set(Some(res))
                case StageFailure(stage, ex) =>
                    errors.append(ex)
            }

            // 尝试重新优化和重新生成计划。如果新计划的成本小于等于当前计划就采用它
            // 通过replaceWithQueryStagesInLogicalPlan方法将逻辑执行计划中的所有QueryStage替换为LogicalQueryStage（LogicalQueryStage是QueryStageExec的逻辑执行计划包装）
            val logicalPlan = replaceWithQueryStagesInLogicalPlan(currentLogicalPlan, stagesToReplace)
            // 转换为logicalQueryStage后有了新的统计数据可用，然后调用reOptimize尝试重新优化，最终调用optimizer.execute(logicalPlan)方法执行
            // 目前支持以下几种逻辑优化：
            // + Propagate Empty Relations：AQEPropagateEmptyRelation、ConvertToLocalRelation、UpdateAttributeNullability
            // + Dynamic Join Selection：DynamicJoinSelection
            val (newPhysicalPlan, newLogicalPlan) = reOptimize(logicalPlan)
            val origCost = costEvaluator.evaluateCost(currentPhysicalPlan)
            val newCost = costEvaluator.evaluateCost(newPhysicalPlan)
            if (newCost < origCost || (newCost == origCost && currentPhysicalPlan != newPhysicalPlan)) {
                cleanUpTempTags(newPhysicalPlan)
                currentPhysicalPlan = newPhysicalPlan
                currentLogicalPlan = newLogicalPlan
                stagesToReplace = Seq.empty[QueryStageExec]
            }
            // Now that some stages have finished, we can try creating new stages.
            // 一些阶段已经结束，可以创建新的阶段
            result = createQueryStages(currentPhysicalPlan)
        }

        // 所有子阶段都物化后，进行物理计划优化（ReuseAdaptiveSubquery、CoalesceShufflePartitions、OptimizeSkewedJoin、OptimizeLocalShuffleReader）生成最终计划
        currentPhysicalPlan = applyPhysicalRules(result.newPlan, finalStageOptimizerRules, Some((planChangeLogger, "AQE Final Query Stage Optimization")))
        isFinalPlan = true
        executionId.foreach(onUpdatePlan(_, Seq(currentPhysicalPlan)))
        currentPhysicalPlan
    }

    /** 递归调用createQueryStage来从下到上遍历物理执行计划，如果当前节点为Exchange并且其所有子阶段都已物化，则创建一个新的QueryStage（或复用已存在的）
      * CreateStageResult作为createQueryStages的返回类型，包含以下信息：
      * + 替换为QueryStageExec的新计划
      * + 所有子阶段是否全部物化
      * + 一组已经创建的QueryStage
      */
def createQueryStages(plan: SparkPlan): CreateStageResult = plan match {
    case e: Exchange =>
        // 对于Exchange节点，如果开启了Exchange复用（配置项spark.sql.exchange.reuse为true）并且已经存在相应的Stage，则直接复用Stage封装返回CreateStatgeResult
        // 否则，从下向上递归调用createQeuryStage，将Broadcast转换为BroadcastQueryStageExec、Shuffle转换为ShuffleQueryStageExec，并封装为CreateStageResult
        context.stageCache.get(e.canonicalized) match {
            case Some(existingStage) if conf.exchangeReuseEnabled =>
                val stage = reuseQueryStage(existingStage, e)
                val isMaterialized = stage.resultOption.get().isDefined
                CreateStageResult(newPlan = stage, allChildStagesMaterialized = isMaterialized, newStages = if (isMaterialized) Seq.empty else Seq(stage))

            case _ =>
                val result = createQueryStages(e.child)
                val newPlan = e.withNewChildren(Seq(result.newPlan)).asInstanceOf[Exchange]
                // 如果所有子阶段都已物化，则创建QueryStage
                if (result.allChildStagesMaterialized) {
                    var newStage = newQueryStage(newPlan)
                    // 开启Exchange复用时，如果stageCache中已存在对应Stage，则直接复用Stage，否则，更新stageCache
                    if (conf.exchangeReuseEnabled) {
                        val queryStage = context.stageCache.getOrElseUpdate(newStage.plan.canonicalized, newStage)
                        if (queryStage.ne(newStage)) {
                            newStage = reuseQueryStage(queryStage, e)
                        }
                    }
                    val isMaterialized = newStage.resultOption.get().isDefined
                    CreateStageResult(newPlan = newStage, allChildStagesMaterialized = isMaterialized, newStages = if (isMaterialized) Seq.empty else Seq(newStage))
                } else {
                    CreateStageResult(newPlan = newPlan, allChildStagesMaterialized = false, newStages = result.newStages)
                }
        }
        // 对于QueryStageExec节点，直接封装为CreateStageResult返回
        case q: QueryStageExec =>
            CreateStageResult(newPlan = q, allChildStagesMaterialized = q.resultOption.get().isDefined, newStages = Seq.empty)
        // 对于其余类型节点，对其直接子节点应用CreateStageResult
        case _ =>
            if (plan.children.isEmpty) {
                CreateStageResult(newPlan = plan, allChildStagesMaterialized = true, newStages = Seq.empty)
            } else {
                val results = plan.children.map(createQueryStages)
                CreateStageResult(newPlan = plan.withNewChildren(results.map(_.newPlan)), allChildStagesMaterialized = results.forall(_.allChildStagesMaterialized), newStages = results.flatMap(_.newStages))
            }
    }

    /** 创建QueryStage，QeuryStageExec是物理执行计划的独立子图，在继续执行物理执行计划的其他算子之前，将物化其输出，将输出的统计信息用于优化后续阶段
      * 
      * 有两种类型的QueryStageExec可以物化，用于AQE后续优化
      * + ShuffleQueryStageExec：该阶段将其输出物化为Shuffle文件，Spark启动另一个作业来执行后续阶段
      * + BroadcastQueryStageExec：该阶段将其输出物化为Driver JVM中的一个数组，Spark执行后续阶段前先广播数组
      */
    def newQueryStage(e: Exchange): QueryStageExec = {
        val optimizedPlan = applyPhysicalRules(e.child, queryStageOptimizerRules, Some((planChangeLogger, "AQE Query Stage Optimization")))
        val queryStage = e match {
            case s: ShuffleExchangeLike =>
                val newShuffle = applyPhysicalRules(s.withNewChildren(Seq(optimizedPlan)), postStageCreationRules, Some((planChangeLogger, "AQE Post Stage Creation")))
                ShuffleQueryStageExec(currentStageId, newShuffle)
            case b: BroadcastExchangeLike =>
                val newBroadcast = applyPhysicalRules(b.withNewChildren(Seq(optimizedPlan)), postStageCreationRules, Some((planChangeLogger, "AQE Post Stage Creation")))
                BroadcastQueryStageExec(currentStageId, newBroadcast)
        }
        currentStageId += 1
        setLogicalLinkForNewQueryStage(queryStage, e)
        queryStage
    }

    def reOptimize(logicalPlan: LogicalPlan): (SparkPlan, LogicalPlan) = {
        logicalPlan.invalidateStatsCache()
        val optimized = optimizer.execute(logicalPlan)
        val sparkPlan = context.session.sessionState.planner.plan(ReturnAnswer(optimized)).next()
        val newPlan = applyPhysicalRules(sparkPlan, preprocessingRules ++ queryStagePreparationRules, Some((planChangeLogger, "AQE Replanning")))
        (newPlan, optimized)
    }

    // The partitioning of the query output depends on the shuffle(s) in the final stage. If the
    // original plan contains a repartition operator, we need to preserve the specified partitioning,
    // whether or not the repartition-introduced shuffle is optimized out because of an underlying
    // shuffle of the same partitioning. Thus, we need to exclude some `CustomShuffleReaderRule`s
    // from the final stage, depending on the presence and properties of repartition operators
    def finalStageOptimizerRules: Seq[Rule[SparkPlan]] = {
        val origins = inputPlan.collect {
            case s: ShuffleExchangeLike => s.shuffleOrigin
        }
        val allRules = queryStageOptimizerRules ++ postStageCreationRules
        allRules.filter {
            case c: CustomShuffleReaderRule => origins.forall(c.supportedShuffleOrigins.contains)
            case _ => true
        }
    }

    // 新阶段执行前应用的一组物理计划优化规则，这些优化独立于阶段
    val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
        ReuseAdaptiveSubquery(context.subqueryCache),
        CoalesceShufflePartitions(context.session),
        OptimizeSkewedJoin,
        OptimizeLocalShuffleReader
    )

    // 新阶段创建后应用的一组物理计划优化规则
    val postStageCreationRules = Seq(
        ApplyColumnarRulesAndInsertTransitions(context.session.sessionState.columnarRules),
        CollapseCodegenStages()
    )

    /**
      * 对物理执行计划应用一组物理算子规则
      */
    def applyPhysicalRules(plan: SparkPlan, rules: Seq[Rule[SparkPlan]], loggerAndBatchName: Option[(PlanChangeLogger[SparkPlan], String)] = None): SparkPlan = {
        if (loggerAndBatchName.isEmpty) {
            rules.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
        } else {
            val (logger, batchName) = loggerAndBatchName.get
            val newPlan = rules.foldLeft(plan) { case (sp, rule) =>
                val result = rule.apply(sp)
                logger.logRule(rule.ruleName, sp, result)
                result
            }
            newPlan
        }
    }
}
```

