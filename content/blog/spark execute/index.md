---
title: Spark执行原理
date: "2021-04-19"
description: Spark应用执行时，先建立DAG型的逻辑处理流程（Logical plan），然后根据数据依赖关系将逻辑处理流程转化为物理执行计划（Physical plan），最后Spark将任务调度到Executor执行
tags: 逻辑处理流程，物理执行计划，数据依赖关系
---

## 系统架构

Spark采用Master-Worker结构：

**Cluster Manager** 集群资源管理器，是指在集群上获取资源的外部服务，目前有以下几种

+ Standalone：Spark原生的资源管理，由Master负责资源的管理
+ Hadoop Yarn：由YARN中的ResourceManager负责资源的管理
+ Mesos：由Mesos中的Mesos Master负责资源的管理

**Mater** 总控节点，Master节点上常驻Master进程，负责管理全部Worker节点，如将Spark任务分配给Worker节点，收集Worker节点上任务的运行信息，监控Worker节点的存活状态等

**Worker** 工作节点，集群中任何可以运行Spark Application代码的节点，在Standalone模式中指的就是通过Slave文件配置的Worker节点，在Spark on Yarn模式中指的就是NodeManager节点。Worker节点上常驻Worker进程，负责启停和观察任务的执行情况，如与Master节点通信，启动Executor来执行具体的Spark任务，监控任务运行状态。

**Spark Application** Spark应用，指用户编写的Spark应用程序，包含配置参数、数据处理流程等，在执行过程中由一个或多个作业组成。

**Spark Driver** Spark驱动程序，指运行Spark应用中的main()函数并且创建SparkContext的进程。创建SparkContext的目的是为了准备Spark应用程序的运行环境。在Spark中由SparkContext负责与ClusterManager通信，进行资源的申请、任务的分配和监控等。通常用SparkContext代表Driver。Driver独立于Master进程。

**Executor** Spark执行进程，是Spark计算资源的一个单位，Spark先以Executor为单位占用集群资源，然后可以将具体的计算任务分配给Executor执行。Executor在物理上是一个JVM进程，可以运行多个线程（计算任务）。在Spark on Yarn模式下，其进程名称为CoarseGrainedExecutorBackend[^ 类似于Hadoop MapReduce中的YarnChild]，一个CoarseGrainedExecutorBackend进程有且仅有一个executor对象，负责将Task包装成taskRunner，并从线程池中抽取出一个空闲线程运行Task，每个CoarseGrainedExecutorBackend能并行运行Task的数量取决于分配给它的CPU的个数。Executor内存大小由用户配置，而且Executor的内存空间由多个Task共享。

**Task** Spark应用的计算任务，是Spark中最小的计算单位，以线程方式^[为了数据共享和提高执行效率，Spark采用了以线程为最小的执行单位，缺点是线程间会有资源竞争]运行在Executor进程中，执行具体的计算任务。

## 作业执行原理

Spark应用执行时，先建立DAG型的逻辑处理流程（Logical plan）^[toDebugString()方法用于输出逻辑处理流程]，逻辑处理流程只是表示输入/输出、中间数据，以及它们之间的依赖关系，并不涉及具体的计算任务。然后根据数据依赖关系将逻辑处理流程转化为物理执行计划（Physical plan），包括执行阶段（Stage）和执行任务（Task），物理执行计划中包含具体的计算任务（task），最后Spark将任务调度到Executor执行，在同一个阶段的任务可以并行执行，在同一个任务的操作可以进行串行、流水线式的处理。

作业（Job）：RDD中由行动操作所生成的一个或多个调度阶段

调度阶段（Stage）：每个作业会因为RDD之间的依赖关系拆分成多组任务集合，称为调度阶段，也叫做任务集（TaskSet）。调度阶段的划分是由DAGScheduler来划分的，调度阶段有Shuffle Map Stage和Result Stage两种

任务（Task）：分发到Executor上的工作任务，是Spark实际执行应用的最小单元

DAGScheduler：是面向调度阶段的调度器，负责任务的逻辑调度，接收Spark应用提交的作业，根据RDD的依赖关系划分成不同阶段的具有依赖关系的调度阶段，并提交调度阶段给TaskScheduler。另外，DAGScheduler记录了哪些RDD被存入磁盘等物化动作，寻求任务的最优化调度[^ 如数据本地性]，监控运行调度阶段过程，如果某个调度阶段运行失败，则需要重新提交该调度阶段

TaskScheduler：是面向任务的调度器，负责具体任务的调度执行，接收DAGScheduler提交过来的任务集，然后以把任务分发到Work节点运行，由Worker节点的Executor来运行该任务

### 逻辑处理流程

  将Spark应用程序转化为逻辑处理流程（及RDD及其之间的数据依赖关系），需要解决一下3个问题：

1. 根据应用程序如何产生RDD，产生什么样的RDD？
2. 如何建立RDD之间的数据依赖关系？
3. 如何计算RDD中的数据？

**根据应用程序如何产生RDD，产生什么样的RDD？** Spark用转换方法（transformation()）返回（创建）一个新的RDD^[只适用于逻辑比较简单的转换操作，如map()，对于join()、distinct()等复杂的转换操作，Spark依据其子操作的顺序，将生成的多个RDD串联在一起，只返回给用户最后生成的RDD]，并且使用不同类型的RDD（如ParallelCollectionRDD，MapPartitionsRDD，ShuffledRDD等）来表示不同的数据类型、不同的计算逻辑，以及不同的数据依赖。

**如何建立RDD之间的数据依赖关系** RDD之间的数据依赖关系包括两方面：一是RDD之间的依赖关系；一是RDD自身分区之间的关联关系。建立RDD之间的数据依赖关系，需要解决以下3个问题：

1. 如何建立RDD之间的数据依赖关系？对于一元操作，如 rdd2 = rdd1.transformation()，关联关系是 `rdd1 => rdd2`；对于二元操作，如rdd3 = rdd1.join(rdd2)，关联关系是`(rdd1, rdd2) => rdd3`，依此类推。
2. 新生成的RDD应该包含多少个分区？新生成RDD的分区个数由用户和parentRDD共同决定，对于join()等转换操作，可以指定生成分区的个数，如果不指定，则取其parentRDD分区个数的最大值；对于map()等转换操作，其生成RDD的分区个数与数据源的分区个数相同。
3. 新生成的RDD与其parentRDD中的分区间是什么依赖关系？是依赖parentRDD中的一个分区还是多个分区？分区之间的依赖关系基于转换操作的语义有关，也与RDD分区个数有关。根据child RDD的各个分区是否完全依赖parent RDD的一个或者多个分区，Spark将常见数据操作的数据依赖关系分为两大类：窄依赖（Narrow Dependency）和宽依赖（Shuffle Dependency）。

窄依赖：如果新生成的child RDD中每个分区都依赖parent RDD中的一部分分区，那么这个分区依赖关系被称为NarrowDependency。

窄依赖可以进一步细分为4中依赖^[Spark代码中没有ManyToOneDependency和ManyToManyDependency，统称为NarrowDependency]：

+ 一对一依赖（OneToOneDependency）：表示child RDD和parent RDD中分区个数相同，并存在一一映射关系
+ 区域依赖（RangeDependency）：表示child RDD和parent RDD的分区经过区域化后存在一一映射关系
+ 多对一依赖（ManyToOneDependency）：表示child RDD中的一个分区同时依赖多个parent RDD中的分区
+ 多对多依赖（ManyToManyDependency）：表示child RDD中的一个分区依赖parent RDD中的多个分区，同时parent RDD中的一个分区被child RDD中的多个分区依赖

宽依赖：如果新生成的child RDD中的分区依赖parent RDD中的每个分区的一部分，那么这个分区依赖关系被称为ShuffleDependency。

窄依赖、宽依赖的区别是child RDD的各个分区是否完全依赖parent RDD的一个或者多个分区。对于窄依赖，parent RDD中的一个或者多个分区中的数据需要全部流入child RDD的某一个或者多个分区；对于宽依赖，parent RDD分区中的数据需要一部分流入child RDD的某一个分区，另外一部分流入child RDD的另外分区。

对数据依赖进行分类有以下用途：一是明确RDD分区之间的数据依赖关系；二是对数据依赖进行分类有利于生成物理执行计划，窄依赖在执行时可以在同一个阶段进行流水线（pipeline）操作，而宽依赖则需要进行Shuffle；三是有利于代码实现，OneToOneDependency可以采用一种实现方式，ShuffleDenpendency采用另外一种实现方式。

**如何计算RDD中的数据** 根据数据依赖关系，对child RDD中每个分区的输入数据应用`transformation(func)`处理，并将生成的数据推送到child RDD中对应的分区即可。不同的转换操作有不同的计算方式（控制流），如`map(func)`每读入一个record就进行处理，然后输出一个record，recuceByKey(func)操作对中间结果和下一个record进行聚合计算并输出结果，mapPartitions()操作可以对分区中的数据进行多次操作后再输出结果。

作业和任务调度流程：

1. 提交作业。Spark应用程序进行各种转换操作，通过行动操作触发作业运行，提交作业后根据RDD之间的依赖关系构建DAG图，DAG图提交给DAGScheduler进行解析。行动操作内部调用SparkContext的`runJob()`方法来提交作业，SparkContext的`runJob()`方法经过几次调用后进入DAGScheduler的runJob方法。在DAGScheduler类内部会进行一系列的方法调用，首先是在`runJob()`方法里，调用`submitJob()`方法来继续提交作业，这里会发生阻塞，直到返回作业完成或失败的结果；然后在`submitJob()`方法里，创建一个JobWaiter对象，并借助内部消息处理进行把这个对象发送给DAGScheduler的内嵌类DAGSchedulerEventProcessLoop进行处理；最后在DAGSchedulerEventProcessLoop消息接收方法`OnReceive()`中，接收到JobSubmitted样例类完成模式匹配后，继续调用DAGScheduler的handleJobSubmitted方法来提交作业，在该方法中划分阶段。
2. 划分调度阶段。DAGScheduler从最后一个RDD出发使用广度优先遍历整个依赖树，把DAG拆分成相互依赖的调度阶段，拆分调度阶段是依据RDD的依赖是否为宽依赖，当遇到宽依赖就划分为新的调度阶段[^ 当某个RDD的操作是Shuffle时，以该Shuffle操作为界限划分成前后两个调度阶段]。每个调度阶段包含一个或多个任务，这些任务形成任务集，提交给底层调度器TaskScheduler进行调度运行。在SparkContext中提交运行时，会调用DAGScheduler的`handleJobSubmitted()`方法进行处理，在该方法中会先找到最后一个finalRDD，并调用`getParentStage()`方法，`getParentStage()`方法判断指定RDD的祖先RDD是否存在Shuffle操作，如果没有存在shuffle操作，则本次作业仅有一个ResultStage；如果存在Shuffle操作，则需要从发生shuffle操作的RDD往前遍历，找出所有ShuffleMapStage，在`getAncestorShuffleDependencies()`方法中实现，`getAncestorShuffleDependencies()`方法找出所有操作类型是宽依赖的RDD，然后通过`registerShuffleDependencies()`和`newOrUsedShuffleStage()`两个方法划分所有ShuffleMapStage，最后生成finalRDD的ResultStage。当所有调度阶段划分完毕时，这些调度阶段建立起依赖关系，依赖关系是通过调度阶段属性parents\[Stage\]定义，通过该属性可以获取当前阶段所有祖先阶段，可以根据这些信息按顺序提交调度阶段进行。
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

## Shuffle

在Spark中Shuffle分为写和读两种操作，而Shuffle写操作分为基于Hash的Shuffle写操作和基于排序的Shuffle写操作。

Spark中Shuffle实现方式的演化历程，经历了Hash、Sort和Tungsten-Sort三个重要阶段：

+ 在1.1之前的版本中，采用基于Hash方式的Shuffle实现，从1.1版本开始Spark参考MapReduce的实现，引入了Sort Shuffle机制，自此Hash Shuffle与Sort Shuffle共同存在，并在1.2版本时将Sort Shuffle设置为默认的Shuffle方式
+ 1.4版本时，Tungsten引入了UnSafe Shuffle机制来优化内存和CPU的使用，并在1.6版本中进行代码优化，统一实现到Sort Shuffle中，通过Shuffle管理器自动选择最佳Shuffle方式
+ 到2.0版本时，Hash Shuffle的实现从Spark中删除了，所有Shuffle方式全部统一到Sort Shuffle一个实现中

```bob-svg
                                           ,----------------,
,------------,      ,--------------,       |Hash Shuffle    |
|Hash Shuffle|----->|Hash Shuffle  |------>|& Sort Shuffle  |
`------------`      |& Sort Shuffle|       |& Unsafe Shuffle|
                    `--------------`       `-------+--------`
                                                   |
 +-------------------------------------------------+
 |      ,-------------------------,          ,-----------------------,
 +----->|Hash Shuffle             |--------->|(Tungsten) Sort Shuffle|
        |& (Tungsten) Sort Shuffle|          `-----------------------`
        `-------------------------`
```

### 基于Hash的Shuffle写操作

主要思想是按照Hash的方式在每个ShuffleMapTask中为每个ShuffleReduceTask生成一个文件。

每一个ShuffleMapTask会根据ShuffleReduceTask的数量创建出相应的桶（bucket）^[这里的桶是一个抽象概念，每一个桶对应一个文件]，桶的数量是M*R（M是ShuffleMapTask的个数，R是ShuffleReduceTask的个数），ShuffleMapTask生成的结果会根据设置的分区（Partition）算法填充到每个桶中。当Reduce启动时，会根据任务的编号和所依赖的ShuffleMapTask的编号从远端或本地取得相应的桶作为Reduce任务的输入进行处理。

Spark假定大多数情况下Shuffle的数据排序是不必要的，因此并不在ShuffleReduceTask中做归并排序（merge sort），而是使用聚合（aggregator）。聚合实际上是一个HashMap，它以当前任务输出结果的key作为键，以任意要combine类型为值，并将shuffle读到的每一个键值对更新或插入到HashMap中。这样就不需要预先把所有的键值对进行归并排序，而是来一个处理一个，省下了外部排序这个步骤。

基于Hash的Shuffle实现在ShuffleMapTask和ShuffleReduceTask数量较大时会导致写文件数量大和缓存开销过大的问题。

基于hash的shuffle写操作：

1. 在ShuffleMapTask的runTask方法中，ShuffleManager调用getWriter方法得到ShuffleWriter对象（对于基于Hash的Shuffle实现对应HashShuffleWriter），通过HashShuffleWriter的writer方法把RDD的计算结果持久化，持久化完毕后，将元数据信息写入到MapStatus中，后续的任务可以通过该MapStatus得到处理结果信息

   ```scala
   override def runTask(context: TaskContext): MapStatus = {
       // ...
       var writer: ShuffleWriter[Any, Any] = null
       try {
           // 从SparkEnv获取ShuffleManager，在系统启动时，会根据设置初始化
           val manager = SparkEnv.get.shuffleManager
           writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)

           // 调用RDD进行计算，通过HashShuffleWriter的writer方法把RDD的计算结果持久化
           writer.write(rdd.iterator(partition, context)).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
           writer.stop(success = true).get
       } catch { // ...}
   }
   ```

2. 在HashShuffleWriter的write方法中，通过ShuffleDependency是否定义了Aggregator判断是否需要在Map端对数据进行聚合操作，如果需要则对数据进行聚合操作。然后调用ShuffleWriterGroup的writer方法得到一个DiskBlockObjectWriter对象，调用该对象的writer方法写入

   ```scala
   // 写入前定义blockManager和shuffle
   private val blockManager = SparkEnv.get.blockManager
   private val shuffle = shuffleBlockResolver.forMapTask(dep.shuffleId, mapId, numOutputSplits, dep.serializer, writerMetrics)

   override def write(records: Iterator[Product2[K, V]]): Unit = {
       // 判断是否需要聚合，如果需要，则对数据按照键值进行聚合
       val iter = if (dep.aggregator.isDefined) {
           if (dep.mapSideCombine) {
               dep.aggregator.get.combineValuesByKey(records, context)
           } else {
               records
           }
       } else {
           records
       }

       // 对原始结果或者是聚合后的结果调用ShuffleWriterGroup的writers方法得到一个DiskBlockObjectWriter对象，调用该对象的writer方法写入
       for (elem <- iter) {
           val bucketId = dep.partitioner.getPartition(elem._1)
           shuffle.writers(bucketId).write(elem._1, elem._2)
       }
   }
   ```

3. 在步骤2中生成writer中定义了shuffle，该shuffle在FileShuffleBlockResolver由forMapTask方法定义，需要注意的是，writers是通过ShuffleDependency来获取分区的数量，这个数量和后续任务数相对应，这样在运行的时候，每个分区对应一个任务，从而形成流水线高效并发地处理数据

   ```scala
   private val shuffleStates = new ConcurrentHashMap[ShuffleId, ShuffleState]
   def forMapTask(shuffleId: Int, mapId: Int, numReducers: Int, serializer: Serializer, writeMetrics: ShuffleWriteMetrics): ShuffleWriterGroup = {
       new ShuffleWriterGroup {
           // 在这里使用Java的ConcurrentHashMap，而不使用Scala的Map并使用getOrElseUpdate方法，是因为其不是原子操作
           private val shuffleState: ShuffleState = {
               shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numReducers))
               shuffleStates.get(shuffleId)
           }
           val openStartTime = System.nanoTime

           // 获取序列化实例，该实例在SparkEnv实例化，由spark.serializer进行配置
           val serializerInstance = serializer.newInstance()
           val writers: Array[DiskBlockObjectWriter] = {
               Array.tabulate[DiskBlockObjectWriter](numReducers) { bucketId =>
                   // 获取数据块编号，规则为"shuffle_"+shuffleId+"_"+mapId+"_"+reduceId
                   val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
                   // 在DiskBlockManager.getFile方法中，根据该编号获取该数据文件
                   val blockFile = blockManager.diskBlockManager.getFile(blockId)
                   
                   // 返回临时文件路径，该路径为：blockFile绝对路径+UUID，然后根据该临时文件路径生成磁盘写入器实例writers
                   val tmp = Utils.tempFileWith(blockFile)
                   blockManager.getDiskWriter(blockId, tmp, serializerInstance, bufferSize, writeMetrics)
               }
           }
           writeMetrics.incWriteTime(System.nanoTime - openStartTime)

           // 释放写入器实例方法
           override def releaseWriters(success: Boolean) {
               shuffleState.completeMapTasks.add(mapId)
           }
       }
   }
   ```

### 基于排序的Shuffle写操作

每一个ShuffleMapTask文件会将所有的结果写到同一个文件中，对应的生成一个Index文件进行索引。

1. SortShuffleWriter的write方法，先判断ShuffleMapTask输出结果在Map端是否需要合并（Combine），如果需要合并，则外部排序中进行聚合并排序；如果不需要，则外部排序中不进行聚合和排序。确认外部排序方式后，在外部排序中将使用PartitionedAppendOnlyMap来存放数据，当排序中的Map占用的内存已经超越了使用的阈值，则将Map中的内容溢写到磁盘中，每一次溢写产生一个不同的文件，当所有数据处理完毕后，在外部排序中有可能一部分计算结果在内存中，另一部分计算结果溢写到一或多个文件之中，这是通过merge操作将内存和溢写文件中的内容合并整到一个文件里

   ```scala
   override def write(records: Iterator[Product2[K, V]]): Unit = {
       // 获取ShuffleMapTask输出结果的排序方式
       sorter = if (dep.mapSideCombine) {
           // 当输出结果需要合并，那么外部排序算法中进行聚合
           require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
           new ExternalSorter[K, V, C](dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
       } else {
           new ExternalSorter[K, V, V](aggregator=None, Some(dep.partitioner), ordering=None, dep.serializer)
       }

       // 根据获取的排序方式，对数据进行排序并写入到内存缓冲区中，如果排序中的Map占用的内存已经超越了使用的阈值，则将Map中的内容溢写到磁盘中，每一次溢写产生一个不同的文件
       sorter.insertAll(records)

       // 通过Shuffle编号和Map编号获取该数据文件
       val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
       val tmp = Utils.tempFileWith(output)

       // 通过Shuffle编号和Map编号获取该数据文件
       val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
       val tmp = Utils.tempFileWith(output)

       // 通过Shuffle编号和Map编号获取ShuffleBlock编号
       val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)

       // 在外部排序中有可能一部分计算结果在内存中，另一部分计算结果溢写到一个或多个文件之中，通过merge操作将内存和spill文件中的内容合并整到一个文件里
       val partitionLengths = sorter.writePartitionedFile(blockId, tmp)

       // 创建索引文件，将每个partition在数据文件中的起始位置和结束位置写入到索引文件
       shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
       // 将元数据信息写入到MapStatus中，ShuffleReduceTask任务可以通过该MapStatus得到处理结果信息
       mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
   }
   ```

2. 在ExternalSorter的insertAll方法中，先判断是否需要进行聚合（Aggregation），如果需要，则根据键值进行合并（Combine），然后把这些数据写入到内存缓冲区中，如果排序中的Map占用的内存已经超越了使用的阈值，则将Map中的内容溢写到磁盘中，每一次溢写产生一个不同的文件。如果不需要聚合，则直接把数据写入到内存缓冲区

   ```scala
   override def insertAll(records: Iterator[Product2[K, V]]): Unit = {
       // 获取外部排序中是否需要进行聚合
       val shouldCombine = aggregator.isDefined
       if (shouldCombine) {
           // 如果需要聚合，则使用AppendOnlyMap根据键值进行合并
           val mergeValue = aggregator.get.mergeValue
           val createCombiner = aggregator.get.createCombiner
           var kv: Product2[K, V] = null
           val update = (hadValue: Boolean, oldValue: C) => {
               if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
           }
           while (records.hasNext) {
               addElementsRead()
               kv = records.next()
               map.changeValue((getPartition(kv._1), kv._1), update)
               // 对数据进行排序并写入到内存缓冲区中，如果排序中的Map占用的内存已经超越了使用的阈值，则将Map中的内容溢写到磁盘中，每一次溢写产生一个不同的文件
               maybeSpillCollection(usingMap = true)
           }
       } else {
           // 不需要进行聚合，对数据进行排序并写入到内存缓冲区中
           while (records.hasNext) {
               addElementsRead()
               val kv = records.next()
               buffer.insert(getParition(kv._1), kv._1, kv._2.asInstanceOf[C])
               maybeSpillCollection(usingMap = false)
           }
       }
   }
   ```

### shuffle的读操作

1. Shuffle读的起始点是由ShuffledRDD#compute方法发起的，在该方法中会调用ShuffleManager的getReader方法^[在SparkEnv启动时，会对ShuffleManager、BlockManager和MapOutputTracker等实例化，ShuffleManager配置项有HashShuffleManager、SortShuffleManager和自定义的ShuffleManager等3种选项，前两种在Shuffle中均实例化为BlockStoreShuffleReader，但是在HashShuffleManager中持有的是FileShuffleBlockResolver实例，SortShuffleManager中持有的是IndexShuffleBlockResolver实例]

   ```scala
   override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
       val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
       // 根据配置ShuffleReader，基于哈希和排序的Shuffle读都使用HashShuffleReader
       SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context).read().asInstanceOf[Iterator[(K, C)]]
   }
   ```

2. 通过请求Driver端的MapOutputTrackerMaster得到上游Shuffle结果的位置信息。在BlockStoreShuffleReader的read方法中，调用MapOutputTracker的getMapSizesByExecutorId方法，由Executor的MapOutputTracker发送获取结果状态的GetMapOutputStatuses消息给Driver端的MapOutputTrackerMaster，请求获取上游Shuffle输出结果对应的MapStatus，在该MapStatus存放了结果数据的位置信息。具体实现为：在Hash
3. 对Shuffle结果的位置进行筛选，判断当前运行的数据是从本地还是从远程节点获取。如果是本地获取，直接调用BlockManager的getBlockData方法，在读取数据的时候会根据写入方式采取不同ShuffleBlockResolver读取；如果是在远程节点上，需要通过Netty网络方式读取数据。在远程读取的过程中使用多线程的方式进行读取^[一般来说，会启动5个线程到5个节点进行读取所有，每次请求的数据大小不会超过系统设置的1/5，该大小由spark.reducer.maxSizeInFlight配置项进行设置，默认情况下该配置为48MB]
4. 读取数据后，判断ShuffleDependency是否定义聚合，如果需要，则根据键值进行聚合。如果在上游ShuffleMapTask已经做了合并，则在合并数据的基础上做键值聚合。待数据处理完毕后，使用外部排序（ExternalSorter）对数据进行排序并放入存储中，至此完成Shuffle读数据操作