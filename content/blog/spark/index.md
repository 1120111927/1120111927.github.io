---
title: Spark
date: "2021-04-19"
description: 
tags: RDD, DataSet, Dataframe
---

## RDD

RDD（Resilient Distributed Datasets，弹性分布式数据集）是对于数据模型的抽象，用于囊括所有内存中和磁盘中的分布式数据实体。

RDD具有5大特性：优先位置列表（preferredLocations）、分区列表（partitions）、依赖列表（dependencies）、计算函数（compute）、分区函数（partitioner）。

```scala

// 获取指定分区的优先位置列表，需要考虑该RDD是否checkpointed
def preferredLocations(split: Partition): Seq[String] = {
    checkpointRDD.map(_.getPreferredLocations(split)).getOrElse {
        getPreferredLocations(split)
    }
}

// 获取该RDD的分区列表，需要考虑RDD是否checkpointed
def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
        if (partitions_ == null) {
            stateLock.synchronized {
                if (partitions_ == null) {
                    partitions_ = getPartitions
                    partitions_.zipWithIndex.foreach { case (partition, index) =>
                        require(partition.index == index, s"partitions($index).partition == ${partition.index}, but it should equal $index")
                    }
                }
            }
        }
        partitions_
    }
}

// 子类实现，返回该RDD的分区列表，仅被调用一次，列表中的分区必须满足：`rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
def getPartitions: Array[Partition]

// 获取该RDD的依赖列表，需要考虑RDD是否checkpointed
def dependencies: Seq[Dependency[_]] = {
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
        if (dependencies_ == null) {
            stateLock.synchronized {
                if (dependencies_ == null) {
                    dependencies_ = getDependencies
                }
            }
        }
        dependencies_
    }
}

// 子类实现，返回该RDD如何依赖父RDD，仅被调用一次
def getDependencies: Seq[Dependency[_]] = deps

// 子类实现，用来计算指定分区
def compute(split: Partition, context: TaskContext): Iterator[T]

// 子类可选实现，用来指定分区方式
val partitioner: Option[Partitioner] = None
```

preferredLocations：在Spark形成任务有向无环图（DAG）时，会尽可能地把计算分配到靠近数据的位置，减少数据网络传输。RDD产生的时候存在优先位置，如HadoopRDD分区的优先位置就是HDFS块所在的节点；当RDD分区被缓存，则计算应该发送到缓存分区所在的节点进行，再不然回溯RDD的血统一直找到具有优先位置属性的父RDD，并据此决定子RDD的位置。

partitions：RDD划分成很多分区分布到集群的节点中，分区的多少涉及对这个RDD进行并行计算的粒度。分区是一个逻辑概念，变换前后的新旧分区在物理上可能是同一块内存或存储，这种优化防止函数式不变性导致的内存需求无限扩张。如果没有指定将使用默认值，默认值是该程序分配到的CPU核数，如果是从HDFS文件创建，默认为文件的数据块数。

dependencies记录了RDD的父依赖（或父RDD）。在RDD中存在两种依赖：窄依赖（Narrow Dependencies）和宽依赖（Wide Dependencies）。窄依赖是指每个父RDD的分区都至多被一个子RDD的分区使用，而宽依赖是多个子RDD的分区依赖一个父RDD的分区。如 map操作是一种窄依赖，join操作是一种宽依赖（除非父RDD已经基于Hash策略被划分过了）。窄依赖允许在单个集群节点上流水线式执行，宽依赖需要所有的父RDD数据可用，并且数据已经通过Shuffle操作；在窄依赖中，节点失败后的恢复更加高效，只需重新计算丢失的父级分区，并且这些丢失的父级分区可以并行地在不同节点上重新计算，在宽依赖的继承关系中，单个失败的节点可能导致一个RDD地所有先祖RDD中的一些分区丢失，导致计算重新执行。

compute封装了从父RDD到当前RDD转换的计算逻辑。Spark中RDD计算是以分区为单位地，而且计算函数都是在对迭代器复合，不需要保存每次计算的结果。

partitioner定义了划分数据分片的逻辑，分区划分对于Shuffle类操作很关键，决定了该操作地父RDD和子RDD之间的依赖类型，如join操作，如果协同划分的话，两个父RDD之间、父RDD与子RDD之间能形成一致地分区安排，即同一个Key保证被映射到同一个分区，这样就能形成窄依赖，反之，如果没有协同划分，导致宽依赖，协同划分是指定分区划分器以产生前后一致地分区安排。Spark默认提供两种分区函数：哈希分区函数（HashPartitioner）和范围分区函数（RangePartitioner），且Partitioner只存在于键值对RDD中，对于非键值对RDD，其值为None。

**MapPartitionsRDD**对父RDD的每个分区应用指定函数的RDD。

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

### 操作

Spark中的操作大致可以分为四类操作：

+ 创建操作（creation operation）：用于RDD创建工作。RDD创建只有两种方法，一种是来自于内存集合和外部存储系统，一种是通过转换操作生成的RDD
+ 转换操作（transformation operation）：将RDD通过一定的操作变换成新的RDD，RDD转换操作是惰性操作，只是定义了一个新的RDD，并没有立即执行
+ 控制操作（control operation）：进行RDD持久化，可以让RDD按不同的存储策略保存在磁盘或者内存中
+ 行动操作（action operation）：能够触发Spark运行的操作。Spark中行动操作分为两类，一类的操作结果变成Scala集合或者变量，另一类将RDD保存到外部文件系统或者数据库中

#### 创建操作

#### 转换操作

#### 控制操作

#### 行动操作

## 作业执行原理

Spark作业调度主要是指基于RDD的一系列操作构成一个作业，然后在Executor中执行。

应用程序（Application）：是指用户编写的Spark应用程序，包含驱动程序（Driver）和分布在集群中多个节点上运行的Executor代码，在执行过程中由一个或多个作业组成

驱动程序（Driver）：运行上述Application的main函数并且创建SparkContext，创建SparkContext的目的是为了准备Spark应用程序的运行环境。在Spark中由SparkContext负责与ClusterManager通信，进行资源的申请、任务的分配和监控等。通常用SparkContext代表Driver

集群资源管理器（Cluster Manager）：是指在集群上获取资源的外部服务，目前有以下几种

+ Standalone：Spark原生的资源管理，由Master负责资源的管理
+ Hadoop Yarn：由YARN中的ResourceManager负责资源的管理
+ Mesos：由Mesos中的Mesos Master负责资源的管理

工作节点（Worker）：集群中任何可以运行Application代码的节点。在Standalone模式中指的就是通过Slave文件配置的Worker节点，在Spark on Yarn模式中指的就是NodeManager节点

总控进程（Master）：负责管理和分配集群资源来运行Spark Application

执行进程（Executor）：Application运行在Worker节点上的一个进程，该进程负责运行Task，并负责将数据存在内存或者磁盘上，每个Application都有各自独立的一批Executor。在Spark on Yarn模式下，其进程名称为CoarseGrainedExecutorBackend[^ 类似于Hadoop MapReduce中的YarnChild]。一个CoarseGrainedExecutorBackend进程有且仅有一个executor对象，负责将Task包装成taskRunner，并从线程池中抽取出一个空闲线程运行Task。每个CoarseGrainedExecutorBackend能并行运行Task的数量取决于分配给它的CPU的个数

作业（Job）：RDD中由行动操作所生成的一个或多个调度阶段

调度阶段（Stage）：每个作业会因为RDD之间的依赖关系拆分成多组任务集合，称为调度阶段，也叫做任务集（TaskSet）。调度阶段的划分是由DAGScheduler来划分的，调度阶段有Shuffle Map Stage和Result Stage两种

任务（Task）：分发到Executor上的工作任务，是Spark实际执行应用的最小单元

DAGScheduler：是面向调度阶段的调度器，负责任务的逻辑调度，接收Spark应用提交的作业，根据RDD的依赖关系划分成不同阶段的具有依赖关系的调度阶段，并提交调度阶段给TaskScheduler。另外，DAGScheduler记录了哪些RDD被存入磁盘等物化动作，寻求任务的最优化调度[^ 如数据本地性]，监控运行调度阶段过程，如果某个调度阶段运行失败，则需要重新提交该调度阶段

TaskScheduler：是面向任务的调度器，负责具体任务的调度执行，接收DAGScheduler提交过来的任务集，然后以把任务分发到Work节点运行，由Worker节点的Executor来运行该任务

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

## 存储原理

Spark的存储采取了主从模式，即Master/Slave模式，使用了RPC的消息通信方式。Master负责整个应用程序运行期间的数据块元数据的管理和维护，Slave一方面负责将本地数据块的状态信息上报给Master，另一方面接收从Master传过来的执行命令，如获取数据块状态、删除RDD、数据块等命令。每个Slave存在数据传输通道，根据需要在Slave之间进行远程数据的读取和写入。

### 存储级别

Spark存储介质包括内存和磁盘，RDD的数据集不仅可以存储在内存中，还可以使用`persist()`或`cache()`方法显式地将RDD的数据集缓存到内存或者磁盘中。

`persist()`方法实现：

```scala
private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // 如果RDD指定了非NONE的存储级别，该存储级别将不能修改
    if (storageLevel != StorageLevel.None && newLevel != storageLevel && !allowOverride) {
        throw new UnsupportedOperationException("Cannot change storage level of an RDD after it was already assigned a level")
    }

    // 当RDD原来的存储级别为NONE时，可以对RDD进行持久化处理，在处理前需要先清除SparkContext中原来RDD相关存储元数据，然后加入该RDD持久化信息
    if (storageLevel == StorageLevel.NONE) {
        sc.cleaner.foreach(_.registerRDDForCleanup(this))
        sc.persistRDD(this)
    }

    // 当RDD原来的存储级别为NONE时，把RDD存储级别修改为传入新值
    storageLevel = newLevel
    this
}
```

当RDD第一次被计算时，persist方法会根据参数StorageLevel的设置采取特定的缓存策略，当RDD原本存储级别为NONE或者新传递进来的存储级别值与原来的存储级别相等时才进行操作，由于persist操作是控制操作的一种，只是改变了原RDD的元数据信息，并没有进行数据的存储操作，真正进行是在RDD的iterator方法中。cache方法只是persist参数为MEMORY_ONLY的情况。

StorageLevel类中，根据useDisk、useMemory、useOffHeap、deserialized、replication 5个参数的组合，Spark提供了12中存储级别的缓存策略，可以将RDD持久化到内存、磁盘和外部存储系统，或者是以序列化的方式持久化到内存中，甚至在集群的不同节点之间存储多份副本。

```scala
class StorageLevel private(
    private var _useDisk: Boolean,
    private var _useMemory: Boolean,
    private var _useOffHeap: Boolean,
    private var _deserialized: Boolean,
    private var _replication: Int = 1
)

object StorageLevel {
    val NONE = new StorageLevel(false, false, false)
    val DISK_ONLY = new StorageLevel(true, false, false, false)
    val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
    val MEMORY_ONLY = new StorageLevel(false, true, false, true)
    val MEMORY_ONLY_2 = new StorageLevel(false, true, flase, true, 2)
    val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
    val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
    val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
    val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
    val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
    val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
    val OFF_HEAP = new StorageLevel(false, false, true, false)
}
```

|存储级别|描述|
|---|---|
|NONE|不进行数据存储|
|MEMORY_ONLY|将RDD作为反序列化的对象存储在JVM中，如果RDD不能被内存装下，一些分区将不会被缓存，并且在需要的时候重新计算，是默认级别|
|MEMORY_AND_DISK|将RDD作为反序列化的对象存储在JVM中。如果RDD不能被内存装下，超出的分区将保存在硬盘上，并且在需要时被读取|
|MEMORY_ONLY_SER|将RDD作为序列化的对象进行存储（每一分区占用一个字节数组）|
|MEMORY_AND_DISK_SER|与MEMORY_ONLY_SER相似，但是把超出内存的分区存储在硬盘上，而不是在每次需要的时候重新计算|
|DISK_ONLY|只将RDD分区存储在硬盘上|
|XXX_2|与上述存储级别一样，但是将每一个分区都复制到两个集群节点上|
|OFF_HEAP|将RDD存储到分布式内存文件系统中|


