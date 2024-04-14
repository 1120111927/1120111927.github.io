---
title: Flink资源管理
date: "2023-01-11"
description: Flink使用TaskSlot对自身资源进行精细化管理，算子链将尽可能多的算子链在一起作为一个Task执行来提高分布式执行效率，Slot共享机制尽可能多地让多个SubTask共享一个TaskSlot来提高资源利用率。
tags: TaskSlot、算子链（OperatorChain）、Slot共享机制（SlotSharingGroup）
---

```toc
ordered: true
class-name: "table-of-contents"
```

Flink资源分为两级：集群资源和Flink自身资源。集群资源由资源管理器框架来管理^[资源管理框架（Yarn、K8s、Mesos等）提供硬件资源的抽象、计算任务的隔离、资源的管理等特性]，Flink从资源管理框架申请和释放资源（使用资源管理器（ResourceManager）来解耦Flink和资源管理框架）。Flink自身资源由Flink自身通过TaskSlot进行精细管理。Flink从资源管理框架申请资源容器（Yarn的Container或K8s的Pod），1个资源容器中运行1个TaskManager进程。Flink对1个TaskManager的计算资源通过线程进行切分（当前仅隔离托管内存），每一份叫做TaskSlot。一个TaskSlot上运行一个SubTask（一个Task的一个并行子任务，一个Task对应一个算子或算子链）。

## 资源抽象：TaskSlot

TaskSlot代表了TaskManager中资源的固定子集，既是资源调度的基本单元，又是申请资源的基本单元。分配资源，即将资源Slot化，意味着不同作业的SubTask不会竞争托管内存，而是具有一定数量的保留托管内存。通过调整TaskSlot的数量，用户可以定义SubTask之间如何互相隔离。如果一个TaskManager只有一个Slot，意味着每个SubTask独立地运行在JVM中，而一个TaskManager有多个Slot，则意味着更多的SubTask可以共享一个JVM，同一JVM进程中的SubTask将共享TCP连接（通过多路复用）和心跳消息，还可以共享数据集和数据结构，减少每个SubTask开销。

## 资源管理

<div class='wrapper' markdown='block'>

![资源管理涉及组件](imgs/resource%20manage.svg)

</div>

资源管理涉及JobMaster、ResourceManager、TaskManager三种角色。JobMaster是TaskSlot使用者，向ResourceManager申请资源。ResourceManager负责分配、申请和释放资源。TaskManager是TaskSlot持有者，其Slot清单（TaskSlotTable）中记录了Slot分配情况。

资源管理器（ResourceManager）是Flink集群资源管理的抽象，位于Flink和资源管理框架之间，其主要作用如下：

+ 申请容器启动新的TaskManager，或者为作业申请Slot
+ 处理JobManager和TaskManager的异常退出
+ 缓存TaskManager，等待一段时间之后再释放掉不用的容器，避免资源反复的申请释放
+ JobManager和TaskManager的心跳感知，对JobManager和TaskManager的退出进行对应的处理

Slot管理器（SlotManager）是ResourceManger中用于管理TaskSlot的组件，其中的TaskManagerTracker维护了所有已经注册的TaskManager的所有TaskSlot的状态及分配情况，还维护了所有处于等待状态的TaskSlot请求。每当有一个新的TaskSlot注册或者一个已经分配的TaskSlot被释放的时候，SlotManager会试图去满足处于等待状态的SlotRequest，如果可用的TaskSlot不足以满足需要时，SlotManager会通过ResourceActions的allocateResource()方法告知ResourceManager，ResourceManager可能会尝试启动新的TaskManager。另外，长时间处于空闲状态的TaskManager或者长时间没有被满足的PendingSlotRequest，会触发超时机制进行处理。

### 注册和更新Slot

<details>

<summary>具体实现</summary>

```Java
class SlotManager
{
    // 注册TaskManager
    boolean registerTaskManager(
        TaskExecutorConnection taskExecutorConnection,
        SlotReport initialSlotReport,
        ResourceProfile totalResourceProfile,
        ResourceProfile defaultSlotResourceProfile)
    {
        // 通过TaskManager的instanceId标识它们
        Optional<PendingTaskManagerId> matchedPendingTaskManagerOptional =
            initialSlotReport.hasAllocatedSlot() ? Optional.empty() : findMatchingPendingTaskManager(totalResourceProfile, defaultSlotResourceProfile);

            if (!matchedPendingTaskManagerOptional.isPresent()
                    && isMaxTotalResourceExceededAfterAdding(totalResourceProfile)) {
                LOG.info(
                        "Releasing task manager {}. The max total resource limitation <{}, {}> is reached.",
                        taskExecutorConnection.getResourceID(),
                        maxTotalCpu,
                        maxTotalMem.toHumanReadableString());
                resourceActions.releaseResource(
                        taskExecutorConnection.getInstanceID(),
                        new FlinkException("The max total resource limitation is reached."));
                return false;
            }

            taskManagerTracker.addTaskManager(taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);

            if (initialSlotReport.hasAllocatedSlot()) {
                slotStatusSyncer.reportSlotStatus(
                        taskExecutorConnection.getInstanceID(), initialSlotReport);
            }

            if (matchedPendingTaskManagerOptional.isPresent()) {
                PendingTaskManagerId pendingTaskManager = matchedPendingTaskManagerOptional.get();
                allocateSlotsForRegisteredPendingTaskManager(
                        pendingTaskManager, taskExecutorConnection.getInstanceID());
                taskManagerTracker.removePendingTaskManager(pendingTaskManager);
                return true;
            }

            checkResourceRequirementsWithDelay();
            return true;
        }
    }
}
```

</details>

### 算子链

Flink通过算子链（OperatorChain）将多个算子链接在一起形成Task，减少线程之间的切换，减少消息的序列化/反序列化，减少数据在缓冲区的交换，减少延迟同时提高吞吐。

算子链接在一起需要满足以下条件：

+ 没有禁用算子链
+ 上下游算子并行度一致
+ 下游算子只有一个上游算子
+ 上下游算子都在同一个SlotGroup中
+ 下游算子的ChainingStrategy为ALWAYS（可以与上下游链接）
+ 上游算子的ChainingStrategy为ALWAYS或HEAD（只能与下游链接）
+ 两个算子间数据分区方式是forward

算子链行为可以通过DataStream API（startNewChain()和disableChaining()，底层都是通过调整Opertor的ChainingStrategy实现）指定，也可以通过StreamExecutionEnvironment的disableOperatorChaining()来全局禁用算子链。

### SlotSharingGroup

按Task对计算资源的需求不同，可以分为IO密集型、CPU密集型、内存密集型等。考虑到不同Task的计算任务对资源需求的差异，Flink在Slot的基础上，设计了Slot共享机制。默认情况下，Flink允许同一个作业的不同Task的SubTask共享Slot（可能在一个Slot中运行该作业的整个流水线），共享Slot有以下优点：

+ 资源分配简单：Flink集群需要的TaskSlot数和作业中的最高并行度一致，不需要计算一个程序总共包含多少个SubTask
+ 资源利用率高：如果没有Slot共享，非资源密集型的SubTask将阻塞和资源密集型SubTask一样多的资源，同时Slot共享可以确保繁重的SubTask在TaskManager之间公平分配

默认情况下，所有算子都属于默认的共享组default，即 默认情况下所有的算子都可以共享一个Slot。当所有上游算子具有相同的SlotSharingGroup时，该算子会继承这个共享组。为了防止不合理的共享，可以通过DataStream的slotSharingGroup()方法强制指定共享组

SlotSharingGroup用于实现Slot共享，它尽可能地让SubTask共享一个Slot。CoLocationGroup用来强制将SubTask放到同一个Slot中，CoLocationGroup主要用于迭代流中，用来保证迭代头与迭代尾的第i个SubTask能被调度到同一个TaskManager上。

## 组件



**SlotManager** 是ResourceManager的组件^[SlotManager虽然是ResourceManager的组件，但是其逻辑是通用的，并不关心到底使用了哪种资源集群]，其工作是维护TaskManager中的Slot。从全局角度维护当前有多少TaskManager、每个TaskManager有多少空闲的Slot和Slot资源的使用情况。当Flink作业调度执行时，根据Slot分配策略为Task分配执行的位置。面向不同的对象，SlotManager提供不同的功能
+ 对TaskManager提供注册、取消注册、空闲退出等管理动作
+ 对Flink作业，接收Slot的请求和释放、资源汇报等。当资源不足时，SlotManager将资源请求暂存在等待队列中，SlotManager通知ResourceManager取申请更多的资源，启动新的TaskManager,TaskManager注册到SlotManager之后，SlotManager就有可用的新资源了，从等待队列中依次分配资源

SlotManager接口有两个实现：DeclarativeSlotManager和FineGrainedSlotManager。

<details> <summary>具体实现</summary>

```Java
interface SlotManager {

    int getNumberRegisteredSlots();
    int getNumberRegisteredSlotsOf(InstanceID instanceId);
    int getNumberFreeSlots();
    int getNumberFreeSlotsOf(InstanceID instanceId);
    Map<WorkerResourceSpec, Integer> getRequiredResources();
    ResourceProfile getRegisteredResource();
    ResourceProfile getRegisteredResourceOf(InstanceID instanceID);
    ResourceProfile getFreeResource();
    ResourceProfile getFreeResourceOf(InstanceID instanceID);
    Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID);
    void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions);
    void suspend();
    void clearResourceRequirements(JobID jobId);
    void processResourceRequirements(ResourceRequirements resourceRequirements);
    boolean registerTaskManager(TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport, ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile);
    boolean unregisterTaskManager(InstanceID instanceId, Exception cause);
    boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport);
    void freeSlot(SlotID slotId, AllocationID allocationId);
    void setFailUnfulfillableRequest(boolean failUnfulfillableRequest);

}
```

</details>

**SlotProvider** 该接口定义了Slot的请求行为，最终的实现在SchedulerImpl中，支持两种请求模式
+ 立即响应模式：Slot请求会立即执行
+ 排队模式：排队等待可用Slot，当资源可用时分配资源

**SlotSelectionStrategy** 是策略定义的接口，分为两大类
+ LocationPreferenceSlotSelectionStrategy：位置优先的选择策略
    + DefaultLocationPreferenceSlotSelectionStrategy：默认策略，不考虑资源的均衡分配，会从满足条件的可用Slot集合选择第一个，以此类推
    + EvenlySpreadOutLocationPreferenceSlotSelectionStrategy：均衡策略，考虑资源的均衡分配，会从满足条件的可用Slot集合中选择剩余资源最多的Slot，尽量让各个TaskManager均衡地承担计算压力
+ PreviousAllocationSlotSelectionStrategy：已分配Slot优先的选择策略^[如果当前没有空闲的已分配Slot，则仍然会使用位置优先的策略来分配和申请Slot]

**SlotPool** 槽资源池是JobMaster中记录当前作业从TaskManager获取的Slot的集合，JobMaster的调度器首先从SlotPool中获取Slot来调度任务，SlotPool在没有足够的Slot资源执行作业的时候，首先会尝试从ResourceManager中获取资源，如果ResourceManager当前不可用、ResourceManager拒绝资源请求或请求超时，资源申请失败，则作业启动失败。JobMaster申请到资源之后，会在本地持有Slot，避免ResourceManager异常导致作业运行失败。当作业已经执行完毕或者作业完全启动且资源有剩余时，JobMaster会将剩余资源交还给ResourceManager

## Slot分配

当ResourceManager请求Slot时，会触发SlotManager的registerSlotRequest()方法。在注册Slot分配请求时，首先经过Allocation去重，检查在等待中的Slot分配请求表和已满足的Slot分配请求表中是否包含相同的AllocationId，然后创建一个等待中的Slot分配请求，并在所有空闲Slot中匹配是否有满足条件的Slot，如果有则直接分配，否则在等待注册的Slot中匹配是否有满足条件的Slot，如果依然匹配不到，则根据failUnfulfillableRequest的配置直接fail该请求或是继续让该请求排队。

Slot共享管理器（SlotSharingManager）和Slot共享组（SlotSharingGroup）在作业调度执行时发挥作用，部署Task之前，选择Slot确定Task发布到哪个TaskManager。SlotSharingmanager负责管理资源共享与分配，1个SlotSharingGroup对应1个SlotSharingManager。Flink有两种共享组：SlotSharingGroup和CoLocationGroup。SlotSharingGroup为非强制性共享约束，Slot共享根据组内的JobVerticesID查找是否已有可以共享的Slot，只要确保相同JobVertexID不能出现在同一个共享的Slot内即可。在符合资源要求的Slot中，找到没有相同JobVertexID的Slot，根据Slot选择策略选择一个Slot即可，如果没有符合条件的，则申请新的Slot。CoLcationGroup为本地约束共享组，具有强制性的Slot共享限制，CoLocationGroup用在迭代运算中，即在IterativeStream的API中调用，迭代运算中的Task必须共享同一个TaskManager的Slot。CoLacationGroup根据组内每个ExecutionVertex关联的CoLocationConstraint查找是否有相同CoLocationConstraint约束已分配Slot可用，在调用作业执行的时候，首先要找到本约束中其他Task部署的TaskManager，如果没有则申请一个新的Slot，如果有则共享该TaskManger上的Slot。

JobGraph向ExecutionGraph的转换过程中，为每一个ExecutionVertex赋予了按照并行度编写的编号，相同编号的迭代计算ExecutionVertex会被放入本地共享约束组中，共享相同的CoLocationConstraint对象，在调度的时候，根据编号就能找到本组其他Task的Slot信息。

**单独Slot资源申请** 首先会从JobMaster的当前SlotPool中尝试获取资源，如果资源不足，则从SlotPool中申请新的Slot，然后SlotPool向ResourceManager请求新的Slot

**共享Slot资源申请** 向SlotSharingManager请求资源，如果有CoLocation限制，则申请CoLocation MultiTaskSlot，否则申请一般的MultiTaskSlot。

Flink中用TaskSlot来定义Slot共享的结构，SingleTaskSlot表示运行单个Task的Slot，每个SingleTaskSlot对应于一个LogicalSlot，MultiTaskSlot中包含了一组TaskSlot。借助SingleTaskSlot和MultiTaskSlot，Flink实现了一般Slot共享和CoLocationGroup共享。

#### 计算资源管理

在Flink中，计算资源的是以Slot作为基本单位进行分配的。在Flink集群中，每个TaskManager都是一个单独的JVM进程，并且在一个TaskManager中可能运行多个子任务，这些子任务都在各自独立的线程中运行。为了控制一个TaskManager中可以运行的任务的数量，引入了Task Slot的概念。每一个Task Slot代表了TaskManager所拥有的计算资源的一个固定子集，这样，运行在不同slot中的子任务不会竞争内存资源。通过调整slot的数量可以控制子任务的隔离程度。在同一个JVM进程中的子任务，可以共享TCP连接和心跳消息，减少数据的网络传输，也能共享一些数据结构，一定程度上减少了每个子任务的消耗。默认情况下， Flink允许子任务共享slot，前提是，它们属于同一个Job并且不是同一个operator的子任务，这样在同一个Slot中可能会运行Job的一个完整的pipeline。允许Slot共享有两个主要的好处：

1. Flink计算一个Job所需的Slot数量时，只需要确定其最大并行度即可，而不用考虑每一个任务的并行度
2. 能更好的利用资源。如果没有Slot共享，那些资源需求不大的子任务和资源需求大的子任务会占用相同的资源，但一旦允许Slot共享，它们就可能被分配到同一个Slot中

Flink通过SlotSharingGroup和CoLocationGroup来确定在调度任务的时候如何进行资源共享，分别对应两种约束条件：

+ SlotSharingGroup：相同SlotSharingGroup的不同JobVertex的子任务可以被分配在同一个Slot中，但不保证能做到
+ CoLocationGroup：相同SlotSharingGroup的不同JobVertex，它们的第n个子任务必须保证都在同一个Slot中，这是一种强制性的约束

SlotID是一个Slot的唯一标识，它包含两个属性，其中ResourceID表明该Slot所在的TaskExecutor，slotNumber是该Slot在TaskExecutor中的索引位置。TaskSlot是TaskExecutor对Slot的抽象，存在ACTIVE、ALLOCATED、RELEASING三种状态。TaskSlot提供了修改状态的方法，如markActive()方法会将Slot标记为ACTIVE状态，markInactive()会将Slot标记为ALLOCATED状态，但只有在所有Task都被移除之后才能释放成功。Slot在切换状态时会先判断当前所处的状态。另外，可以通过add()方法向Slot中添加Task，需要保证这些Task都来自同一个Job。

<details>

<summary>具体实现</summary>

```Java
class TaskSlot<T extends TaskSlotPayload>
{
    int index;
    TaskSlotState state;
    ResourceProfile resourceProfile;
    Map<ExecutionAttemptID, T> tasks;
    JobID jobId;
    AllocationID allocationId;

    boolean add(T task) {
        tasks.put(task.getExecutionId(), task);
    }
}
```

</details>

TaskExecutor主要通过TaskSlotTable（实现类为TaskSlotTableImpl）来管理其拥有的所有Slot，包括Slot的申请与释放。其allocateSlot()方法将指定index的Slot分配给AllocationID对应的请求，createSlotReport()方法用于获取当前TaskExecutor中所有Slot的状态以及它们的分配情况（封装在SlotReport对象中）。

<details>

<summary>具体实现</summary>

```Java
class TaskSlotTableImpl<T extends TaskSlotPayload> implements TaskSlotTable<T> {
    // Slot数目
    int numberSlots;
    // 所有Slot
    Map<Integer, TaskSlot<T>> taskSlots;
    // 分配ID到Slot的映射
    Map<AllocationID, TaskSlot<T>> allocatedSlots;
    // ExecutionAttemptID到Slot的映射
    Map<ExecutionAttemptID, TaskSlotMapping<T>> taskSlotMappings;
    // JobID与分配的Slot
    Map<JobID, Set<AllocationID>> slotsPerJob;

    // 分配Slot
    boolean allocateSlot(int requestedIndex, JobID jobId, AllocationID allocationId, ResourceProfile resourceProfile, Time slotTimeout) {
        assert requestedIndex < numberSlots;
        int index = requestedIndex < 0 ? nextDynamicSlotIndex() : requestedIndex;
        ResourceProfile effectiveResourceProfile = resourceProfile.equals(ResourceProfile.UNKNOWN)
            ? defaultSlotResourceProfile
            : resourceProfile;
        TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
        assert taskSlot != null;
        taskSlot = new TaskSlot<>(
            index,
            effectiveResourceProfile,
            memoryPageSize,
            jobId,
            allocationId,
            memoryVerificationExecutor);
        taskSlots.put(index, taskSlot);
        allocatedSlots.put(allocationId, taskSlot);
        timerService.registerTimeout(allocationId, slotTimeout.getSize(), slotTimeout.getUnit());
        Set<AllocationID> slots = slotsPerJob.get(jobId);
        if (slots == null) {
            slots = new HashSet<>(4);
            slotsPerJob.put(jobId, slots);
        }
        slots.add(allocationId);
        return true;
    }
    // 释放Slot
    int freeSlot(AllocationID allocationId, Throwable cause) {
        // getTaskSlot
        TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
        AllocationID allocationId = taskSlot.AllocationId;
        LOG.info("Free slot {}.", ...);
        if (taskSlot.isEmpty()) {
            allocatedSlots.remove(allocationId);
            timerService.unregisterTimeout(allocationId);
            JobID jobId = taskSlot.getJobId();
            Set<AllocationID> slots = slotsPerJob.get(jobId);
            slots.remove(allocationId);
            if (slots.isEmpty()) {
                slotsPerJob.remove(jobId);
            }
            taskSlots.remove(taskSlot.getIndex());
            budgetManager.release(taskSlot.getResourceProfile());
        }
        return taskSlot.closeAsync(cause).isDone() ? taskSlot.getIndex() : -1;
    }
}
```

</details>

TaskExecutor需要向ResourceManager报告其所有Slot的状态，这样ResourceManager就知道了所有Slot的分配情况，这主要发生在两种情况之下：

+ TaskExecutor首次和ResourceManager建立连接的时候，需要发送SlotReport
+ TaskExecutor和ResourceManager定期发送心跳信息，心跳信息中包含SlotReport

<details>

<summary>具体实现</summary>

```Java
class TaskExecutor {
    void establishResourceManagerConnection(
        ResourceManagerGateway resourceManagerGateway,
        ResourceID resourceManagerResourceId,
        InstanceID taskExecutorRegistrationId,
        ClusterInformation clusterInformation) {
        // 首次建立连接，向ResourceManager报告Slot信息
        resourceManagerGateway.sendSlotReport(
            getResourceID(),
            taskExecutorRegistrationId,
            taskSlotTable.createSlotReport(getResourceID()),
            taskManagerConfiguration.getRpcTimeout());
        ......
    }
    class ResourceManagerHeartbeatListener
        implements HeartbeatListener<Void, TaskExecutorHeartbeatPayload>
    {
        @Override
        TaskExecutorHeartbeatPayload retrievePayload(ResourceID resourceID) {
            // 心跳信息
            return new TaskExecutorHeartbeatPayload(
                taskSlotTable.createSlotReport(getResourceID()),
                partitionTracker.createClusterPartitionReport());
        }
    }
}
```

</details>

ResourceManager通过TaskExecutor的requestSlot()方法要求TaskExecutor分配Slot，由于ResourceManager知道所有Slot的当前状况，因此分配请求会精确到具体的SlotID，在Slot被分配给之后，TaskExecutor通过offerSlotsToJobManager()方法将对应的Slot提供给JobManager。Slot分配过程如下：

1. 首先检查请求Slot的ResourceManager是否是当前建立连接的ResourceManager，如果不是，就抛出相应的异常，需要等到TaskManager连接上ResourceManager之后才能处理ResourceManager的Slot请求
2. 判断TaskManager上的Slot是否可以分配
   + 如果Slot是FREE状态，则调用TaskSlotTable的allocateSlot()方法进行分配
   + 如果Slot已经分配，检查是不是分配给当前作业，如果不是，则抛出相应的异常告诉ResourceManager该Slot已经分配
3. 如果TaskManager已经有了这个JobManager，会重新向JobManager汇报该TaskManager上的Slot已分配给该Job

<details>

<summary>具体实现</summary>

```Java
class TaskExecutor {

    CompletableFuture<Acknowledge> requestSlot(
        SlotID slotId,
        JobID jobId,
        AllocationID allocationId,
        ResourceProfile resourceProfile,
        String targetAddress,
        ResourceManagerId resourceManagerId,
        Time timeout) {
        log.info("Receive slot request {} for job {} from resource manager with leader id {}.", ...);
        // 检查请求Slot的ResourceManager是否是当前建立连接的ResourceManager
        assert establishedResourceManagerConnection != null
            && resourceManagerAddress != null
            && resourceManagerAddress.ResourceManagerId.equals(resourceManagerId);
        // allocateSlot，分配Slot
        if (!taskSlotTable.taskSlots.containsKey(slotId.SlotNumber)) {
            // Slot尚未分配
            taskSlotTable.allocateSlot(
                slotId.getSlotNumber(),
                jobId,
                allocationId,
                resourceProfile,
                taskManagerConfiguration.getSlotTimeout());
            log.info("Allocated slot for {}.", ...);
        } else {
            // Slot应该分配给当前作业
            assert taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId);
        }
        // 将分配的Slot提供给发送请求的JobManager
        // 如果和对应的JobManager已经建立了连接，则向JobManager提供Slot
        // 否则，先和JobManager建立连接，连接建立后会调用offerSlotsToJobManager()方法
        // 在 Slot 被分配给之后，TaskExecutor 需要将对应的 slot 提供给 JobManager，而这正是通过 offerSlotsToJobManager(jobId) 方法来实现的
        JobTable.Job job = jobTable.getOrCreateJob(jobId, () -> registerNewJobAndCreateServices(jobId, targetAddress));
        // getOrCreateJob
        JobTable.Job job = jobTable.jobs.get(jobId);
        if (job == null) {
            job = new JobOrConnection(jobId, registerNewJobAndCreateServices(jobId, targetAddress));
            jobTable.jobs.put(jobId, job);
        }
        if (job.isConnected()) {
            // 如果TaskManager已经有了这个JobManager，会重新向JobManager汇报该TaskManager上的Slot已分配给该Job
            offerSlotsToJobManager(jobId);
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }
    void offerSlotsToJobManager(JobID jobId) {
        JobTable.Connection jobManagerConnection = jobTable.getConnection(jobId);
        if (taskSlotTable.hasAllocatedSlots(jobId)) {
            log.info("Offer reserved slots to the leader of job {}.", ...);
            JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();
            // 获取分配给当前Job的Slot，只取得状态为allocated的Slot
            Iterator<TaskSlot<Task>> reservedSlotsIterator = taskSlotTable.getAllocatedSlots(jobId);
            JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();
            Collection<SlotOffer> reservedSlots = new HashSet<>(2);
            while (reservedSlotsIterator.hasNext()) {
                SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
                reservedSlots.add(offer);
            }
            UUID slotOfferId = UUID.randomUUID();
            currentSlotOfferPerJob.put(jobId, slotOfferId);
            // 通过RPC调用，将Slot提供给JobMaster
            CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture =
                jobMasterGateway.offerSlots(
                    getResourceID(),
                    reservedSlots,
                    taskManagerConfiguration.getRpcTimeout());
            acceptedSlotsFuture.whenComplete(
                (Iterable<SlotOffer> acceptedSlots, Throwable throwable) -> {
                    asset offerId.equals(currentSlotOfferPerJob.get(jobId));
                    if (throwable != null) {
                        if (throwable instanceof TimeoutException) {
                            // 超时，则重试
                            offerSlotsToJobManager(jobId);
                        } else {
                            // 发生异常，则释放所有的Slot
                            for (SlotOffer reservedSlot : offeredSlots) {
                                freeSlotInternal(reservedSlot.getAllocationId(), throwable);
                            }
                        }
                    } else {
                        // 调用成功
                        if (isJobManagerConnectionValid(jobId, jobMasterId)) {
                            // 对于被JobMaster确认接受的Slot， 标记为Active状态
                            for (SlotOffer acceptedSlot : acceptedSlots) {
                                AllocationID allocationId = acceptedSlot.getAllocationId();
                                if (!taskSlotTable.markSlotActive(allocationId)) {
                                    jobMasterGateway.failSlot(...);
                                }
                                offeredSlots.remove(acceptedSlot);
                            }
                            // 释放剩余没有被接受的Slot
                            for (SlotOffer rejectedSlot : offeredSlots) {
                                freeSlotInternal(rejectedSlot.getAllocationId(), ...);
                            }
                        }
                    }
                },
                getMainThreadExecutor());
        } else {
            log.debug("There are no unassigned slots for the job {}.", ...);
        }
    }
}
```

</details>

通过freeSlot()方法可以请求TaskExecutor释放和AllocationID关联的Slot。Slot释放过程如下：

1. 先调用TaskSlotTable的freeSlot()方法，尝试释放Slot
    + 如果Slot上没有Task在运行，那么释放Slot并更新状态为FREE
    + 先将Slot状态更新为RELEASING，然后再遍历Slot上的Task，逐个将其标记为Failed
2. 如果Slot被成功释放（状态是FREE），将通知ResourceManager这个Slot又可用了
3. 更新缓存信息

<details>

<summary>具体实现</summary>

```Java
class TaskExecutor
{
    CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
        log.debug("Free slot with allocation id {} because: {}", ...);
        // 获取Slot分配到的JobID
        JobID jobId = taskSlotTable.allocatedSlots.get(allocationId).getJobId();
        // 尝试释放allocationId关联的Slot
        int slotIndex = taskSlotTable.freeSlot(allocationId, cause);
        if (slotIndex != -1) {
            // Slot释放成功
            if (isConnectedToResourceManager()) {
                // 告知ResourceManager当前Slot可用
                ResourceManagerGateway resourceManagerGateway = establishedResourceManagerConnection.getResourceManagerGateway();
                resourceManagerGateway.notifySlotAvailable(
                    establishedResourceManagerConnection.taskExecutorRegistrationId,
                    new SlotID(getResourceID(), slotIndex),
                    allocationId);
            }
            if (jobId != null) {
                // closeJobManagerConnectionIfNoAllocatedResources，如果和allocationID关联的Job已经没有分配的Slot了，那么断开和JobMaster的连接
                if (taskSlotTable.getAllocationIdsPerJob(jobId).isEmpty()
                    && !partitionTracker.isTrackingPartitionsFor(jobId)) {
                    log.debug("Releasing job resources for job {}.", ...);
                    partitionTracker.stopTrackingAndReleaseJobPartitionsFor(jobId);
                    jobLeaderService.removeJob(jobId);
                    JobTable.Connection jobManagerConnection = jobTable.getJob(jobId).asConnection();
                    disconnectJobManagerConnection(jobManagerConnection);
                    changelogStoragesManager.releaseStateChangelogStorageForJob(jobId);
                    currentSlotOfferPerJob.remove(jobId);
                }
            }
        }
        // 释放allocationId的相应状态信息
        localStateStoresManager.releaseLocalStateForAllocationId(allocationId);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }
}
```

</details>

ResoureManager需要对所有TaskExecutor的Slot进行管理，所有JobManager都是通过ResourceManager进行资源申请，ResourceManager则根据当前集群计算资源的使用情况将请求转发给TaskExecutor。ResourceManager通过SlotManager来管理Slot，SlotManager维护了所有已经注册的TaskExecutor的所有Slot的状态及它们的分配情况，SlotManager还维护了所有处于等待状态的Slot请求。每当有一个新的Slot注册或者一个已经分配的Slot被释放的时候，SlotManager会试图去满足处于等待状态SlotRequest，如果可用的Slot不足以满足要求，SlotManager会通过ResourceActions的allocateResource()方法告知 ResourceManager，ResourceManager可能会尝试启动新的TaskExecutor。另外，长时间处于空闲状态的TaskExecutor或者长时间没有被满足的pending slot request，会触发超时机制进行处理。

<details>

<summary>具体实现</summary>

```Java
class DeclarativeSlotManager implements SlotManager
{
    SlotTracker slotTracker;
    Map<SlotID, AllocationID> pendingSlotAllocations;
}

class DefaultSlotTracker implements SlotTracker
{
    Map<SlotID, DeclarativeTaskManagerSlot> slots;
    Map<SlotID, DeclarativeTaskManagerSlot> freeSlots;
    @Override
    void addSlot(
        SlotID slotId,
        ResourceProfile resourceProfile,
        TaskExecutorConnection taskManagerConnection,
        JobID assignedJob) {
        DeclarativeTaskManagerSlot slot = new DeclarativeTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);
        slots.put(slotId, slot);
        freeSlots.put(slotId, slot);
        slotStatusStateReconciler.executeStateTransition(slot, assignedJob);
    }
    @Override
    void notifyResourceRequirements(
        JobID jobId,
        Collection<ResourceRequirement> resourceRequirements) {
        LOG.trace("Received notification for job {} having new resource requirements {}.", ...);
        trackers.get(jobId).notifyResourceRequirements(resourceRequirements);
        if (resourceRequirements.isEmpty()) {
            checkWhetherTrackerCanBeRemoved(jobId, trackers.get(jobId));
        }
    }
}

class JobScopedResourceTracker
{
    void notifyResourceRequirements(
        Collection<ResourceRequirement> newResourceRequirements) {
        resourceRequirements = ResourceCounter.empty();
        for (ResourceRequirement newResourceRequirement : newResourceRequirements) {
            resourceRequirements = resourceRequirements.add(
                newResourceRequirement.getResourceProfile(),
                newResourceRequirement.getNumberOfRequiredSlots());
        }
        // findExcessSlots
        Collection<ExcessResource> excessResources = new ArrayList<>();
        for (ResourceProfile requirementProfile : resourceToRequirementMapping.getAllRequirementProfiles()) {
            int numTotalRequiredResources = resourceRequirements.getResourceCount(requirementProfile);
            int numTotalAcquiredResources = resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);
            if (numTotalAcquiredResources > numTotalRequiredResources) {
                int numExcessResources = numTotalAcquiredResources - numTotalRequiredResources;
                for (Map.Entry<ResourceProfile, Integer> acquiredResource :
                    resourceToRequirementMapping
                        .getResourcesFulfilling(requirementProfile)
                        .getResourcesWithCount()) {
                    ResourceProfile acquiredResourceProfile = acquiredResource.getKey();
                    int numAcquiredResources = acquiredResource.getValue();
                    if (numAcquiredResources <= numExcessResources) {
                        excessResources.add(
                            new ExcessResource(
                                requirementProfile,
                                acquiredResourceProfile,
                                numAcquiredResources));
                        numExcessResources -= numAcquiredResources;
                    } else {
                        excessResources.add(
                            new ExcessResource(
                                requirementProfile,
                                acquiredResourceProfile,
                                numExcessResources));
                        break;
                    }
                }
            }
        }
        if (!excessResources.isEmpty()) {
            LOG.debug("Detected excess resources for job {}: {}", ...);
            for (ExcessResource excessResource : excessResources) {
                resourceToRequirementMapping.decrementCount(
                    excessResource.requirementProfile,
                    excessResource.resourceProfile,
                    excessResource.numExcessResources);
                this.excessResources =
                    this.excessResources.add(
                        excessResource.resourceProfile,
                        excessResource.numExcessResources);
            }
        }
        // tryAssigningExcessSlots
        ResourceCounter assignedResources = ResourceCounter.empty();
        for (Map.Entry<ResourceProfile, Integer> excessResource : excessResources.getResourcesWithCount()) {
            for (int i = 0; i < excessResource.getValue(); i++) {
                ResourceProfile resourceProfile = excessResource.getKey();
                Optional<ResourceProfile> matchingRequirement = findMatchingRequirement(resourceProfile);
                if (matchingRequirement.isPresent()) {
                    resourceToRequirementMapping.incrementCount(matchingRequirement.get(), resourceProfile, 1);
                    assignedResources = assignedResources.add(resourceProfile, 1);
                } else {
                    break;
                }
            }
        }
        for (Map.Entry<ResourceProfile, Integer> assignedResource : assignedResources.getResourcesWithCount()) {
            excessResources = excessResources.subtract(assignedResource.getKey(), assignedResource.getValue());
        }
    }
}
```

</details>

**注册Slot** 当一个新的TaskManager注册的时候，registerTaskManager()方法被调用来注册Slot。另外，TaskManager也会定期通过心跳向ResourceManager报告Slot的状态，在reportSlotStatus()方法中会更新Slot状态。

<details>

<summary>具体实现</summary>

```Java
@Override
class DeclarativeSlotManager implements SlotManager {
    @Override
    boolean registerTaskManager(
        TaskExecutorConnection taskExecutorConnection,
        SlotReport initialSlotReport,
        ResourceProfile totalResourceProfile,
        ResourceProfile defaultSlotResourceProfile) {
        LOG.debug("Registering task executor {} under {} at the slot manager.", ...);
        taskExecutorManager.registerTaskManager(
            taskExecutorConnection,
            initialSlotReport,
            totalResourceProfile,
            defaultSlotResourceProfile);
        // 依次注册所有的Slot
        for (SlotStatus slotStatus : initialSlotReport) {
            slotTracker.addSlot(
                slotStatus.getSlotID(),
                slotStatus.getResourceProfile(),
                taskExecutorConnection,
                slotStatus.getJobID());
        }
        return true;
    }
}
```

</details>

**请求Slot** ResourceManager通过SlotManager的processResourceRequirement()方法请求Slot。

<details>

<summary>具体实现</summary>

```Java
class DeclarativeSlotManager implements SlotManager
{
    @Override
    void processResourceRequirements(ResourceRequirements resourceRequirements) {
        LOG.info("Received resource requirements from job {}: {}", ...);
        jobMasterTargetAddresses.put(
            resourceRequirements.getJobId(),
            resourceRequirements.getTargetAddress());
        resourceTracker.notifyResourceRequirements(
            resourceRequirements.getJobId(),
            resourceRequirements.getResourceRequirements());
    }
}
```

</details>