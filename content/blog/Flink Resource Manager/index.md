---
title: Flink资源管理
date: "2023-01-11"
description: Slot物理上代表了TaskManager JVM进程中的一个线程，逻辑上代表了作业中一个算子（或多个共享Slot算子）的一个并行度。
tags: Slot分配、Slot共享、Slot回收
---

```toc
ordered: true
class-name: "table-of-contents"
```

Slot物理上代表了TaskManager JVM进程中的一个线程，逻辑上代表了作业中一个算子（或多个共享Slot算子）的一个并行度。

按对计算资源的需求不同，计算任务可以分为IO密集型、CPU密集型、内存密集型等。Flink资源抽象为两个层面：集群资源抽象和Flink自身资源抽象。集群资源使用资源管理器（ResourceManager）来解耦Flink和资源管理集群，Flink从资源管理框架申请和释放资源^[资源管理框架（Yarn、K8s、Mesos等）提供硬件资源的抽象、计算任务的隔离、资源的管理等特性]。Flink使用Slot精细地划分自身资源，从资源管理框架申请资源容器，并对申请到的资源进行切分，每一份叫做Task Slot，作业能够充分利用计算资源，同时使用Slot共享进一步提高资源利用效率。

Slot表示TaskManager拥有资源的一个固定大小的子集，Slot的资源化意味着一个作业的Task将不需要跟来自其他作业的Task竞争内存、CPU等计算资源。通过调整Slot的数量，用户可以定义Task之间如何互相隔离。如果一个TaskManager只有一个Slot，意味着每个Task独立地运行在JVM中，而一个TaskManager有多个Slot，则意味着更多的Task可以共享一个JVM。在同一个JVM进程中的Task将共享TCP连接和心跳消息。考虑到不同Task的计算任务对资源需求的差异，Flink在Slot的基础上，设计了Slot共享机制。SlotSharingManager用在Flink作业的执行调度中，负责Slot的共享，不同的Task可以共享Slot。

## 组件

Flink资源管理中涉及JobMaster、ResourceManager、TaskManager三种角色，JobMaster是Slot资源的使用者，向ResourceManager申请资源，ResourceManager负责分配资源和资源不足时申请资源，资源空闲时释放资源，TaskManager是Slot资源的持有者，在其Slot清单中记录了Slot分配给了哪个作业的哪个Task。1个容器中运行1个TaskManager进程，TaskManager为每个Task分配独立的执行线程，一个TaskManger中可能执行一个或多个Task。

**ResourceManager** 资源管理器位于Flink和资源管理集群之间，是Flink集群级资源管理的抽象，Flink内置了YarnResourceManager、KubernetesResourceManager、StandaloneResourceManager、MesosResourceManager等4种ResourceManager，分别对应不同的资源管理框架，主要作用如下：
+ 申请容器启动新的TaskManager，或者为作业申请Slot
+ 处理JobManager和TaskManager的异常退出
+ 缓存TaskManager，等待一段时间之后再释放掉不用的容器，避免资源反复的申请释放
+ JobManager和TaskManager的心跳感知，对JobManager和TaskManager的退出进行对应的处理

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

## Slot共享

默认情况下，Flink作业共享同一个SlotSharingGroup，同一个作业中来自不同JobVertex的作业可以共享作业，使用Slot共享，可以在一个Slot中运行Task组成的流水线。在实际运行时，每个Task会被赋予一个编号，在Slot共享时，同一个计算步骤的不同编号的Task不能分配到相同的Slot中。共享Slot有以下优点：
+ 资源分配简单：Flink集群需要的Slot数量和作业中的最高并行度一致，不需要计算一个程序总共包含多少个Task
+ 资源利用率高：如果没有Slot共享，资源密集型的Task跟非密集型的作业占用相同的资源，在整个TaskManager层面上，资源没有充分利用

Slot共享管理器（SlotSharingManager）和Slot共享组（SlotSharingGroup）在作业调度执行时发挥作用，部署Task之前，选择Slot确定Task发布到哪个TaskManager。SlotSharingmanager负责管理资源共享与分配，1个SlotSharingGroup对应1个SlotSharingManager。Flink有两种共享组：SlotSharingGroup和CoLocationGroup。SlotSharingGroup为非强制性共享约束，Slot共享根据组内的JobVerticesID查找是否已有可以共享的Slot，只要确保相同JobVertexID不能出现在同一个共享的Slot内即可。在符合资源要求的Slot中，找到没有相同JobVertexID的Slot，根据Slot选择策略选择一个Slot即可，如果没有符合条件的，则申请新的Slot。CoLcationGroup为本地约束共享组，具有强制性的Slot共享限制，CoLocationGroup用在迭代运算中，即在IterativeStream的API中调用，迭代运算中的Task必须共享同一个TaskManager的Slot。CoLacationGroup根据组内每个ExecutionVertex关联的CoLocationConstraint查找是否有相同CoLocationConstraint约束已分配Slot可用，在调用作业执行的时候，首先要找到本约束中其他Task部署的TaskManager，如果没有则申请一个新的Slot，如果有则共享该TaskManger上的Slot。

JobGraph向ExecutionGraph的转换过程中，为每一个ExecutionVertex赋予了按照并行度编写的编号，相同编号的迭代计算ExecutionVertex会被放入本地共享约束组中，共享相同的CoLocationConstraint对象，在调度的时候，根据编号就能找到本组其他Task的Slot信息。

**单独Slot资源申请** 首先会从JobMaster的当前SlotPool中尝试获取资源，如果资源不足，则从SlotPool中申请新的Slot，然后SlotPool向ResourceManager请求新的Slot

**共享Slot资源申请** 向SlotSharingManager请求资源，如果有CoLocation限制，则申请CoLocation MultiTaskSlot，否则申请一般的MultiTaskSlot。

Flink中用TaskSlot来定义Slot共享的结构，SingleTaskSlot表示运行单个Task的Slot，每个SingleTaskSlot对应于一个LogicalSlot，MultiTaskSlot中包含了一组TaskSlot。借助SingleTaskSlot和MultiTaskSlot，Flink实现了一般Slot共享和CoLocationGroup共享。