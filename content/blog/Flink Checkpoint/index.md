---
title: Flink应用容错
date: "2023-01-02"
description: Task有不同可靠级别的容错语义保证，底层依赖于轻量级异步快照。轻量级异步快照机制通过在数据流中注入检查点Barrier，随着数据流动，在各个算子上执行检查点，保证状态到外部的可靠存储中，当作业发生异常的时候，从最后一次成功的检查点中恢复状态。同时在检查点基础上设计了保存点，使得在作业迁移、集群升级等过程中也能保证作业执行结果的精确性。另外，基于检查点机制，在框架级别支持两阶段提交协议，实现了端到端严格一次的语义保证
tags: 检查点、两阶段提交协议、轻量级异步分布式快照、Flink作业恢复
---

Task有不同可靠级别的容错语义保证，底层依赖于轻量级异步快照。轻量级异步快照机制通过在数据流中注入检查点Barrier，随着数据流动，在各个算子上执行检查点，保证状态到外部的可靠存储中，当作业发生异常的时候，从最后一次成功的检查点中恢复状态。同时在检查点基础上设计了保存点，使得在作业迁移、集群升级等过程中也能保证作业执行结果的精确性。另外，基于检查点机制，在框架级别支持两阶段提交协议，实现了端到端严格一次的语义保证。

按照数据处理保证的可靠程度，从低到高包含4个不同的层次：
+ 至多一次（At-Most-Once）：数据不重复处理，但可能丢失
+ 最少一次（At-Least-Once）：数据可能重复处理，但保证不丢失，在Flink中，开启检查点不进行Barrier对齐就是最少一次的处理保证
+ 引擎内严格一次（Exactly-Once）：在计算引擎内部，数据不丢失、不重复，在Flink中开启检查点，且对Barrier进行对齐就是引擎内严格一次的处理保证，如果数据源支持断点读取，则能支持从数据源到引擎处理完毕，再写出到外部存储之前的过程中的严格一次
+ 端到端严格一次（End-to-End Exactly-Once）：从数据读取、引擎处理到写入外部存储的整个过程中，数据不重复、不丢失。端到端严格一次语义需要数据源支持可重放、外部存储支持事务机制、能够进行回滚

## 作业恢复

检查点（Checkpoint）是Flink实现应用容错的核心机制，根据配置周期性通知Stream中各个算子的状态来生成检查点快照，从而将这些状态数据持久化存储下来，Flink程序一旦意外崩溃，重新运行程序时可以有选择地从这些快照进行恢复，将应用恢复到最后一次快照的状态，从此刻开始执行，避免数据的丢失、重复。

保存点（Savepoint）是基于Flink检查点机制的应用完整快照备份机制，用来保存状态，可以在另一个集群或者另一个时间点，从保存的状态中将作业恢复回来，适用于应用升级、集群迁移、Flink集群版本更新等场景。保存点可以视为一个算子ID->状态的Map，对于每一个有状态的算子，Key是算子ID，Value是算子状态。

Flink提供了两种手动作业恢复方式：
+ 外部检查点：检查点完成时，在用户给定的外部持久化存储保护，当作业失败（或取消）时，外部存储的检查点会保留下来，用户在恢复时需要提供用于恢复的作业状态的检查点路径
+ 保存点：用户通过命令触发，由用户手动创建、清理，使用了标准化格式存储，允许作业升级或者配置变更，用户在恢复时需要提供用于恢复的作业状态的保存点路径

### 检查点恢复

**自动检查点恢复** 可以在配置文件中提供全局配置，也可以在代码中为Job特别设定。支持以下重启策略
+ 固定延迟重启策略：配置参数fixed-delay，会尝试一个给定的次数来重启作业，如果超过了最大的重启次数，Job最终将失败，在连续的两次重启尝试之间，重启策略会等待一个固定的时间，默认为Integer.MAX_VALUE次
+ 失败率重启策略：配置参数failure-rate，在作业失败后会重启，但是超过失败率后，作业会最终被认定失败，在两个连续的重启尝试之间，重启策略会等待一个固定的时间
+ 直接失败策略：配置参数None，失败不重启

**手动检查点恢复** 在启动之时通过设置-s参数指定检查点目录的功能，让新的jobId读取该检查点元文件信息和状态信息，从而达到指定时间节点启动作业的目的

### 保存点恢复

从保存点恢复作业需要考虑以下几点：
1. 算子的顺序改变：如果算子对应的UID没变，则可以恢复，如果对应的UID变了则恢复失败
2. 作业中添加了新的算子：如果是无状态算子，则没有影响，可以正常恢复，如果是有状态的算子，跟无状态的算子一样处理
3. 从作业中删除了一个有状态的算子：从保存点恢复时通过在命令中添加--allowNonRestoreState（或 -n）跳过无法恢复的算子

## 关键组件

**检查点协调器（CheckpointCoordinator）** 负责协调Flink算子的状态的分布式快照，当出发快照的时候，CheckpointCoordinator向Source算子注入Barrier消息，然后等待所有的Task通知检查点确认完成，同时持有所有Task在确认完成消息中上报的状态句柄

**检查点消息** 在执行检查点的过程中，TaskManager和JobManager之间通过消息确认检查点执行成功还是取消，Flink中设计了检查点消息类体系，检查点消息中有3个重要信息，该检查点所属的作业标识（JobID）、检查点编号、Task标识（ExecutionAttemptID）
+ AcknowledgeCheckpoint消息：从TaskExecutor发往JobMaster，告知算子的快照备份完成
+ DeclineCheckpoint消息：从TaskExecutor发送JobMaster，告知算子无法执行快照备份

## 轻量级异步分布式快照

Flink采用轻量级分布式快照实现应用容错。分布式快照最关键的是能够将数据流切分，Flink中使用屏障（Barrier）来切分数据流。Barrier会周期性地注入数据流中，作为数据流的一部分，从上游到下游被算子处理。Barrier会严格保证顺序，不会超过其前边的数据。Barrier将记录分割成记录集，两个Barrier之间的数据流中的数据隶属于同一个检查点，每一个Barrier都携带一个其所属快照的ID编号。Barrier随着数据向下流动，不会打断数据流，因此非常轻量。

Barrier在Source算子被注入并行数据流中，Barrier n所在的位置就是恢复时数据重新处理的起始位置^[在Kafka中，这个位置就是最后一个记录在分区内的偏移量，作业恢复时，会根据这个位置从这个偏移量之后向kafka请求数据，这个偏移量就是State中保存的内容之一]。Barrier向下游传递，当一个非数据源算子从所有的输入流中收到了快照n的Barrier时，该算子就会对自己的状态保存快照，并向自己的下游广播快照n的Barrier。一旦Sink算子接收到Barrier，有两种情况：
+ 如果是引擎内严格一次处理保证，当Sink算子已经收到了所有上游的Barrier n时，Sink算子对自己的状态进行快照，然后通知检查点协调器，当所有的Sink算子都向检查点协调器汇报成功之后，检查点协调器向所有算子确认本次快照完成
+ 如果是端到端严格一次处理保证，当Sink算子已经收到了所有上游的Barrier n时，Sink算子对自己的状态进行快照，并预提交事务，再通知检查点协调器，检查点协调器向所有的算子确认本次快照完成，Sink算子提交事务，本次快照完成

**Barrier对齐** 当一个算子有多个上游输入的时候，为了达到引擎内严格一次、端到端严格一次两种保证语义，此时必须要Barrier对齐。对于有两个上游输入通道的算子，算子Barrier对齐过程如下
1. 开始对齐：算子收到输入通道1的Barrier，输入通道2的Barrier尚未到达
2. 对齐：算子继续从输入通道1接收数据，但是并不处理，而是保存在输入缓存中，等待输入通道2的Barrier到达
3. 执行检查点：输入通道2的Barrier到达，算子开始对其状态进行异步快照，并将Barrier向下游广播，并不等待快照执行完毕
4. 继续处理数据：算在在做异步快照，首先处理缓存中积压的数据，然后再从输入通道中获取数据

### 检查点执行过程

JobMaster作为作业的管理者，是作业的控制中枢，在JobMaster中的CheckpointCoordinator组件专门负责检查点的管理，包括何时触发检查点、检查点完成的确认。检查点的具体执行者则是作业的各个Task，各个Task再将检查点的执行交给算子，算子是最底层的执行者

```bob-svg
                                    ,---------------------,
           trigger Checkpoint       |     JobMaster       |
      +-----------------------------+   ,-------------,   |
      |                             |   | Checkpoint  |   |
      |     +---------------------->+   | Coordinator |   +<-------------------------+
      |     |   report succ/fail    |   '-------------'   |    report succ/fail      |
      |     |                       '----------+----------'                          |
      |     |                                  ^                                     |
      v     |                                  |                                     |
 ,----+-----+----,                             |                              ,------+--------,
 |  StreamSink   |                     ,-------+--------,                     |  StreamSink   |
 |               |-------------------->+ StreamOperator +-------------------->+               |
 | Sink Operator | Checkpoint Barrier  '----------------' Checkpoint Barrier  | Sink Operator |
 '---------------'                                                            '---------------'
```

检查点分为两个阶段：第一阶段由JobMaster触发检查点，各算子执行检查点并汇报；第二阶段是JobMaster确认检查点完成，并通知各个算子，在这个过程中，出现任何异常都会导致检查点失败，放弃本次检查点

1. JobMaster触发检查点：JobMaster中的CheckpointCoordinator周期性地触发检查点的执行，CheckpointCoordinator触发检查点的时候，只需通知执行数据读取的SourceTask，从SourceTask开始产生CheckPointBarrier事件，注入数据流中，数据流向下游流动时被算子读取，在算子上触发检查点行为
    1. 前置检查：
        1. 未启用Checkpoint、作业关闭过程中或尚未达到触发检查点的最小间隔时都不允许执行
        2. 检查是否所有需要执行检查点的Task都处于执行状态，能够执行检查点和向JobMaster汇报
        3. 执行`CheckpointID=CheckpointIdCounter.getAndIncrement()`，生成一个新的id，然后生成一个PendingCheckpoint^[PendingCheckpoint是一个启动了的检查点，但是还没有被确认，等到所有的Task都确认了本次检查点，那么这个检查点对象将转化为一个CompletedCheckpoint]
        4. 检查点执行超时时取消本次检查点
        5. 触发MasterHooks^[用户可以定义一些额外的操作，用以增强检查点的功能]
        6. 再次执行步骤1和2中的检查，如果一切正常，则向各个SourceStreamTask发送通知，触发检查点执行
    2. 向Task发送触发检查点消息Execution表示一次ExecutionVertex的执行，对应于Task实例，在JobMaster端通过Execution的Slot可以找到对应的TaskManagerGateway，远程触发Task的检查点
2. TaskExecutor执行检查点：JobMaster通过TaskManagerGateway触发TaskManager的检查点执行，TaskManager则转交给Task执行
    1. Task层面的检查点执行准备：Task类中的CheckpointMetaData对象确保Task处于Running状态，把工作转交给StreamTask，而StreamTask也转交给更具体的类，直到最终执行用户编写的函数
    2. StreamTask执行检查点：SourceStreamTask是检查点的触发点，产生CheckpointBarrier并向下游广播，下游的StreamTask根据CheckpointBarrier触发检查点。核心逻辑为如果Task是Running状态，就可以执行检查点，首先在OperatorChain上执行准备CheckpointBarrier的工作，然后向下游所有Task广播CheckpointBarrier，最后触发自己的检查点^[这样做可以尽快将CheckpointBarrier广播到下游，避免影响下游CheckpointBarrier对齐，降低整个检查点执行过程的耗时]；如果Task是非Running，就向下游发送CancelCheckpointMarker，通知下游取消本次检查点
    3. 算子生成快照：在StreamTask中经过一系列简单调用之后，异步触发OperatorChain中所有算子的检查点。算子开始从StateBackend中深度复制状态数据，并持久化到外部存储中。注册回调，执行完检查点后向JobMaster发出CompletedCheckPoint消息
    4. 算子保存快照与状态持久化：触发保存快照的动作之后，首先对OperatorState和KeyState分别进行处理，如果是异步的，则将状态写入外部存储。持久化策略负责将状态写入目标存储
    5. Task报告检查点完成：当一个算子完成其状态的持久化之后，就会向JobMaster发送检查点完成消息，具体逻辑在reportCompletedSnapshotStates中，该方法又把任务委托给了RpcCheckpointResponder类。在向JobMaster汇报的消息中，TaskStateSnapshot中保存了本次检查点的状态数据^[内存型StateBackend中保存的是真实的状态数据，文件型StateBackend中保存的是状态的句柄]，在分布式文件系统中的保存路径也是通过TaskStateSnapshot中保存的信息恢复回来的。状态的句柄分为OperatorStateHandle和KeyedStateHandle，分别对应OperatorState和KeyState，同时也区分了原始状态和托管状态
3. Job Master确认检查点：JobMaster通过调度器SchedulerNG任务把消息交给CheckpointCoordinator.receiveAcknowledgeMessage来响应算子检查点完成事件。CheckpointCoordinator在触发检查点时，会生成一个PendingCheckpoint，保存所有算子的ID。当PendingCheckpoint收到一个算子的完成检查点的消息时，就把这个算子从未完成检查点的节点集合移动到已完成的集合。当所有的算子都报告完成了检查点时，CheckpointCoordinator会触发`completePendingCheckpoint()`方法，该方法做了以下事情：
    1. 把pendingCgCheckpoint转换为CompletedCheckpoint
    2. 把CompletedCheckpoint加入已完成的检查点集合，并从未完成检查点集合删除该检查点，CompletedCheckpoint中保存了状态的句柄、状态的存储路径、元信息的句柄等信息
    3. 向各个算子发出RPC请求，通知该检查点已完成

```Java
// CheckpointCoordinator触发检查点
// CheckpointCoordinator.java
@VisibleForTesting
public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(long timestamp, CheckpointProperties props, @Nullable String externalSavepointLocation, boolean isPeriodic, boolean advanceToEndOfTime) throws CheckpointException {
     ......
     try {
          ......
          // 通知作业的各个SourceTask触发Checkpoint，同步则触发Savepoint，异步则创建Checkpoint
          for (Execution execution : executions) {
               if (props.isSynchronous()) {
                    execution.triggerSynchronousSavepoint(checkpointID, timestamp, checkpointOptions, advanceToEndOfTime);
               } else {
                    execution.triggerCheckpoint(checkpointID, timestamp, checkpointOptions);
               }
          }
          numUnsuccessfulCheckpointsTriggers.set(0);
          return checkpoint.getCompletionFuture();
     } catch (Throwable t) {
          ......
     }
}

// 通知Task执行检查点
// Execution.java
private void triggerCheckpointHelper(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
     final LogicalSlot slot = assignedResource;
     if (slot != null) {
          final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
          taskManagerGateway.triggerCheckpoint(attemptId, getVertex().getJobId(), checkpointId, timestamp, checkpointOptions, advanceToEndOfEventTime);
     }
}

// Task处理检查点
// Task.java
public void triggerCheckpointBarrier(final long checkpointID, final long checkpointTimestamp, final CheckpointOptions checkpointOptions, final boolean advanceToEndOfEventtime) {
     final AbstractInvokable invokable = this.invokable;
     final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointID, checkpointTimestamp);
     if (executionState == ExecutionState.RUNNING && invokable != null) {
          try {
               invokable.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
          } catch (RejectedExecutionException ex) {
               ......
          }
     } else {
          checkpointResponder.declineCheckpoint(jobId, executionId, checkpointID, new CheckpointException("Task name with subtask : " + taskNameWithSubtask, CheckpointFailureReasono.CHECKPOINT_DECLINED_TASK_NOT_READY));
     }
}

// SourceStreamTask触发Checkpoint
@Override
public Future<Boolean> triggerCheckpointAsync(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
     return mailboxProcessor.getMainMailboxExecutor().submit(() -> triggerCheckpoint(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime), "checkpoint %s with %s", checkpointMetaData, checkpointOptions);
}

// Barrier触发检查点
@Override
public void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throw Exception {
     try {
          if (performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics, false)) {
               if (isSynchronousSavepointId(checkpointMetaData.getCheckpointId())) {
                    runSynchronousSavepointMailboxLoop();
               }
          }
     }
     ...
}

// StreamTask执行检查点
// StreamTask.java
private boolean performCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics, boolean advanceToEndOfTime) throws Exception {
     ......
     synchronized (lock) {
          if (isRunning) {
              ......
              // 下面的3个步骤，在实现时采用的是异步方式，后续步骤并不等待前边的操作完成
              operatorChain.prepareSnapshotPreBarrier(checkpointId);
              operatorChain.broadcastCheckpointBarrier(checkpointId, checkpointMetaData.getTimestamp(), checkpointOptions);
              checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);
              result = true;
          } else {
               // 告诉下游取消Checkpoint
               final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
               Exception exception = null;
               for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter : recordWriters) {
                    ......
                    recordWriter.broadcastEvent(message);
                    ......
               }
          }
     }
     ......
}

// StreamTask中异步触发算子检查点
// SteamTask.java
private static final class CheckpointingOperation {
     public void executeCheckpointing() throws Exception {
          ......
          try {
               // 调用StreamOperator进入snapshot的入口
               for (StreamOperator<?> op : allOperators) {
                    checkpointStreamOperator(op);
               }
               // 启动线程执行Checkpoint并注册清理行为
               AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(owner, operatorSnapshotsInProgress, checkpointMetaData, checkpointMetrics, startAsyncPartNano);
               owner.cancelables.registerCloseable(asyncCheckpointRunnable);
               // 注册runnable，执行完checkpoint之后，向JM发出CompletedCheckpoint
               owner.asyncOperationsThreadPool.submit(asyncCheckpointRunnable);
          } catch (Exception ex) {
               // 清理释放资源
               for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProcess.values()) {
                    ......
                    operatorSnapshotResult.cancel();
                    ......
               }
          }
     }
}

// 算子持久化State
// AbstractStreamOperator.java
@Override
public final OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, CheckpointStreamFactory factory) throws Exception {
     // 持久化原始State
     snapshotState(snapshotContext);
     // 持久化OperatorState
     if (null != operatorStateBackend) {
          snapshotInProgress.setOperatorStateManagedFuture(operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
     }
     // 持久化KeyedState
     if (null != operatorStateBackend) {
          snapshotInProgress.setkeyedStateManagedFuture(keyedStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
     }
}

// 汇报检查点完成
// RpcCheckpointResponder.java
@Override
public void acknowledgeCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
     // 向CheckpointCoordinator发送消息报告Checkpoint完成
     checkpointCoordinatorGateway.acknowledgeCheckpoint(jobID, executionAttemptID, checkpointId, checkpointMetrics, subtaskState);
}

// 确认检查点完成
// CheckpointCoordinator.java
// 响应消息，判断checkpoint是否完成或者丢弃
public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message, String taskManagerLocationInfo) throws CheckpointException {
     ......
     switch (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getSubtaskState(), message.getCheckpointMetrics())) {
          case SUCCESS:
              if (checkpoint.isFullyAcknowledged()) {
                  completePendingCheckpoint(checkpoint);
              }
              break;
          ......
     }
}

// 向Task发送检查点完成消息
private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws CheckpointException {
    ......
    for (ExecutionVertex ev : tasksToCommitTo) {
        Execution ee = ev.getCurrentExecutionAttempt();
        if (ee != null) {
            ee.notifyCheckpointComplete(checkpointId, timestamp);
        }
    }
}
```

### 检查点恢复过程

作业状态以算子为粒度进行恢复，包括OperatorState恢复、KeyedState恢复、函数State恢复。

**OperatorState恢复** 在初始化算子状态时，从OperatorStateStore中获取ListState类型的状态，由OperatorStateStore负责从对应的StateBackend中读取状态重新赋予算子中的状态变量

```Java
// 异步算子恢复状态示例
// AsyncWaitOperator中有一个名为"_async_wait_operator_state_"的状态，在算子初始化状态时，对其进行了恢复
// AsyncWaitOperator.java
@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    recoveredStreamElements = context
        .getOperatorStateStore()
        .getListState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
}
```

**KeyedState恢复**

```Java
// WindowOperator恢复KeyedState
// WindowOperator使用了KeyedState在StateBackend中保存窗口数据、定时器等
@Override
public void open() throws Exception {
    super.open();
    ......
    if (windowStateDescriptor != null) {
        windowState = (InternalAppendingState<K, W, IN, ACC, ACC>)getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
    }
}
```

**函数State恢复** 主要是针对有状态函数（继承了CheckpointedFunction接口或ListCheckpointed接口），在Flink内置的有状态函数主要是Source、Sink函数，为了支持端到端严格一次的情况，Source函数需要能够保存数据读取的断点位置，作业故障恢复后能够从断点位置开始读取。在Sink写出数据到外部存储时，有时也会需要恢复状态（如BucketingSink和TwoPhaseCommitSinkFunction）

```Java
// 恢复状态函数
// StreamingFunctionUtils.java
private static boolean tryRestoreFunction(StateInitializationContext context, Function userFunction) throws Exception {
    if (userFunction instanceof CheckpointedFunction) {
        ((CheckpointedFunction) userFunction).initializeState(context);
        return true;
    }
    ......
}
```

### 端到端严格一致

Flink设计实现了一种两阶段提交协议（分为预提交阶段和提交阶段），能够保证从读取、计算到写出整个过程的端到端严格一次。两阶段提交依赖于Flink的两阶段检查点机制，JobMaster触发检查点，所有算子完成各自快照备份即预提交阶段，在这个阶段Sink也要把待写出的数据备份到可靠存储中，确保不会丢失，向支持外部事务的存储项提交，当检查点的第一阶段完成之后，JobMaster确认检查点完成，此时Sink提交才真正写入外部存储

Flink为满足以下条件的Source/Sink提供端到端严格一次支持：
1. 数据源支持断点读取，即能够记录上次读取的位置，失败之后能够从断点处继续读取
2. 外部存储支持回滚机制或者满足幂等性，回滚机制指当作业失败之后能够将部分写入的结果回滚到写入之前的状态；幂等性指重复写入不会带来错误的结果

**预提交阶段** 在预提交阶段，Sink把要写入外部存储的数据以State的形式保存到StateBackend中，同时以事务的方式将数据写入外部存储，倘若在预提交阶段任何一个算子发生异常，导致检查点没有备份到StateBackend，所有其他算子的检查点也必须被终止，Flink回滚到最近成功完成的检查点
**提交阶段** 预提交阶段完成之后，通知所有的算子，确认检查点已成功完成，然后进入提交阶段，该阶段中JobMaster会为作业中每个算子发起检查点已完成的回调逻辑。在预提交阶段，数据实际上已经写入外部存储，但是因为事务原因是不可读的，但所有的Sink实例提交成功之后，一旦预提交完成，必须确保提交外部事务也要成功，此时算子和外部系统系统来保证。如果提交外部事务失败，Flink应用就会崩溃，然后根据用户重启策略进行回滚，回滚到预提交时的状态，之后再次重试提交

Flink抽取了两阶段提交协议公共逻辑并封装进TwoPhaseCommitSinkFunction抽象类，该类继承了CheckpointedFunction接口，在预提交阶段，能够通过检查点将待写出的数据可靠地存储起来；继承了CHeckpointListener接口，在提交阶段，能够接收JobMaster的确认通知，触发提交外部事务。

以基于文件的Sink为例，若要实现端到端严格一次，最重要的是以下4种方法：
1. beginTransaction：开启一个事务，在临时目录下创建一个临时文件，之后写入数据到该文件中，此过程为不同的事务创建隔离，避免数据混淆
2. preCommit：在预提交阶段，将缓存数据块写出到创建的临时文件，然后关闭该文件，确保不再写入新数据到该文件，同时开启一个新事务，执行属于下一个检查点的写入操作，此过程用于准备需要提交的数据，并且将不同事务的数据隔离开来
3. commit：在提交阶段，以原子操作的方式将上一阶段的文件写入真正的文件目录下。如果提交失败，Flink应用会重启，并调用`TwoPhaseCommitSinkFunction#recoverAndCommit`方法尝试恢复并重新提交事务^[两阶段提交可能会导致数据输出的延迟，即需要等待JobMaster确认检查点完成才会写入真正的文件目录]
4. abort：一旦终止事务，删除临时文件
