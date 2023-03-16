---
title: Flink作业提交、调度、执行
date: "2022-12-24"
description: Flink作业核心执行流程为DataStream API -> Transformation -> StreamGraph -> JobGraph -> ExecutionGraph -> 物理执行图
tags: Flink作业提交、Flink作业调度、Flink作业执行、算子融合
---

```toc
ordered: true
class-name: "table-of-contents"
```

Flink作业执行前需要提交Flink集群，Flink集群可以与不同的资源框架（Yarn、K8s、Mesos等）进行集成，可以按照不同的模式（Session模式、PerJob模式）运行。在Flink作业提交过程中，先在资源框架上启动Flink集群，接着提交作业，在Flink客户端中进行StreamGraph、JobGraph的转换，提交JobGraph到Flink集群，然后JobMaster将JobGraph转换为ExecutionGraph，之后进入调度执行阶段。在作业调度阶段，调度器根据调度模式选择对应的调度策略，申请所需要的资源，将作业发布到TaskManager上，启动作业执行。JobMaster调度作业的Task到TaskManager，所有的Task启动成功进入执行状态，则整个作业进入执行状态。作业开始消费数据（从外部数据源读取数据），执行业务逻辑，数据在Task DAG中流转，处理完毕后，写出到外部存储。在作业的整个执行过程中，涉及计算任务的提交、分发、管理和故障恢复（Failover）等。Flink作业真正执行起来之后，会在物理上构成Task相互连接的DAG，在执行过程中上游Task结果写入ResultPartition，ResultPartition又分成ResultSubPartition，下游的Task通过InputGate与上游建立数据传输通道，InputGate中的InputChannel对应于ResultSubPartition，将数据交给Task执行。Task执行的时候，根据数据的不同类型（StreamRecord、Watermark、LatencyMarker）进行不同的处理逻辑，处理完后再交给下游的Task。

Flink客户端在提交Flink应用时触发其main方法，用户使用DateStream API编写的业务逻辑组装成Transformation流水线，调用StreamExecutionEnvironment的execute()方法时触发StreamGraph的构建。概括来说，流计算应用经过DataStream API -> Transformation -> StreamGraph -> JobGraph -> ExecutionGraph转换过程后，经过Flink的调度执行，在Flink集群中启动计算任务，形成一个物理执行图。

## 图（Graph）

图有节点，节点之间有边相连。节点用来表示数据的处理逻辑，边用来表示数据处理的流转。从数据源读取数据开始，上游的数据处理完毕之后，交给下游继续处理，直到数据输出到外部存储中，整个过程用图来表示。

### 流图（StreamGraph）

StreamGraph与具体的执行无关，核心是表达计算过程的逻辑，由StreamNode和StreamEdge构成。StreamNode是StreamGraph中的节点，从Transformation转换而来，表示一个算子，可以有多个输入/输出，分为实体StreamNode和虚拟StreamNode，实体StreamNode会最终变成物理算子，虚拟StreamNode会附着在StreamEdge上。StreamEdge是StreamGraph中的边，用来连接两个StreamNode，包含了旁路输出、分区器、字段筛选输出等信息。

StreamGraph在Flink客户端生成，入口为StreamExecutionEnvironment的`getStreamGraph()`方法，实际在StreamGraphGenerator中生成。从SinkTransformation向前追溯到SourceTransformation，在遍历过程中一边遍历一边构建StreamGraph，在遍历Transformation的过程中，会对不同类型的Transformation分别进行转换。对于物理Transformation则转换为StreamNode实体，对于虚拟Transformation则作为虚拟StreamNode。针对具体某一种具体类型的Transformation，会调用其相应的`transformXxx()`方法进行转换。`transformXxx()`首先对上游Transformation进行递归转换，确保上游都已经完成了转换，然后通过`addOperator()`方法构造出StreamNode。通过`addEdge()`方法与上游进行连接，构造出StreamEdge。在构造StreamNode的过程中，运行时所需要的关键信息，如执行算子的容器类（StreamTask及其子类）和实例化算子的工厂（StreamOperatorFactory）也会确定下来，封装到StreamNode中。添加StreamEdge过程中，如果ShuffleNode为null，则使用ShuffleMode.PIPELINED模式，在构建StreeamEdge时，转换Transformation过程中生成的虚拟StreamNode会将虚拟StreamNode的信息附着在StreamEdge上。

```Java
// StreamGraphGenerator.java中负责具体的StreamGraph生成
public static StreamGraph generate(StreamExecutionEnvironment env, List<Transformation<?>> transformations) {
    return new StreamGraphGenerator(env).generateInternal(transformations);
}

// 实际执行StreamGraph生成
private StreamGraph generateInternal(List<Transformation<?>> transformations) {
    // 遍历Transformation集合，并对其每一个Transformation调用transform()方法
    for (Transformation<?> transformation : transformations) {
        transform(transformation);
    }
    return streamGraph;
}

private void transform(Transformation transformation) {
    if (transform instanceof OneInputTransformation<?, ?>) {
        transformedIds = transformOneInputTransform((OneInputTransformation<?, ?>)transform);
    } else if (transform instanceof TwoInputTransformation<?, ?, ?>) {
        transformedIds = transformTwoInputTransform((TwoInputTransformation<?, ?, ?>)transform);
    } else if (transform instanceof SourceTransforamtion<?>) {
        transformedIds = transformSource((SourceTransformation<?>)transform);
    }
    ...
}
```

**物理Transformation转换过程**：
1. 存储这个Transformation上游Transformation的id，用于构造边，并进行递归，确保所有上游Transformation都已经转化
2. 确定共享Slot组
3. 添加算子到StreamGraph中
4. 设置StateKeySeletor
5. 设置并行度、最大并行度
6. 构造StreamEdge的边，关联上下游StreamNode

    ```Java
    // 以OneInputTransformation示例
    <IN, OUT> Collection<Integer> transformOneInputTransform(OneInputTransformation<IN, OUT> transform) {
        Collection<Integer> inputIds = transform(transform.getInput());
    
        // 防止重复转换，如果已经转换过了则直接返回转换结果
        if (alreadyTransformed.containsKey(transform)) {
            return alreadyTransformed.get(transform);
        }
        // 确定Slot组
        String slotSharingGroup = determineSlotSharingGroup(transform.getSlotSharingGroup(), inputIds);
        // 添加StreamNode到StreamGraph中
        streamGraph.addOperator(
            transform.getId(),
            slotSharingGroup,
            transform.getCoLocationGroupKey(),
            transform.getOperatorFactory(),
            transform.getInputType(),
            transform.getOutputType(),
            transform.getName());
        if (transform.getStateKeySelector() != null) {
            TypeSerializer<?> keySerializer = transform.getStateKeyType().createSerializer(env.getConfig());
            streamGraph.setOneInputStateKey(transform.getId(), transform.getStateKeySelector(), keySerializer);
        }
    
        // 设定并行度
        streamGraph.setParallelism(transform.getId(), transform.getParallelism());
        streamGraph.setMaxParallelism(transform.getId(), transform.getMaxParallelism());
        // 添加StreamEdge，建立StreamNode之间的关联关系
        for (Integer inputId : inputIds) {
            streamGraph.addEdge(inputId, transform.getId(), 0);
        }
    
        return Collections.singleton(transform.getId());
    }
    ```

**虚拟Transformation转换过程** 不会转换为StreamNode，而是通过streamGraph的`addVirtualPartitionNode()`方法添加虚拟节点，当下游Transformation调用StreamGraph的`addEdge()`方法添加StreamEdge时，会把相关信息封装进StreamEdge中

    ```Java
    // 以PartitionTransformation为例
    private <T> Collection<Integer> transformPartition(PartitionTransfromation<T> partition) {
        Transformation<T> input = partition.getInput();
        List<Integer> resultIds = new ArrayList<>();
        // 递归对该transformation的直接上游进行转换
        Collection<Integer> transformedIds = transform(input);
        for (Integer transformedId : transformedIds) {
            int virtualId = Transformation.getNewNodeId();
            // 添加一个虚拟分区节点，不会生成StreamNode
            streamGraph.addVirtualPartitionNode(transformedId, virtualId, partition.getPartitioner());
            resultIds.add(virtualId);
        }
        return resultIds;
    }
    
    private void addEdgeInternal(Integer upStreamVertexID, Integer downStreamVertexID, int typeNumber, StreamPartitioner<?> partitioner, List<String> outputNames, OutputTag outputTag) {
        // 当上游时sideOutput时，递归调用，并传入sideOutput信息
        if（virtualSideOutputNodes.containsKey(upStreamVertexID)) {
            int virtualId = upStreamVertexID;
            upStreamVertexID = virtualSideOutputNodes.get(virtualId).f0;
            if (outputTag == null) {
                outputTag = virtualSideOutputNodes.get(virtualId).f1;
            }
            addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, null, outputTag);
        }
        // 当上游是select时，递归调用，并传入select信息
        else if (virtualSelectNodes.containsKey(upStreamVertexID)) {
            int virtualId = upStreamVertexID;
            upStreamVertexID = virtualSelectNodes.get(virtualId).f0;
            if (outputNames.isEmpty()) {
                outputNames = virtualSelectNodes.get(virtualId).f1;
            }
            addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag);
        }
        // 当上游是partition时，递归调用，并传入partitioner信息
        else if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
            int virtualId = upStreamVertexID;
            upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
            if (partitioner == null) {
                partitioner = virtualPartitionNodes.get(virtualId).f1;
            }
            addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, pertitioner, outputNames, outputTag);
        }
        // 不是以上逻辑转换的情况，真正构建StreamEdge
        else {
            StreamNode upStramNode = getStreamNode(upStreamVertexID);
            StreamNode downStreamNode = getStreamNode(downStreamVertexID);
            // 没有指定partitioner时，会为其选择forward或者rebalance分区
            if (partitioner == null && upStreamNode.getParallelism() == downStreamNode.getParallelism()) {
                partitioner = new ForwardPartitioner<Object>();
            } else if (partitioner == null) {
                partitioner = new RebalancePartitioner<Object>();
            }
            // 创建StreamEdge，并将该StreamEdge添加到上游的输出，下游的输入
            StreamEdge edge = new StreamEdge(upStreamNode, downStreamNode, typeNumber, outputNames, partitioner, outputTag);
            getStreamNode(edge.getSourceId()).addOutEdge(edge);
            getStreamNode(edge.getTargerId()).addInEdge(edge);
        }
    }
    ```

### 作业图（JobGraph）

JobGraph在StreamGraph的基础上进行了一些优化（如通过OperatorChain机制将算子合并起来，在执行时调度在同一个Task线程上，避免数据的跨线程、跨网络的传递），由JobVertex、JobEdge和IntermediateDataSet组成。 JobVertex是JobGraph中的节点，包含一个或多个算子，输入是JobEdge，输出是IntermediateDataSet。JobEdge是JobGraph中的边，表示一个数据流转通道，连接了上游生产的中间数据集IntermediateDataSet和下游消费者JobVertex。JobEdge中的数据分发模式会直接影响执行时Task之间的数据连接关系，是点对点连接还是全连接。 IntermediateDataSet是一种逻辑结构，用来表示JobVertex的输出。IntermediateDataSet的个数与该JobVertex对应的StreamNode的出边数量相同，可以是一个或多个。

JobGraph的生成入口在StreamGraph中，StreamingJobGraphGenerator负责流计算JobGraph的生成，在转换前需要进行一系列的预处理，之后开始构建JobGraph中的点和边，从Source开始，向下递归遍历StreamGraph，执行具体的Chain和JobVertex生成、JobEdge关联、IntermediateDataSet，逐步创建JobGraph，在创建的过程中同时完成算子融合（OperatorChain）优化。构建JobVertex时需要将StreamNode中的重要配置信息复制到JobVertex中，之后 ，构建JobEdge将JobVertex连接起来（内部算子之间无须构建JobEdge进行连接）。构建JobEdge时很重要一点是确定上游JobVertex和下游JobVertex的数据交换方式，根据ShuffleMode来确定ResultPartition的类型（在执行算子写出数据和数据交换中使用），ShuffleMode确定了ResultPartition，也就确定了上游JobVertex输出的IntermediateDataSet类型，也就知道该JobEdge的输入IntermediateDataSet了。ForwardPartitioner和RescalePartitioner两种Partitioner转换为DistributionPattern.POINTWISE的分发模式，其它类型的Partitioner统一转换为DistributionPattern.ALL_TO_ALL模式。

```Java
private JobGraph createJobGraph() {
    // 设置调度模式
    jobGraph.setScheduleMode(streamGraph.getScheduleMode());
    // 为每个节点生成确定的hash id作为唯一标识，在提交和执行过程中保持不变
    Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);
    Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();
    // 真正对StreamGraph进行转换，生成JobGraph图
    setChaining(hashes, legacyHashes, chainedOperatorHashes);
    setPhysicalEdges();
    // 设置共享slotGroup
    setSlotSharingAndCoLocation();
    // 配置Checkpoint
    configureCheckpointing();
    // 如果有之前的缓存文件的配置，则重新读入
    JobGraphGenerator.addUserArtifactEntries(streamGraph.getEnvironment().getCachedFiles(), jobGraph);
    // 设置执行环境配置
    jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
    return jobGraph;
}

private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
    // 从所有Source节点开始生成OperatorChain
    for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
        createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
    }
}

private List<StreamEdge> createChain(Integer startNodeId, Integer currentNodeId, Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, int chainIndex, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
    if (!buildVertices.contains(startNodeId)) {
        List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();
        List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
        List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();
        // 获取当前节点的出边，判断边是否符合OperatorChain的条件
        // 分为两类：chainableOutputs, nonChainableOutputs
        for (StreamEdge outEdge : streamGraph.getStreamNode(currentNodeId).getOutEdges()) {
            if (isChainable(outEdge, streamGraph)) {
                chainableOutputs.add(outEdge);
            } else {
                nonChainableOutputs.add(outEdge);
            }
        }
        // 对于chainable的边，递归调用createChain
        // 返回值添加到transitiveOUtEdges中
        for (StreamEdge chainable : chainableOutputs) {
            transitiveOutEdges.addAll(createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex+1, chainedOperatorHashes));
        }
        // 对于无法chain在一起的边，边的下游节点作为OperatorChain的Head节点
        // 进行递归调用，返回值添加到transitiveOutEdges中
        for (StreamEdge nonChainable : nonChainableOutputs) {
            transitiveOutEdges.add(nonChainable);
            createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
        }
        List<Tuple2<byte[], byte[]>> operatorHashes = chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());
        byte[] primaryHashBytes = hashes.get(currentNodeId);
        for (Map<Integer, byte[]> legacyHahs : legacyHashes) {
            operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
        }
        chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
        chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
        chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));
        // 如果当前节点时起始节点，则直接创建JobVertex，否则返回一个空的StreamConfig
        StreamConfig config = currentNodeId.equals(startNodeId) ? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes) : new StreamConfig(new Configuration());
        // 将StreamNode中的配置信息序列化到StreamConfig中
        setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);
        // 再次判断，如果是chain的起始节点，执行connect()方法，创建JobEdge和IntermediateDataSet
        // 否则将当前节点的StreamConfig添加到chainedConfig中
        if (currentNodeId.equals(startNodeId)) {
            config.setChainStart();
            config.setChainIndex(0);
            config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
            config.setOutEdgesInOrder(transitiveOutEdges);
            config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());
            for (StreamEdge edge : transitiveOutEdges) {
                connect(startNodeId, edge);
            }
            config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));
        } else {
            chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());
            config.setChainIndex(chainIndex);
            StreamNode node = streamGraph.getStreamNode(currentNodeId);
            config.setOperatorName(node.getOperatorName());
            chainedConfigs.get(startNodeId).put(currentNodeId, config);
        }
        config.setOperatorId(new OperatorId(primaryHashBytes));
        if (chainableOutputs.isEmpty()) {
            config.setChainEnd();
        }
        return transitiveOutEdges;
    } else {
        return new ArrayList<>();
    }
}

```

**算子融合** 为了更高效地实现分布式执行，Flink会尽可能地将多个算子融合在一起，形成一个OperatorChain，一个OperatorChain在同一个Task线程内执行。OperatorChain内的算子之间，在同一个线程内通过方法调用地方式传递数据，能减少线程之间地切换，减少消息的序列化/反序列化，无需借助内存缓冲区，也无需通过网络在算子间传递数据，可在减少延迟的同时提供整体的吞吐量。形成OperatorCahin必须具备以下9个条件：
1. 下游节点的入边为1
2. StreamEdge的下游节点对应的算子不为null
3. StreamEdge的上游节点对应的算子不为null
4. StreamEdge的上下游节点拥有相同的slotSharingGroup，默认都是default
5. 下游算子的连接策略为ALWAYS
6. 上游算子的连接策略为ALWAYS或HEAD
7. StreamEdge的分区类型为ForwardPartitioner
8. 上下游节点的并行度一致
9. 当前StreamGraph允许chain

### 执行图（ExecutionGraph）

ExecutionGraph是调度Flink作业执行的核心数据结构，包含了作业中所有并行执行的Task的信息、Task之间的关联关系、数据流转关系。由ExecutionJobVertex、ExecutionVertex、IntermediateResult、IntermediateResultPartition、ExecutionEdge和Execution组成。JobGraph到ExecutionGraph的转换在JobMaster中完成，转换过程中的重要变化如下：
1. 加入了并行度的概念，成为真正可调度的图结构
2. 生成与JobVertex对应的ExecutionJobVertex和ExecutionVertex，与IntermediateDataSet对应的IntermediateResult和IntermediateResultPartition等，并行将通过这些类实现

ExecutionJobVertex和JobVertex一一对应，包含一组ExecutionVertex，数量与该JobVertex中所包含的StreamNode的并行度一致。ExecutionJobVertex用来将一个JobVertex封装成ExecutionJobVertex，并依次创建ExecutionVertex、Execution、IntermediateResult和IntermediateResultPartition，用于丰富ExecutionGraph。在ExecutionJobVertex的构造函数中，首先是依据对应的JobVertex的并行度，生成对应个数的ExecutionVertex，其中，一个ExecutionVertex代表一个ExecutionJobVertex的并发子Task，然后是将原来JobVertex的中间结果IntermediateDataSet转化为ExecutionGraph中的IntermediateResult。

IntermediateResult（中间结果集）是个逻辑概念，标识ExecutionJobVertex的输出，和JobGraph中的IntermediateDataSet一一对应。一个ExecutionJobVertex可以有多个中间结果，取决于当前JobVertex有几个出边。一个IntermediateResult包含多个IntermediateResultPartition（中间结果集分区），其个数等于该JobVertex的并发度（或算子的并行度）。

IntermediateResultPartition（中间结果分集区）表示1个ExecutionVertex输出结果，与ExecutionEdge相关联。

ExecutionEdge表示ExecutionVertex的输入，连接到上游产生的IntermediateResultPartition。

Execution表示一个ExecutionVertex的一次尝试。ExecutionVertex相当于每个Task的模板，在真正执行时会将ExecutionVertex中的信息包装为1个Execution。JobManager和TaskManager之间关于Task的部署和Task执行状态的更新都是通过ExecutionAttemptID来标识实例的。在发生故障或者数据需要重算的情况下，ExecutionVertex可能会有多个ExecutionAttemptID，一个Execution通过ExecutionAttemptID来唯一标识。

ExecutionGraph生成的核心入口在ExecutionGraphBuilder中，从JobGraph向ExecutionGraph转换的核心逻辑中主要完成两件事情：
1. 构造ExecutionGraph的节点，将JobVertex封装成ExecutionJobVertex。Flink作业变成并行Task的逻辑隐含在ExecutionJobVertex的构造函数中，在该构造函数中生成了一组ExecutionVertex，数量与并行度一致
    1. 设置并行度
    2. 设置Slot共享和CoLocationGroup
    3. 构建当前ExecutionJobVertex的IntermediateResult及其IntermediateResultPartition，每对应一个下游JobEdge，创建一个中间结果
    4. 构建ExecutionVertex，根据该ExecutionJobVertex的并行度，创建对应数量的ExecutionVertex，在运行时刻会部署相同数量的Task到TaskManager
    5. 检查IntermediateResultPartition和ExecutionVertex之间有没有重复的引用
    6. 对可切分的数据源进行输入切分（InputSplit）
2. 构造ExecutionGraph的数据交换关系ExecutionEdge，建立ExecutionGraph的节点之间的相互联系，把节点通过ExecutionEdge连接。在创建ExecutionJobVertex的过程中，调用`ejv.connectToPredecessor()`方法，创建ExecutionEdge将ExecutionVertex和IntermediateResult关联起来，运行时建立Task之间的数据交换就是以此为基础建立数据的物理传输通道的。连接过程中，根据JobEdge的DistributionPattern属性创建ExecutionEdge，将ExecutionVertex和上游IntermediateResultPartition连接起来。连接策略有两种DistributionPattern.POINTWISE（点对点连接）和DistributionPattern.ALL_TO_ALL（全连接）。

点对点连接策略（DistributionPattern.POINTWISE）一共分为三种情况（假设上游IntermediateResult分区数记做numSources，ExecutionJobVertex的并发度记做parallelism）：
1. 一对一连接：numSources == parallism，即并发的Task数量与分区数相等时进行一对一连接
2. 多对一连接：parallism < numSources，即下游Task数量小于上游的分区数时进行多对一连接
3. 一对多连接：parallism > numSources，即下游Task数量多于上游的分区数时进行一对多连接
全连接策略（DistributionPattern.ALL_TO_ALL）下游的ExecutionVertex与上游的所有IntermediateResultPartition建立连接，消费其产生的数据，一般意味着数据在Shuffle

## 作业提交

Flink作业提交分为两大步骤：
1. 在Flink Client中通过反射启动Jar中的main函数，生成Flink StreamGraph、JobGraph，将JobGraph提交给Flink集群
2. Flink集群收到JobGraph后，将JobGraph翻译成ExecutionGraph，然后开始调度执行，启动成功之后开始消费数据

根据Flink Client提交作业之后是否退出Client进程，提交模式分为Detached模式和Attached模式。Detached模式下，Flink Client创建完集群之后，可以退出命令行窗口，集群独立运行。Attached模式下，Flink Client创建完集群后，不能关闭命令行窗口，需要与集群之间维持连接。

Flink作业提交入口是ClientFrontend，触发用户开发的Flink应用Jar文件中的main方法，然后交给`PipelineExecutor#execute()`方法，最终会选择触发一个具体的流水线执行器（PipelineExecutor）执行。Flink集群有Session模式和PerJob模式，分别对应AbstractSessionClusterExecutor和AbstractJobClusterExecutor，另外，在IDE环境中运行Flink MiniCluster进行调试时，使用LocalExecutor。

Session模式下，作业共享Dispatcher、ResourceManager、集群资源，作业通过Http协议进行提交，适合执行时间短，频繁执行的短任务。

**Yarn Session提交流程** 分为3个阶段：
1. 启动集群：
    1. 使用bin/yarn-session.sh提交会话模式的作业。如果提交到已经存在的集群，则获取Yarn集群信息、应用ID，并准备提交作业。否则启动新的Yarn Session集群
    2. Yarn启动新Flink Session模式的集群。首先将应用配置文件（flink-conf.yaml、logback.xml、log4j.properties）和相关文件（Flink Jar、配置类文件、用户Jar文件、JobGraph对象等）上传至分布式存储的应用暂存目录。通过Yarn Client向Yarn提交Flink创建集群的申请，Yarn分配资源，在申请的Yarn Container中初始化并启动Flink JobManager进程，在JobManager进程中运行YarnSessionClusterEntrypoint作为集群启动的入口，初始化Dispatcher、ResourceManager，启动相关的RPC服务，等待Client通过Rest接口提交作业
2. 作业提交
    1. Flink Client通过Rest向Dispatcher提交JobGraph
    2. Dispathcer^[不负责实际的调度、执行方面的工作]收到JobGraph后，为作业创建一个JobManager，将工作交给JobMaster（负责作业调度、管理作业和Task生命周期），构建ExecutionGraph
3. 作业调度执行
    1. JobMaster向YarnResourceManager申请资源，开始调度ExecutionGraph执行，向YarnResourceManager申请资源；初次提交作业集群中尚没有TaskManager，此时资源不足，开始申请资源
    2. YarnResourceManager收到JobMaster的资源请求，如果当前有空闲Slot则将Slot分配给JobMaster，否则YarnResourceManager将向Yarn Master请求创建TaskManager
    3. YarnResourceManager将资源请求加入等待请求队列，并通过心跳向YARN RM申请新的Container资源来启动TaskManager进程；Yarn分配新的Container给TaskManager
    4. YarnResourceManager启动，然后从HDFS加载Jar文件等所需的相关资源，在容器中启动TaskManager
    5. TaskManager启动之后，向ResourceManager注册，并把自己的Slot资源情况汇报给ResourceManager
    6. ResouceManager从等待队列中取出Slot请求，向TaskManager确认资源可用情况并告知TaskManager将Slot分配给了哪个JobMaster
    7. TaskManager向JobMaster提供Slot，JobMaster调度Task到TaskManager的此Slot上执行

PerJob模式下，作业独享Dispatcher、ResourceManager、集群资源，作业之间相互隔离，适合长周期执行的任务，集群异常影响范围小。

**Yarn PerJob提交流程**分为3个阶段：
1. 启动集群
    1. 使用`./flink run -m yarn-cluster`提交PerJob模式的作业
    2. Yarn启动Flink集群。首先将应用配置文件（flink-conf.yaml、logback.xml、log4j.properties）和相关文件（Flink Jar、配置类文件、用户Jar文件、JobGraph对象等）上传至分布式存储的应用暂存目录。通过Yarn Client向Yarn提交Flink创建集群的申请，Yarn分配资源，在申请的Yarn Container中初始化并启动Flink JobManager进程，在JobManager进程中运行YarnJobClusterEntrypoint作为集群启动的入口，初始化Dispatcher、ResourceManager，启动相关的RPC服务，等待Client通过Rest接口提交作业
2. 作业提交：Dispatcher从本地文件系统获取JobGraph，为作业创建一个JobManager，将工作交给JobMaster（负责作业调度、管理作业和Task生命周期），构建ExecutionGraph
3. 作业调度执行
    1. JobMaster向YarnResourceManager申请资源，开始调度ExecutionGraph执行，向YarnResourceManager申请资源；初次提交作业集群中尚没有TaskManager，此时资源不足，开始申请资源
    2. YarnResourceManager收到JobMaster的资源请求，如果当前有空闲Slot则将Slot分配给JobMaster，否则YarnResourceManager将向Yarn Master请求创建TaskManager
    3. YarnResourceManager将资源请求加入等待请求队列，并通过心跳向YARN RM申请新的Container资源来启动TaskManager进程；Yarn分配新的Container给TaskManager
    4. YarnResourceManager启动，然后从HDFS加载Jar文件等所需的相关资源，在容器中启动TaskManager
    5. TaskManager启动之后，向ResourceManager注册，并把自己的Slot资源情况汇报给ResourceManager
    6. ResouceManager从等待队列中取出Slot请求，向TaskManager确认资源可用情况并告知TaskManager将Slot分配给了哪个JobMaster
    7. TaskManager向JobMaster提供Slot，JobMaster调度Task到TaskManager的此Slot上执行

## 作业调度

在调度相关的体系中有几个非常重要的组件：
+ 调度器：SchedulerNG及其子类、实现类
+ 调度策略：SchedulingStrategy及其实现类
+ 调度模式：ScheduleMode包含流和批的调度，有各自不同的调度模式

**调度器** 是Flink作业执行的核心组件，管理作业执行的所有相关过程：
1. JobGraph到ExecutionGraph的转换
2. 作业生命周期管理（作业的发布、取消、停止）
3. 作业的Task生命周期管理（Task的发布、取消、停止）
4. 作业执行资源的申请、分配、释放
5. 作业和Task的状态管理
6. 对外提供作业的详细信息

SchedulerNG接口中定义了调度器的行为模型：

```Java
public interface SchedulerNG {
    void setMainThreadExecutor(ComponentMainThreadExecutormainThreadExecutor);
    void registerJobStatusListener(JobStatusListener jobStatusListener);
    void startScheduling();
    void suspend(Throwable cause);
    void cancel();
    completableFuture<Void> getTerminationFuture();
    void handleGlobalFailure(Throwable cause);
    boolean updateTaskExecutionState(TaskExecutionState taskExecutionState);
    SerializedInputSplit requestNextInputSplit(JobVertexId vertexID, ExecutionAttemptID executionAttempt) throws IOException;
    ExecutionState requestPartitionState(IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId) throws PartitionProducerDisposedException;
    void scheduleOrUpdateConsumers(ResultPartitionID partitionID);
    ArchivedExecutionGraph requestJob();
    JobStatus requestJobStatus();
    JobDetails requestJobDetails();
}
```

Scheduler接口只负责Slot资源分配，其所有方法都是跟Slot相关的，在SchedulerNG中使用Scheduler来申请和归还Slot。

在Flink中有两个调度器的实现：DefaultScheduler和LegacyScheduler^[遗留调度器]。DefaultScheduler是新的默认调度器，使用SchedulerStrategy来实现调度。

**调度策略** 对应SchedulingStrategy接口，有EagerSchedulingStrategy和LazyFromSourceSchedulingStrategy两种实现。EagerSchedulingStrategy用来执行流计算作业的调度，一次性获取所有需要的Slot，部署Task并开始执行；LazyFromSourceSchedulingStrategy用来执行批处理作业的调度，分阶段调度执行，上一阶段执行完毕，数据可消费时，开始调度下游的执行。SchedulingStrategy接口中定义了4种行为：
1. startScheduling：调度入口，触发调度器的调度行为
2. restartTasks：重启执行失败的Task
3. onExecutionStateChange：当Execution的状态发生改变时
4. onPartitionConsumable：当IntermediateResultPartition中的数据可以消费时

**调度模式** 有3中调度模式，适用于不同的场景：
+ Eager调度：适用于流计算，一次性申请所有需要的资源，如果资源不足，则作业启动失败
+ 分阶段调度（Lazy_From_Source）：适用于批处理，从Source Task开始分阶段调度，申请资源的时候，一次性申请本阶段所需要的所有资源，上游Task执行完毕后开始调度执行下游的Task，读取上游的数据，执行本阶段的计算任务，执行完毕之后，调度后一个阶段的Task，依次进行调度，指导作业执行完成
+ 分阶段Slot重用调度（Lazy_From_Source_With_Batch_Slot_Request）：适用于批处理，与分阶段调度基本一致，区别在于该模式下使用批处理资源申请模式，可以在资源不足的情况下执行作业，但是需要确保在本阶段的作业执行中没有Shuffle行为

**执行模式** 流计算作业的数据执行模式是推送的模式，Flink在底层统一了批流作业的执行，提供了两类执行模式：流水线（Pipelined）模式和批（Batch）模式，又可细分为5种具体的执行模式：
1. 流水线模式（Pipelined）：以流水线方式执行作业，如果可能会出现数据交换死锁^[当数据流被多个下游分支消费处理，处理后的结果再进行Join时，如果以Pipelined模式运行，则可能出现数据交换死锁]，则数据交换以Batch方式执行
2. 强制流水线模式（Pipelined_forced）：以流水线方式执行作业，即便流水线可能会出现死锁的数据交换时仍然运行^[一般情况下，Pipelined模式是优先选择，确保不会出现数据死锁的情况下才会使用Pipelined_Forced模式]
3. 流水线优先模式（Pipelined_With_Batch_Fallback）：首先使用Pipelined启动作业，如果可能出现死锁则使用Pipelined_Forced启动作业，当作业异常退出时，则使用Batch模式重新执行作业
4. 批处理模式（Batch）：对于所有的Shuffle和Broadcast都适用Batch模式执行，仅本地的数据交换使用Pipelined模式
5. 强制批处理模式（Batch_Forced）：对于所有的数据交换都使用Batch模式

```Java
public enum ExecutionMode {
    PIPELINED,
    PIPELINED_FORCED,
    PIPELINED_WITH_BATCH_FALLBACK,
    BATCH,
    BATCH_FORCED
}
```

**数据交换模式** 执行模式的不同决定了数据交换行为的不同，Flink在ResultPartitonType中定义了4种类型的数据分区模式，与执行模式一起完成批流在数据交换层面的统一：
1. BLOCKING：BLOCKING类型的数据分区会等待数据完全处理完毕，然后才会交给下游进行处理，在上游处理完毕之前，不会与下游进行数据交换。该类型的数据分区可以被多次消费，也可以并发消费。被消费完毕之后不会自动释放，而是等待调度器来判断该数据分区无人再消费之后，由调度器发出销毁指令。该模式适用于批处理，不提供反压流控能力
2. BLOCKING_PERSISTENT：BLOCKING_PERSISTENT类型的数据分区类似于BLOCKING，但是其生命周期由用户指定，调用JobManager或者ResourceManager API进行销毁，而不是由调度器控制
3. PIPELINED：PIPELINED类型的数据分区数据处理结果只能被1个下游的算子消费1次，当数据被消费之后即自动销毁，可以在运行中保留任意数量的数据，当数据量太大内存无法容纳时，可以写入磁盘中。该模式适用于流计算和批处理
4. PIPELINED_BOUNDED：等价于带有一个有限大小本地缓冲池的PIPELINED，对于流计算作业，固定大小的缓冲池可以避免缓冲太多的数据和检查点延迟太久

**JobMaster** 负责单个作业的管理，提供了对作业的管理行为，允许通过外部命令干预作业的运行，同时也维护了整个作业及其Task的状态，对外提供对作业状态的查询能力。JobMaster保存了对作业执行至关重要的状态和数据，JobGraph、用户的Jar包、配置文件、检查点数据等保存在配置的分布式可靠存储中（一般使用HDFS），检查点访问信息保存在ZooKeeper中
1. 调度执行和管理：接收JobGraph并将其转换为ExecutionGraph，调度Task的执行，并处理Task的异常，进行作业恢复或者中止，根据TaskManager汇报的状态维护ExecutionGraph
    + InputSplit分配：批处理中使用，为批处理计算任务分配待计算的数据分片
    + 结果分区跟踪（PartitionTracker）：跟踪非Pipelined模式的分区（即批处理中的结果分区），当结果分区消费完之后，具备结果分区释放条件时，向TaskExecutor和ShuffleMaster发出释放请求
    + 作业执行异常时，选择重启作业或停止作业
2. 作业Slot资源管理：Slot资源的申请、持有和释放，将具体的管理动作交给SlotPool来执行，SlotPool持有资源，资源不足时负责与ResourceManager交互申请资源。作业停止、闲置TM、TM心跳超时时释放TaskManager
3. 检查点与保存点：CheckpointCoordinator负责进行检查点的发起、完成确认，检查点异常或者重复时取消本次检查点的执行，保存点手动触发或接口调用触发
4. 监控运维相关：反压跟踪、作业状态、作业各算子的吞吐量等监控指标
5. 心跳管理：JobMaster、ResourceManager、TaskManager是3个分布式组件，相互之间通过网络进行通信，三者之间通过两两心跳相互感知对方，一旦出现心跳超时，则进入异常处理阶段，或是进行切换，或是进行资源清理

**TaskManager** 是集群的计算执行者，负责执行计算任务，其上执行了一个或多个Task，实现类是TaskExecutor。TaskExecutor对外与JobManager、ResourceManager通信，对内需要管理Task及其相关的资源、结果分区等。TaskManager是Task的载体，负责启动、执行、取消Task，并在Task异常时向JobManager汇报，TaskManager作为Task执行者，为Task之间的数据交换提供基础框架。从集群资源管理的角度，TaskManager是计算资源的载体，一个TaskManager通过Slot切分其CPU、内存等计算资源；从实现Exactly-Once角度，JobManager是检查点的协调管理者，TaskManager是检查点的执行者；从集群管理角度，TaskManager与JobMaster之间通过心跳相互感知，与ResourceManager保持心跳，汇报资源的使用情况，以便ResourceManager能够掌握全局资源的分布和剩余情况^[集群内部的信息交换基于Flink的RPC通信框架]。TaskManager提供的数据交换基础框架，最重要的是跨网络的数据交换、内存资源的申请和分配以及其它需要在计算过程中Task共享的组件

**Task** 是Flink作业的子任务，由TaskManger直接负责管理调度，为StreamTask执行业务逻辑的时候提供基础的组件，如内存管理器、IO管理器、输入网关、文件缓存等。Flink中流执行层面使用StreamTask体系，批处理执行层面使用BatchTask体系，两套体系互不相通，通过Task可以解耦TaskManager，使得TaskManager无须关心计算任务是流计算作业还是批处理作业。Task执行所需要的核心组件如下：
+ TaskStateManager：负责State的整体协调，封装了CheckpointResponder，在StreamTask中用来跟JobMaster交互，汇报检查点的状态
+ MemoryManager：Task通过该组件申请和释放内存
+ LibraryCacheManager：Flink作业Jar包提交给Flink集群，在Task启动时，从此组件远程下载所需的Jar文件，在Task的类加载器中加载，然后才能够执行业务逻辑
+ InputSplitProvider：在数据源算子中，用来向JobMaster请求分配数据集的分片，然后读取该分片的数据
+ ResultPartitionConsumableNotifier（结果分区可消费通知器）：用于通知消费者生产者生产的结果分区可消费
+ PartitionProducerStateChecker（分区状态检查器）：用于检查生产端分区状态
+ TaskLocalStateStore：在TaskManager本地提供State的存储，恢复作业的时候，优先从本地恢复，提高恢复速度
+ IOManager（IO管理器）：负责将数据溢出到磁盘，并在需要时将其读取回来
+ ShuffleEnvironment（数据交换管理环境）：包含了数据写出、数据分区的管理等组件
+ BroadcastVariableManager（广播变量管理器）：Task可以共享该管理器，通过引用计数跟踪广播变量的使用，没有使用的时候则清除
+ TaskEventDispatcher（任务事件分发器）：从消费者任务分发事件给生产者任务

```bob-svg
                              .------------------.
                              | TaskManager      |
                              | .--------------. |
.-----------. TCP Connection  | | Task         | | TCP Connection  .-------------.
|TaskManager|<--------------->| | .------------+ +<--------------->| TaskManager |
'-----------'                 | | | StreamTask | |                 '-------------'
                              | | | .----------+ |
                              | | | | Operator | |
                              | '-+-+----------' |
                              '------------------'
```

**StreamTask** 是所有流计算作业子任务的执行逻辑的抽象基类，是算子的执行容器。StreamTask的类型与算子类型一一对应，其实现分为以下几类：
+ TwoInputStreamTask：两个输入的StreamTask，对应于TwoInputStreamOperator
+ OneInputStreamTask：单个输入的StreamTask，对应于OneInputStreamOperator，其两个子类StreamIterationHead和StreamIterationTail用来执行迭代计算
+ SourceStreamTask：用在流模式的执行数据读取的StreamTask
+ BoundedStreamTask：用在模拟批处理的数据读取行为
+ SourceReaderStreamTask：用来执行SourceReaderStreamOperator
StreamTask的生命周期有3个阶段：
1. 初始化阶段
    1. StateBackend初始化^[StateBackend是实现有状态计算和Exactly-Once的关键组件]
    2. 时间服务初始化
    3. 构建OperatorChain，实例化各个算子
    4. 初始化Task，根据Task类型的不同，其初始化略有不同
        + 对于SourceStreamTask，主要是启动SourceFunction开始读取数据，如果支持检查点，则开启检查点
        + 对于OneInputStreamTask和TwoInputStreamTask，构建InputGate，包装到SteamTask的输入组件StreamTaskNetworkInput中，从上游StreamTask读取数据，构建Task的输出组件StreamTaskNetworkOutput^[StreamTask之间的数据传递关系由下游StreamTask负责建立数据传递通道，上游StreamTask只负责写入内存]，然后初始化SteamInputProcessor，将输入（StreamTaskNetworkInput）、算子处理数据、输出（StreamTaskNetworkOUtput）关联起来，形成StreamTask的数据处理的完整通道。之后设置监控指标，使之在运行时能够将各种监控数据与监控模块打通
    5. 对OperatorChain中所有算子恢复状态，如果作业是从快照恢复的，就把算子恢复到上一次保存的快照状态，如果是无状态算子或者作业第一次执行，则无需恢复
    6. 算子状态恢复之后，启动算子，将UDF函数加载、初始化进入执行状态
2. 运行阶段：初始化StreamTask进入运行状态，StreamInputProcessor持续读取数据，交给算子执行作业业务逻辑，然后输出
3. 关闭与清理阶段：当作业取消、异常时，中止当前的StreamTask的执行，StreamTask进入关闭与清理阶段
    1. 管理OperatorChain中的所有算子，同时不再接收新的Timer定时器，处理完剩余的数据，将算子的数据强制清理
    2. 销毁算子，关闭StateBackend和UDF
    3. 通用清理，停止相关的执行线程
    4. Task清理，关闭StreamInputProcessor，本质上是关闭了StreamTaskInput，清理InputGate、释放序列化器

### 作业生命周期

Flink在JobMaster中有作业级别的状态管理，ExecutionGraph中有单个Task的状态管理。

在Flink中使用有限状态机管理作业的状态，作业状态在JobStatus中定义：
+ Created状态：作业刚被创建，还没有Task开始执行
+ Running状态：作业创建完之后，开始申请资源，申请必要的资源成功，并且向TaskManager调度Task执行成功，就进入Running状态
+ Restarting状态：当作业执行出错，需要重启作业时，首先进入Failing状态，如果可以重启则进入Restarting状态，作业进行重置，释放所申请的所有资源，包括内存、Sllot等，开始重新调度作业的执行
+ Cancelling状态：调用Flink的接口或者在WebUI上对作业进行取消，首先会进入此状态，此状态下，首先会清理资源，等待作业的Task停止
+ Canceled状态：当所有的资源清理完毕，作业完全停止执行后，进入Canceled状态，此状态一般是用户主动停止作业
+ Suspended状态：挂起作业之前，取消Running状态的Task，Task进入Canceled状态，销毁掉通信组件等其他组件，但是仍然保留ExecutionGraph，等待恢复。Suspended状态一般在HA下主JobManager宕机、备JobManager接管继续执行时，恢复ExecutionGraph
+ Finished状态：作业的所有Task都成功地执行完毕后，作业退出，进入Finished状态
+ Failing状态：作业执行失败，进入Failing状态，等待资源清理
+ Failed状态：作业进入Failing状态的异常达到作业自动重启次数的限制，或其他更严重的异常导致作业无法自动恢复时，作业进入Failed状态

有限状态机（FSM，Finite State Machine）是表示有限个状态及在这些状态之间转移和动作等行为的数学模型。状态机可归纳为4个要素，即现态、条件、动作、次态，其中，现态和条件是因，动作和次态是果
+ 现态：当前所处的状态
+ 条件：又称为事件，当一个条件被满足时，将会触发一个动作，或者执行一次状态的迁移
+ 动作：条件满足后执行的动作
+ 次态：条件满足后要迁往的心状态

### Task生命周期

TaskManaer负责Task的生命周期管理，并将状态的变化通知到JobMaster，在ExecutionGraph中跟踪Execution的状态变化，一个Execution对应于一个Task。作业生命周期的状态本质上来说就是各个Task状态的汇总。作业被调度执行发布到各个TaskManager上开始执行，Task在其整个生命周期中有8种状态，定义在ExecutionState中^[Created是起始状态，Failed、Finished、Canceled状态是Final状态]：
1. Ctreated状态：ExecutionGraph创建出来之后，Execution默认的状态就是Created
2. Scheduled状态：表示被调度执行的Task进入Scheduled状态，然后开始申请所需的计算资源
3. Deploying状态：表示资源申请完毕，向TaskManager部署Task
4. Running状态：TaskManager启动Task，并通知JobManager该Task进入Running状态，JobManager将该Task所在的ExecutionGraph中的Execution设置为Finished状态
5. Finished状态：当Task执行完毕，没有异常，则进入Finished状态，JobManager将该Task所在的ExecutionGraph中的Execution设置为Finished状态
6. Cancelling状态：是ExecutionGraph中维护的一种状态，表示正在取消Task执行，等待TaskManager取消Task的执行，并返回结果
7. Cancled状态：TaskManager取消Task执行成功，并通知JobManager，JobManager将该Task所在的ExecutionGraph中对应的Execution设置为Canceled状态
8. Failed状态：若TaskManager执行Task时出现异常导致Task无法继续执行，Task会进入Failed状态，并通知JobManager，JobManager将该Task所在的ExecutionGraph中对应的Execution设置为Failed状态，整个作业也将会进入Failed状态

### 作业启动

Flink作业被提交之后，JobManager中会为每个作业启动一个JobMaster，并将剩余的工作交给JobMaster，JobMaster负责整个作业生命周期中资源申请、调度、容错等细节。在作业启动过程中，JobMaster会与ResourceManager、TaskManager频繁交互，经过一系列复杂的过程之后，作业才真正在Flink集群中运行起来，进入执行阶段，开始读取、处理、写出数据的过程。

**JobMaster启动作业** 作业启动涉及JobMaster和TaskManager两个位于不同进程的组件，在JobMaster中完成作业图的转换，为作业申请资源、分配Slot，将作业的Task交给TaskManager，TaskManager初始化和启动Task。通过JobMaster管理作业的取消、检查点保存等，Task执行过程中持续地向JobMaster汇报自身的状态，以便监控和异常时重启作业或者Task。

```Java
// 作业调度的入口在JobMaster中，由JobMaster发起调度，根据调度器，启动不同调度器的调度，批流调度选择在DefaultScheduler中，通过多态的方式交给调度策略执行具体的调度。
// JobMaster启动调度
// JobMaster.java
private void startScheduling() {
    checkState(jobStatusListener == null);
    // 把自身作为作业状态的监听器
    jobStatusListener = new JobManagerJobStatusListener();
    schedulerNG.registerJobStatusListener(jobStatusListener);
    schedulerNG.startScheduling();
}

// DefaultScheduler.java
protected void startSchedulingInternal() {
    log.debut("Starting scheduling with scheduling strategy [{}]", schedulingStrategy.getClass().getName());
    prepareExecutionGraphForNgScheduling();
    schedulingStrategy.startScheduling();
}
```

**流作业调度** 流计算调度策略计算所有需要调度的ExecutionVertex，然后把需要调度的ExecutionVertex交给DefaultScheduler的`allocateSlotsAndDeploy()`，最终调用Execution的`deploy()`开始部署作业，当作业的所有Task启动之后，则作业启动成功。流作业的调度策略中，申请该作业所需要的Slot来部署Task，在申请之前将为所有的Task构建部署信息。DefaultScheduler中根据调度策略，选择不同的调度方法。流计算作业中，需要一次性部署所有Task，所以会对所有的Execution异步获取Slot，申请到所有需要的Slot之后，经过一系列的过程，最终调用Execution的`deploy()`进行实际的部署，实际上就是将Task部署相关的信息通过TaskManagerGateway交给TaskManager。在将Task发往TaskManager的过程中，需要将部署Task需要的信息进行包装，通过TaskManagerGateway部署Task到TaskManager。至此，将Task发送到TaskManager进程，TaskManager中接收Task部署信息，接下来开始启动Task执行，并向JobMaster汇报状态变换。

```Java
// 流作业申请Slot部署Task
// EagerSchedulingStrategy.java
// 调度执行
private void allocateSlotsAndDeploy(final Set<ExecutionVertexID> verticesToDeploy) {
    // 所有的ExecutionVertex一起调度
    final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions = SchedulingStategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(schedulingTopology, verticesToDeploy, id -> deploymentOption);
    schedulerOperations.allocateSlotsAndDeploy(executionVertexDeploymentOptions);
}

// 流作业调度
// DefaultScheduler.java
public void allocateSlotAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
    // 根据调度策略选择不同的调度分支
    if (isDeployIndividually()) {
        deployIndividually(deploymentHandles);
    } else {
        waitForAllSlotsAndDeploy(deploymentHandles);
    }
}

// 部署所有的Task
// DefaultScheduler.java
private BiFunction<Void, Throwable, Void> deployAll(final List<DeploymentHandle> deploymentHandles) {
    return (ignored, throwable) -> {
        propagateIfNonNull(throwable);
        // 对所有的Task进行调用，异步获取Slot，获取完毕之后异步部署Task
        for (final DeploymentHandle deploymentHandle : deploymentHandles) {
            final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
            final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
            checkState(slotAssigned.isDone());
            FutureUtils.assertNoException(slotAssigned.handle(deployOrHandleError(deploymentHandle)));
        }
        return null;
    };
}

// 部署Task
// Execution.java
public void deploy() throws JobException {
    assertRunningInJobMasterMainThread();
    // 各种Execution的状态检查和切换
    ...
    // 部署
    try {
        ...
        // 创建Task部署描述信息
        final TaskDeploymentDescriptor deployment = TaskDeploymentDescriptorFactory
            .fromExecutionVertex(vertex, attemptNumber)
            .createDeploymentDescriptor(slot.getAllocationId(), slot.getPhysicalSlotNumber(), taskRestore, producedPartitions.values());
        // TaskManager RPC通信接口
        final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
        final ComponentMainThreadExecutor jobMasterMainThreadExecutor = vertex.getExecutionGraph().getJobMasterMainThreadExecutor();
        // 通过异步回调的方式，调用TaskManagerGateway的RPC通信接口将Task发送给TaskManager，避免TDD作业模式信息太大，导致主线程阻塞
        CompletableFuture.supplyAsync(() -> taskManagerGateway.submitTask(deployment, rpcTimeout), executor)
            .thenCompose(Function.identity())
            .whenCompleteAsync((ack, failure) -> {...}, jobMasterMainThreadExecutor);
        )
    } catch (Throwable t) {
        // 异常处理
    }
}
```

**批作业调度** 批处理的本质是分阶段调度，上一个阶段执行完毕，且ResultPartition准备完毕之后，通知JobManager，JobManager调度下游消费ResultPartition的Execution（即Task）启动执行。InputDependencyConstraintChecker用来决定哪些下游Task具备执行条件，可以开始消费，然后调用DefaultScheduler进行资源申请，进入部署阶段。对于本阶段需要调度的Task，异步申请资源，申请资源完毕，最终调用Execution的`deploy()`进行实际的部署。批处理作业是分阶段、分批执行的，所以JobMaster需要知道何时能够启动下游的Task执行，在InputDependencyConstraint中调度时有两种限制规则：ANY规则和ALL规则。对于ANY规则，Task的所有上游输入有任意一个可以消费即可调度执行；对于ALL规则，Task的所有上游输入全部准备完毕后才可以进行调度执行。当接收到结果分区可消费的消息时，会再次触发调度执行的行为，遍历作业所有ExecutionVertex，选择符合调度条件的进行调度。对于PIPELINED类型的结果分组，当中间结果分区开始接收第一个Buffer数据时，触发调度下游消费Task的部署与执行。批处理作业从JobMaster向TaskManager的部署过程也是通过TaskManagerGateway接口进行。

```Java
// 申请Slot部署Task
// LazyFromSourcesSchedulingStrategy.java
// 调度执行
private void allocateSlotsAndDeployExecutionVertices(final Iterable<? extends SchedulingExecutionVertex<?, ?>> vertices) {
    // 从所有的调度节点中筛选出CREATED状态且具备执行条件的ExecutionVertex进行调度，执行条件的判断逻辑在InputDependencyConstraintChecker中
    final Set<ExecutionVertexID> verticesToDeploy = IterableUtils.toStream(vertices)
        .filter(IS_IN_CREATED_EXECUTION_STATE.and(isInputConstraintSatisfied()))
        .map(SchedulingExecutionVertex::getId)
        .collect(Collectors.toSet());

    final List<ExecutionVertexDeployment> vertexDeploymentOptions = SchedulingStrategyUtils.createExecutionVertexDeploymentOptionsInTopologicalOrder(schedulingTopology, verticesToDeploy, deploymentOptions::get);
    // 申请资源，调度可执行的ExecutionVertex启动Task
    schedulerOperations.allocateSlotsAndDeploy(vertexDeploymentOptions);
}

// 部署Task入口
// DefaultScheduler.java
private void deployIndividually(final List<DeploymentHandle> deploymentHandles) {
    for (final DeploymentHandle deploymentHandle : deploymentHandles) {
        FutureUtils.assertNoException(
            deploymentHandle.getSlotExecutionVertexAssignment()
                .getLogicalSlotFuture()
                .handle(assignResourceOrHandleError(deploymentHandle))
                .handle(deployOrHandleError(deploymentHandle));
        )
    }
}

// 调度结果分区下游消费数据
// LazyFromSourcesSchedulingStrategy.java
// 上游结果分区可消费，调度下游执行
public void onPartitionConsumable(ExecutionVertexID executionVertexId, ResultPartitionID resultPartitionId) {
    final SchedulingResultPartition<?, ?> resultPartition = schedulingTopology.getResultPartitionOrThrow(resultPartitionId.getPartitionId());
    if (!resultPartition.getResultType.isPipelined()) {
        return;
    }
    final SchedulingExecutionVertex<?, ?> producerVertex = schedulingTopology.getVertexOrThrow(executionVertexId);
    if (!Iterables.contains(producerVertex.getProducedResults(), resultPartition)) {
        throw new IllegalStateException("partition " + resultPartitionId + " is not the produced partition of " + executionVertexId);
    }
    allocateSlotsAndDeployExecutionVertices(resultPartition.getConsumers());
}
```

**TaskManager启动Task** StreamTask是算子的执行容器，在JobGraph中将算子连接在一起进行了优化，在执行层面上对应的是OperatorChain。JobMaster通过TaskManagerGateway的`submit()`RPC接口将Task发送到TaskManager上，TaskManager接收到Task的部署消息后，分为两个阶段执行：Task部署和启动Task，从部署信息中获取Task执行所需要的信息，初始化Task，然后触发Task的执行。在部署和执行的过程中，TaskExecutor和JobMaster保持交互，将Task的状态汇报给JobMaster，并接受JobMaster的Task管理操作。

 1. Task部署：TaskManager的实现类是TaskExecutor，JobMaster将Task的部署信息封装为TaskDeploymentDescriptor对象，通过SubmitTask消息发送给TaskExecutor。处理该消息的入口方法是submitTask方法，该方法的核心逻辑是初始化Task，在初始化Task的过程中，需要为Task生成核心组件，准备好Task的可执行文件。这些核心组件的准备工作，目的都是实例化Task。Task在实例化过程中，还进行了重要的准备工作。在ExecutionGraph中每一个Execution对应一个Task，ExecutionEdge代表Task之间的数据交换关系，所以在Task的初始化中，需要ExecutionEdge的数据关系落实到运行层面上。在这个过程中，最重要的是建立上下游之间的交换通道，以及Task如何从上游读取，计算结果如何输出给下游。读取上游数据只用InputGate，结果写出使用ResultPartitionWriter。ResultPartitionWriter和InputGate的创建、销毁等管理由ShuffleEnvironment来负责。ShuffleEnvironment底层数据存储对应的是Buffer，一个TaskManager只有一个ShuffleEnvironment，所有的Task共享

2. 启动Task：Task被分配到单独的线程中，循环执行。Task本身是一个Runnable对象，由TaskManager管理和调度，线程启动后进入`run()`方法。Task是容器，最终启动算子的逻辑封装在StreamTask中，在Task中通过反射机制实例化StreamTask子类，触发StreamTask的`invoke()`启动真正的业务逻辑执行。在Task初始化中，构建了InputGate组件和ResultPartitionWriter组件，但ResultPartitionWriter还没有注册到ResultPartitionManager，InputGate也并未与上游Task之间建立物理上的数据传输通道，在Task开始执行的过程中完成了实际的关联。

```Java
// Task执行
// Task.java
private void doRun() {
    // 处理Task的状态变化
    while (true) {
        ...
    }
    Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
    AbstractInvokable invokable = null;
    try {
        ...
        // 创建用户自定义的ClassLoader，避免不同Job在相同JVM中执行时的Jar版本冲突
        userCodeClassLoader = createUserCodeClassLoader();
        final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);
        if (isCanceledOrFailed) {
            throw new CancelTaskException();
        }
        // 初始化Partition、InputGate，并注册Partition
        setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);
        for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
            taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
        }
        ...
        // 初始化用户代码
        Environment env = new RuntimeEnvironment(...);
        // 设置线程的ClassLoader，避免不同Job的Jar版本冲突
        executingThread.setContextClassLoader(userCodeClassLoader);
        // 加载业务逻辑执行代码，并赋予RuntimeEnvironment
        invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
        // 核心逻辑，启动StreamTask执行
        this.invokable = invokable;
        // 从DEPLOYING切换到RUNNINT状态
        if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
            throw new CancelTaskException();
        }
        // 通知相关的组件，Task进入Running状态
        taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING))；
        // 设置执行线程的ClassLoader
        executingThread.setContextClassLoader(userCodeClassLoader);
        // 启动业务逻辑的执行，执行线程中循环执行
        invokable.invoke();
        // 业务逻辑执行完毕
        // 把尚未写出给下游的数据统一Flush，如果失败的话，Task也会失败
        for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
            if (partitionWriter != null) {
                partitionWriter.finish();
            }
        }
        // Task正常执行完毕，迁移到Finished状态
    } catch (Throwable t) {
        // 异常处理，取消StreamTask执行，切换到Failed，如果无法处理，则报告致命错误
    } finally {
        // 释放内存、占用的Cache等，并进入Final状态、停止监控
        ...
    }
}

// 初始化ResultPartition和InputGate
// Task.java
public static void setupPartitionsAndGates(ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException, InterruptedException {
    // 初始化结果分区
    for (ResultPartitionWriter partition : producedPartitions) {
        partition.setup();
    }
    // 初始化InputGate，并请求结果分区
    for (InputGate gate : inputGates) {
        gate.setup();
    }
}
```

## 作业执行

物理执行图并非Flink的数据结构，而是JobMaster根据ExecutionGraph对作业进行调度后，在各个TaskManager上部署Task后形成的图，描述物理上各个Task对象的拓扑关系，包含上下游连接关系、内存中数据的存储、数据的交换等，是运行时的概念，由Task、ResultPartition & ResultSubPartition、InputGate & InputChannel组成。

StreamInputProcessor（输入处理器）是对StreamTask中读取数据行为的抽象，在其实现中要完成数据的读取、处理、输出给下游的过程，分为StreamOneInputProcessor和StreamTwoInputProcessor两种实现。StreamOneInputProcessor用在OneInputStreamTask中，只有1个上游输入；StreamTwoInputProcessor用在TwoInputStreamTask中，有两个上游输入。核心方法是`processInput()`，该方法中调用`emit()`触发数据的读取，将数据反序列化为StreamRecord，交给StreamTaskNetworkOutput，由其出发StreamOperator（算子）的处理，最终触发UDF的`processElement()`，执行用户在DataStream API中编写用户逻辑，处理数据，然后交给下游，整体过程如下：

```bob-svg
                     .------------------------------.
                     |    StreamInputProcessor      |
      .-----------.  |  .-----------------------.   |
  --->| InputGate |--+->|    StreamTaskInput    |   |
      '-----------'  |  '-----------+-----------'   |
                     |              |               |
                     |              v               |
                     |  .-----------------------.   |
                     |  |    Deserialization    |   |
                     |  '-----------+-----------'   |
                     |              |               |
                     |              v               |
                     |  .-------------------------. |  .----------------.
                     |  |StreamTaskNetworkOutput  |-+->| StreamOperator |-->
                     |  '-------------------------' |  '----+-----------'
                     '------------------------------'       |       ^
                                                            v       |
                                                       .------------+---.
                                                       |       UDF      |
                                                       '----------------'
```

StreamTaskInput（Task输入）是StreamTask的数据输入的抽象，分为StreamTaskNetworkInput和StreamTaskSourceINput两种实现。StreamTaskNetworkInput负责从上游Task获取数据，使用InputGate作为底层读取数据；StreamTaskSourceInput负责从外部数据源获取数据，本质上是使用SourceFunction读取数据，交给下游的Task。

SteamTaskNetworkOutput（Task输出）是StreamTask的数据输出的抽象，负责将数据交给算子来进行处理，实际的数据写出是在算子层面上执行的。StreamTaskNetworkOutput也有对应于单流输入和双流输入的两种实现，作为私有内部类定义在StreamOneInputProcessor和StreamTwoInputProcessor中。

ResultPartition（结果分区）是作业单个Task产生数据的抽象，是运行时的实体，与ExecutionGraph中的IntermediateResultPartition对应。下游子任务消费上游子任务产生的ResultPartition，在实际请求的时候，是向上游请求ResultSubPartition，并不是请求整个ResultPartition，请求的方式有远程请求和本地请求两种。Flink作业在集群并行执行时，IntermediateResult会按照并行度切分为与并行度个数相同的ResultPartition，每个ResultPartition又根据直接下游子任务的并行度和数据分发模式进一步切分为一个或多个ResultSubPartition。

ResultSubPartition（结果子分区）是结果分区的一部分，负责存储实际的Buffer，有PipelinedSubPartition和BoundedBlockingSubPartition两种，分别对应流处理中的数据消费模式和批处理中的数据消费模式。PipelinedSubPartition是纯内存型的结果子分区，只能被消费1次，当向PipelinedSubPartition中添加1个完成的BufferConsumer或者添加下一个BufferConsumer时，会通知PipelinedSubPartitionView新数据到达，可以消费了。BoundedBlockingSubPartition可以保存在文件中或者内存映射文件中，其行为是阻塞式的，需要等待上游所有的数据处理完毕，下游才开始消费数据，可以消费1次或者多次。

InputGate（输入网关）时Task的输入数据的封装，和JobGraph中的JobEdge一一对应，对应于上游的ResultPartition。InputGate是InputChannel的容器，用于读取IntermediateResult在并行执行时由上游Task产生的一个或多个ResultPartition。有SingleInputGate、InputGateWithMetrics和UnionInputGate三种实现。SingleInputGate是消费ResultPartition的实体，对应于一个IntermediateResult；UnionInputGate用于将多个InputGate联合起来，当作一个InputGate，一般是对应于上游的多个输出类型相同的IntermediateResult；InputGateWithMetrics是一个带监控统计的InputGate，统计InputGate读取的数据量，单位为byte。

InputChannel（输入通道）和ExecutionEdge一一对应，也和ResultSubPartition一对一相连，即一个InputChannel接收一个ResultSubPartition的输出。有LocalInputChannel、RemoteInputChannel和UnknownInputChannel3种实现。LocalInputChannel对应于本地结果子分区的输入通道，用来在本地进程内不同线程之间的数据交换。LocalInputChannel实际调用SingleInputGate的`notifyChannelNonEmpty()`，这个方法调用inputChannelsWithData的`notifyAll()`，唤醒阻塞在inputChannelsWithData对象实例的所有线程，阻塞在CheckpointBarrierHandler的`getNextNonBlocked()`方法的线程也会被唤醒，返回数据；RemoteInputChannel对应于远程的ResultSubPartition的InputChannel，用来表示跨网络的数据交换；UnknownInputChannel是一种用于占位目的的输入通道，需要占位通道是因为暂未确定相对于Task生产者的位置，在确定上游Task位置之后，如果位于不同的TaskManager则替换为RemoteInputChannel，如果位于相同的TaskManager则替换为LocalInputChannel。

Flink 1.10引入了类Actor模型的基于Mailbox的单线程Task执行模型，取代之前的依赖于锁机制的多线程Task执行模型，所有的并发操作都通过队列进行排队（Mailbox），单线程（Mailbox线程）依次处理，这样就避免了并发操作。

启动Task进入执行状态，开始读取数据，当有可消费的数据时，则持续读取数据。StreamInputProcessor是数据读取、处理、输出的高层逻辑的载体，由其负责触发数据的读取，并交给算子处理，然后输出。StreamInputProcessor实际上将具体的数据读取工作交给了StreamTaskInput，当读取了完整的记录之后就开始向下游发送数据，在发送数据的过程中，调用算子进行数据的处理。读取到完成数据记录之后，根据其类型进行不同的处理。

对于数据记录（SteamRecord），会在算子中包装用户的业务逻辑，即使用DataStream编写的UDF，进入到算子内部，由算子去执行用户编写的业务逻辑。在算子中处理完毕，数据要交给下一个算子或者Task进行计算，此时会涉及3种算子之间数据传递的情形：
1. OperatorChain内部的数据传递，发生在OperatorChain所在的本地线程内
2. 同一个TaskManager的不同Task之间传递数据，发生在同一个JVM的不同线程之间
3. 不同TaskManager的Task之间传递数据，即跨JVM的数据传递，需要使用跨网络的通信，即便TaskManager位于同一个物理机上，也会使用网络协议进行数据传递

对于Watermark，分两种情景，一种是在OneInputStreamOperator（单流输入算子中）；一种是在TwoInputStreamOperator（双流输入算子）种。对于OneInputStreamOperator，如果有定时服务，则判断是否触发计算，并将Watermark发往下游。对于TwoInputStreamOperator，会选择两个输入流中较小的Watermark作为当前Watermark，之后的处理与OneInputStreamOperator一致。

当SourceStreamTask或一般的StreamTask处于闲置状态（IDLE），不会向下游发送数据或Watermark时，就向下游发送StreamStatus.IDLE状态告知下游，依次向下传递。当恢复向下游发送数据或者Watermark前，首先发送StreamStatus.ACTIVE状态告知下游。StreamStatus状态变化在SourceFunction中产生，SourceTask如果读取不到输入数据，则认为是Idle状态，如果重新读取到数据，则认为是Active状态。只要StreamTask有一个上游的Source Task是Active状态，StreamTask就是Active状态，否则处于Idle状态。由于SourceTask保证在Idle状态和Active状态之间不会发生数据元素，所以StreamTask可以在不需要检查当前状态的情况下安全地处理和传播收到数据元素。当前StreamTask在发送Watermark之前必须检查当前算子的状态，如果当前的状态是Idle，则Watermark会被阻塞，不会向下游发送。

对于LatencyMarker，直接交给下游。

```Java
// StreamTask数据处理入口
// OneInputStreamTask.java
protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
    InputStatus status = inputProcessor.processInput();
    if (status == InputStatus.MORE_AVAILABLE && recordWriter.isAvailable()) {
        return;
    }
    if (status == INputStatus.END_OF_INPUT) {
        controller.allActionsCompleted();
        return;
    }
    CompletableFuture<?> jointFuture = getInputOUtputJointFuture(status);
    MailboxDefaultAction.Suspension suspendedDefaultAction = controller.suspendDefaultAction();
    jointFuture.thenRun(suspendedDefaultAction::resume);
}

// StreamOneInputProcessor处理
// StreamOneInputProcessor.java
@Override
public InputStatus processInput() throws Exception {
    InputStatus status = input.emitNext(output);
    if (status == InputStatus.END_OF_INPUT) {
        synchronized (lock) {
            operatorChain.endHeadOperatorInput(1);
        }
    }
    return status;
}

// 从NetworkBuffer中读取完整的StreamRecord并触发处理
// StreamTaskNetworkInput.java
@Override
public InputStatus emitNext(DataOutput<T> output) throws Exception {
    while (true) {
        // 从反序列化器中获取数据流元素
        if (currentRecordDeserializer != null) {
            DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
            if (result.isBufferConsumed()) {
                currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
                currentRecordDeserializer = null;
            }
            if (result.isFullRecord()) {
                processElement(deserializationDelegate.getInstance(), output);
                return InputStatus.MORE_AVAILABLE;
            }
        }
    }
    Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
    if (bufferOrEvent.isPresent()) {
        processBufferOrEvent(bufferOrEvent.get());
    } else {
        if (checkpointedInputGate.isFinished()) {
            checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
            if (!checkpointedInputGate.isEmpty()) {
                throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
            }
            return InputStatus.END_OF_INPUT;
        }
        return InputStatus.NOTHING_AVAILABLE;
    }
}

// 按照StreamRecord的类型分别进行处理
// StreamTaskNetworkInput.java
private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
    if (recordOrMark.isRecord()) {
        output.emitRecord(recordOrMark.asRecord);
    } else if (recordOrMark.isWatermark()) {
        statusWatermarkValue.inputWatermark(recordOrMark.asWatermark(), lastChannel);
    } else if (recordOrMark.isLatencyMarker()) {
        output.emitLatencyMarker(recordOrMark.asLatencyMarker());
    } else if (recordOrMark.isStreamStatus()) {
        statusWatermarkValue.inputStreamStatus(recordOrMark.asStreamStatus()), lastChannel);
    } else {
        throw new UnsupportedOperationException("Unknown type of StreamElemetn");
    }
}

// 触发算子执行用户逻辑
// StreamTaskNetworkOutput.java
@Override
public void emitRecord(StreamRecord<IN> record) throws Exception {
    synchronized (local) {
        numRecordsIn.inc();
        operator.setKeyContextElement1(record);
        operator.processElement(record);
    }
}

// 单流输入Watermark处理
// AbstractStreamOperator.java
public void processWatermark(Watermark mark) throws Exception {
    if (timeServiceManager != null) {
        timeServiceManager.advanceWatermark(mark);
    }
    output.emitWatermark(mark);
}

// 双流输入Watermark处理
// AbstractStreamOperator.java
public void processWatermark1(Watermark mark) throws Exception {
    input1Watermark = mark.getTimestamp();
    long newMin = Math.min(input1Watermark, input2Watermark);
    if (newMin > combinedWatermark) {
        combinedWatermark = newMin;
        processWatermark(new Watermark(combinedWatermark));
    }
}

public void processWatermark2(Watermark mark) throws Exception {
    input2Watermark = mark.getTimestamp();
    long newMin = Math.min(input1Watermark, input2Watermark);
    if (newMin > combinedWatermark) {
        combinedWatermark = newMin;
        processWatermark(new Watermark(combinedWatermark));
    }
}

// 算子处理LatencyMarker
// AbstractSreamOperator.java
@Override
public void emitLatencyMarker(LatencyMarker latencyMarker) {
    output.emitLatencyMarker(latencyMarker);
}
```

## 作业停止

作业停止主要是资源的清理和释放。JobMaster向所有的TaskManager发出取消作业的指令，TaskManager执行Task的取消指令，进行相关的内存资源的清理，当所有的清理作业完成之后，向JobMaster发出通知，最终JobMaster停止，向ResourceManager归还所有的Slot资源，然后彻底退出作业的执行。

## 容错

对于分布式系统来说，守护进程的容错基本包括故障检测和故障恢复两个部分：故障检测通常通过心跳的方式来实现，心跳可以在内部组件间实现或者依赖于zookeeper等外部服务；故障恢复则通常要求将状态持久化到外部存储，然后再故障出现时用于初始化新的进程。

Flink中的Dispatcher、JobMaster、ResourceManager、TaskManager都是非常重要的组件，所以需要进行高可用设计，使组件具备容错能力，防止单个组件故障导致整个集群宕机。容错的基本原理使提供两个或以上的相同组件，选择其中一个作为Leader，其他的作为备选，当Leader出现问题时，各备选组件能够感知到Leader宕机，重新选举，并通知各相关组件新的Leader。

### HA服务

**HA服务（高可用服务）** 提供了Leader选举、Leader获取等服务，也封装了对Flink内部状态的保存和获取服务，以便在异常恢复的时候使用。在Flin中有3种HA服务
+ ZooKeeperHAService：基于ZooKeeper的高可用服务实现，使用ZooKeeper作为集群信息的存储，能够实现真正的高可用  
+ StandaloneHAService：基于内存和本地文件系统实现高可用服务，能在一定程度上实现高可用，多用在测试场景中
+ EmbeddedHAService：在Flink Local模式中实现高可用服务，为了实现Flink的完整作业流程而实现的模拟服务

**Leader服务** 对于JobMaster和ResourceManager，Leader选举服务和Leader变更服务是两项基本服务，Leader选举服务用于切换到新的Leader，Leader变更服务用于通知相关的组件Leader的变化，进行相应的处理
+ Leader选举服务：为竞争者提供选举和确认Leader，在Flink中有3种Leader选举服务的实现
    + ZooKeeperLeaderElectionService：基于ZooKeeper的选举服务
    + StandaloneLeaderElectionService：没有ZooKeeper情况下的选举服务
    + EmbeddedLeaderElectionService：内嵌的轻量级选举服务，用在Flink Local模式中，主要用来做本地的代码调试和单元测试
+ Leader变更服务：HA模式下若出现组件故障，会进行新的Leader选举，即选择主节点，选举完成之后，要通知其他的组件，LeaderRetrievalListener用爱来通知选举了新的Leader，在选举过程中可能顺利选举出新的Leader，也可能因为内部或外部异常，导致无法选举出新的Leader，此时也需要通知相关组件，对于无法处理的故障，无法进行恢复，作业进入停止状态。在Flink中有3中Leader查询服务的实现
    + ZooKeeperLeaderRetrievalService
    + StandaloneLeaderRetrivalService
    + EmbeddedLeaderRetrivalService

```Java
// Leader选举通知
public interface LeaderRetrievalListener {
    // Leader选举服务选举出新的Leader，通知其他相关组件
    void notifyLeaderAddress(@Nullable String leaderAddress, @Nullable UUID leaderSessionID);
    // 当Leader选举服务发生异常时通知各相关组件
    void handlerError(Exception exception);
}
```

**心跳服务** 心跳机制是分布式集群中组件监控的常用手段，Flink各个组件的监控统一使用心跳机制实现。一个完整的心跳机制需要有心跳的发送者和接收者两个实体，心跳行为需要进行管理，心跳超时需要有相应实体进行异常处理。另外，Flink的心跳数据包中还包含了一些技术信息（如TaskManager通过心跳向ResourceManager汇报当前Slot的使用情况。
+ 心跳目标（HeartbeatTarget）：用来表示心跳发送者和心跳接收者，同一个组件既是发送者也是接收者
+ 心跳管理者（HeartbeatManager）：用来启动或停止监视HeartbeatTarget，并报告该目标心跳超时事件，通过monitorTarget来传递并监控HeartbeatTarget。在JobMaster、ResourceManager、TaskManager中使用HeartbeatManager进行两两心跳监控
+ 心跳监听器（HeartbeatListener）：用来心跳超时通知、接收心跳信息中的Payload、检索作为心跳响应输出的Payload。JobManager、ResourceManager、TaskManager都实现了该接口，心跳超时时触发异常处理

```Java
// HeartbeatTarget接口
public interface HeartbeatTarget<I> {
    // 接收监控目标发送来的心跳请求信息
    void receiveHeartbeat(ResourceID heartbeatOrigin, I heartbeatPayload);
    // 向监控目标发送心跳请求
    void requestHeartbeat(ResourceID requestOrigin, I heartbeatPayload);
}

// HeartbeatManager接口
public interface HeartbeatManager<I, O> extends HeartbeatTarget<I> {
    // 开始监控心跳目标，若目标心跳超时，会报告给与HeartbeatManager关联的HeartbeatListener
    void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget);
    // 取消监控心跳目标，ResourceID被监控目标组件标识
    void unmonitorTarget(ResoruceID resourceID);
    // 停止当前心跳管理器
    void stop();
    // 返回最近依次心跳时间，如果心跳目标被移除了则返回-1
    long getLastHeartbeatFrom(ResourceID resourceId);
}

// HeartbeatListener接口
public interfaceHeartbeatListener<I, O> {
    // 心跳超时时会调用该方法
    void notifyHeartbeatTimeout(ResourceID resourceID);
    // 接收到有关心跳的Payload就会执行该方法
    void reportPayload(ResourceID resourceID, I payload);
    // 检索下一个心跳消息的Payload
    O retrievePayload(ResourceID resourceID);
}
```

### JobMaster容错

**TaskManager应对JobMaster故障** TaskManager通过心跳超时检测到JobMaster故障，或者收到ZooKeeper的JobMaster节点失去Leader角色的通知时，就会触发超时的异常处理。TaskManager根据该JobMaster管理的Job的ID，将该TaskManager上所有隶属于该Job的Task取消执行，Task进入Failed状态，然后尝试连接新的JobMaster Leader，如果新的JobMaster超过了等待时间仍然没有连接上，TaskManager不再等待，标记Slot为空闲并通知ResourceManager，ResourceManager就可以在下一次的Job调度执行中分配这些Slot资源

**ResourceManager应对JobMaster故障** ResourceManager通过心跳超时检测到JobMaster故障，或者收到ZooKeeper的JobMaster节点失去Leader角色的通知时，ResourceManager会通知JobMaster重新尝试连接，其他不作处理
**JobMaster切换** JobMaster出现故障之后要选举新的JobMaster Leader，新的Leader选举出来之后，会通知ResourceManager和TaskManager

### ResourceManager容错

ResourceManager的故障不会中断Task的执行，但是无法启动新的Task

**JobMaster应对ResourceManager故障** JobMaster通过心跳超时检测到ResourceManager故障，首先尝试重新连接，如果一直没有连接成功，在HA模式下，JobMaster通过Leader选举通知得到的新的ResourceManager地址，通过该地址重新与ResourceManager连接。实在无法与ResourceManager取得连接的情况下，整个集群就会停止

**TaskManager应对ResourceManager故障** TaskManager通过心跳检测到ResourceManager故障，首先尝试重新连接，如果一直没有连接成功，在HA模式下，TaskManager得知选举出的新ResourceManager Leader后，会与新ResourceManager取得连接，将自己注册到ResourceManager，并将其自身的状态同步到ResourceManager。实在无法与ResourceManager取得连接的情况下，整个集群就会停止

### TaskManager容错

**ResourceManager应对TaskManager故障** ResourceManager通过心跳超时检测到TaskManager故障，会通知对应的JobMaster并启动一个新的TaskManager作为代替^[ResourceManager并不关心Flink作业的情况，管理Flink作业要做何种反应是JobMaster的职责]

**JobMaster应对TaskManager故障** JobMaster通过心跳超时检测到TaskManager故障，首先会从自己的Slot Pool中移除该TaskManager，并释放TaskManager的Slot，最终触发Execution的异常处理，然后触发Job级别的恢复，从而重新申请资源，由ResourceManager启动新的TaskManager来重新执行Job
