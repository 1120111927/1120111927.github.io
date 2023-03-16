---
title: YARN
date: "2023-03-05"
description: 
tags:
---

```toc
ordered: true
class-name: "table-of-contents"
```

YARN采用了Master/Slave结构，Master实现为ResourceManager，负责整个集群资源的管理与调度，Slave实现为NodeManager，负责单个节点的资源管理与任务启动。

## ResourceManager

ResourceManager负责集群中所有资源的统一管理和分配，它接收来自各个节点（NodeManager）的资源汇报信息，并把这些信息按照一定的策略分配给各个应用程序（ApplicationMaster），主要完成以下几个功能：

+ 与客户端交互，处理来自客户端的请求
+ 启动和管理ApplicationMaster，并在它失败时重新启动它
+ 管理NodeManager，接收来自NodeManager的资源汇报信息，并向NodeManager下达管理指令
+ 资源管理与调度，接收来自ApplicationMaster的资源申请请求，并为之分配资源

ResourceManager通过两个RPC协议与NodeManager和ApplicationMaster交互：

+ ResourceTracker：NodeManager通过该RPC协议向ResourceManager注册、汇报节点健康状况和Container运行状态，并领取ResourceManager下达的命令（重新初始化、清理Container等）。在该RPC协议中，ResourceManager为RPC Server（由内部组件ResourceTrackerService实现），NodeManager为RPC Client。NodeManager与ResourceManager之间采用了pull模型，NodeManager总是周期性地主动向ResourceManager发起请求，并通过领取下达给自己的命令
+ ApplicationMasterProtocol：ApplicationMaster通过该协议向ResourceManager注册、申请资源和释放资源。在该RPC协议中，ResourceManager为RPC Server（由内部组件ApplicationMasterService实现），ApplicationMaster为RPC Client
+ ApplicationClientProtocol：客户端通过该协议向ResourceManager提交应用程序、查询应用程序状态和控制应用程序等。在该RPC协议中，ResourceManager为RPC Server（由内部组件ClientRMService实现），客户端为RPC Client

ResourceManager主要由以下几个部分组成：
+ 用户交互模块
    + ClientRMService：处理来自客户端的各种RPC请求，如提交应用程序、终止应用程序、获取应用程序状态等
    + AdminService：为管理员提供了一套独立的服务接口^[防止大量普通用户请求使管理员发送的管理命令饿死]，如动态更新节点列表、更新ACL列表、更新队列信息等
    + WebApp：仿照Haml开发的一个轻量级嵌入式Web框架，对外提供Web界面展示集群资源使用情况和应用程序运行状态等信息
+ NM管理模块
    + NMLivelinessMonitor：监控NodeManager是否活着
    + NodesListManager：维护正常节点和异常节点列表，管理exclude和include节点列表
    + ResourceTrackerService：处理来自NodeManager的请求，主要包括注册和心跳两种请求。注册是NodeManager启动时发生的行为，请求包中包含节点ID、可用的资源上限等信息；心跳是周期性行为，包括各个Container运行状态，运行的Application列表、节点健康状况等信息，作为请求应答，ResourceTrackerService为NodeManager返回待释放的Container列表、Application列表等信息
+ AM管理模块
    + AMLivelinessMonitor：监控ApplicationMaster是否活着
    + ApplicationMasterLauncher：与某个NodeManager通信启动ApplicationMaster
    + ApplicationMasterService：处理来自ApplicationMaster的请求，主要包括注册和心跳两种请求。注册是ApplicationMaster启动时发生的行为，注册请求包中包含ApplicationMaster启动节点、对外RPC端口号和tracking URL等信息；心跳则是周期性行为，汇报信息包含所需资源描述、待释放Container列表、黑名单列表等，作为请求应答，ApplicationMasterService为ApplicationMaster返回新分配的Container、失败的Container、待抢占的Container列表等信息
+ Application管理模块
    + ApplicationACLsManager：管理应用程序访问权限
    + RMAppManager：管理应用程序的启动和关闭
    + ContainerAllocationExpire：决定和执行一个已经分配的Container是否该被回收
+ 状态机管理模块
    + RMApp：维护了一个应用程序的整个生命周期^[由于一个Application的生命周期可能会启动多个Application运行实例（Application Attempt），RMApp维护的是同一个Application启动的所有运行实例的生命周期]
    + RMAppAttempt：维护了应用程序一次运行尝试的整个生命周期
    + RMContainer：维护了一个Container的整个生命周期
    + RMNode：维护了一个NodeManager的生命周期
+ 资源分配模块
    + ResourceScheduler：资源调度器按照一定的约束条件将集群中的资源分配给各个应用程序。ResourceScheduler是一个插拔式模块，YARN自带了一个批处理调度器（FIFO）和两个多用户调度器（Fair Scheduler和Capacity Scheduler）
+ 安全管理模块
    + ClientToAmSecretManager
    + ContainerTokenSecretManager
    + ApplicationTokenSecretManager

ResourceManager所有服务和组件是通过中央异步调度器组织在一起的，不同组件之间通过事件交互，从而实现了一个异步并行的高效系统。ResourceManager内部各个事件和服务以及它们处理和输出的事件类型如下：

|组件名称|事件处理器/服务|处理的事件类型|输出事件类型|
|--|---|---|---|
|ClientRMService|服务||RMAppEventType|
|NMLivelinessMonitor|服务||RMNodeEventType|
|NodesListManager|事件处理器服务|NodesListManagerEvent||
|ResourceTrackerserver|服务||RMNodeEventType|
|AMLivelinessMonitor|服务||RMAppAttemptEventType|
|ApplicationMasterLauncher|事件处理器服务|AMLauncherEventType||
|RMAppManager|事件处理器|RMAppManagerEventType|RMAppEventType|
|ContainerAllocation|服务||SchedulerEventType|
|RMApp|事件处理器|RMAppEventType|RMAppAttemptEventType<br>RMNodeEventType|
|RMAppAttempt|事件处理器|RMAppAttemptEventType|SchedulerEventType<br>RMAppEventType|
|RMContainer|事件处理器|RMContainerEventType|RMAppAttemptEventType<br>RMNodeEventType|
|RMNode|事件处理器|RMNodeEventType|SchedulerEventType<br>RMNodeEventType<br>NodeslistMangerEventType|
|ResourceScheduler|事件处理器|SchedulerEventType|RMContainerEventType<br>RMAppAttemptEventType<br>RMNodeEventType|

ResourceManager启动ApplicationMaster过程：
1. ResourceManager收到客户端提交应用程序请求后，先向资源调度器申请用以启动ApplicationMaster的资源，待申请到资源后，再由ApplicationMasterLauncher与对应的NodeManager通信，从而启动应用程序的ApplicationMaster
2. ApplicationMaster启动完成后，ApplicationMasterLauncher会通过事件的形式，将刚刚启动的ApplicationMaster注册到AMLivelinessMonitor，以启动心跳监控
3. ApplicationMaster启动后，先向ApplicationMasterService注册，并将自己所在host、端口号等信息汇报给它
4. ApplicationMaster运行过程中，周期性地向ApplicationMasterService汇报心跳信息
5. ApplicationMasterService每次收到ApplicationMaster的心跳信息后，将通知AMLivelinessMonitor更新该应用程序的最近汇报心跳的时间
6. 当应用程序运行完成后，ApplicationMaster向ApplicationMasterService发送请求，注销自己
7. ApplicationMasterService收到注销请求后，标注应用程序运行状态为完成，同时通知AMLivelinessMonitor移除对它的心跳监控

### ClientRMService

ClientRMService是一个RPC Server，为来自客户端的各种RPC请求提供服务，是一个实现了ApplicationCientProtocol协议的服务。

```Java
class ClientRMService extends AbstractService implements ApplicationClientProtocol {

    // RMContext是ResoueceManager上下文对象，保存ResourceManager的节点列表、队列组织、应用程序列表等信息，实现类是RMContextImpl
    RMContext rmContext;
    RMDelegationTokenSecretManager rmDTSecretManager;
    InetSocketAddress clientBindAddress;

    void serviceStart() {
        Configuration conf = getConfig();
        YarnRPC rpc = YarnRPC.create(conf);
        // 实现了RPC协议ApplicationClientProtocol
        this.server = rpc.getServer(ApplicationClientProtocol.class, this, clientBindAddress, conf, this.rmDTSecretManager, conf.getInt(YarnConfiguration.RM_CLIENT_THREAD_COUNT, YarnConfiguration.DEFAULT_RM_CLIENT_THREAD_COUNT));
        ...
    }
}

class RMContextImpl implements RMContext {
    Dispatcher rmDispatcher;        // 中央异步调度器
    ConcurrentMap<ApplicationId, RMApp> applications = new ConcurrentHashMap<ApplicationId, RMApp>();      // 应用程序列表
    ConcurrentMap<NodeId, RMNode> nodes = new ConcurrentHashMap<NodeId, RMNode();    // 非活跃节点列表
    ....
}
```

### ApplicationMasterLauncher

### AMLivelinessMonitor

### ApplicationMasterService
