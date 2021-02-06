---
title: YARN
date: "2021-01-23"
description: YARN主要由ResourceManager、NodeManager、ApplicationMaster和Container等几个组件组成
tags: YARN
---

## YARN基本组成结构

YARN总体上为Master/Slave结构，主要包含以下几个组件：

+ ResourceManager(RM)：一个全局的资源管理器，负责整个系统的资源管理和分配，主要由调度器（Scheduler）和应用程序管理器（Applications Manager，ASM）构成

  + 调度器：根据容量、队列等限制条件将系统中的资源分配给各个正在运行的应用程序，是一个可插拔的组件，用户可根据自己的需要设计新的调度器，YARN提供了Fair Scheduler和Capacity Scheduler等可直接可用的调度器
  + 应用程序管理器：负责管理整个系统中所有应用程序，包括应用程序提交、与调度器协商资源以启动ApplicationMaster、监控ApplicationMaster运行状态并在失败时重新启动它等

+ ApplicationMaster(AM)：每个应用程序均包含一个AM，负责单个应用程序的管理，具体功能为：与RM调度器协商以获取资源（用Container表示）、将得到的任务进一步分配给内部的任务、与NM通信以启动/停止任务、监控所有任务运行状态并在任务运行失败时重新为任务申请资源以重启任务

+ NodeManager(NM)：每个节点上的资源和任务管理器，定时向RM汇报本节点上的资源使用情况和各个Container的运行状态，接收并处理来自AM的Container启动/停止等各种请求

+ Container：YARN中的资源抽象，封装了某个节点上的多维度资源，如内存、CPU、磁盘、网络等，YARN会为每个任务分配一个Container，且该任务只能使用该Container中描述的资源。目前YARN仅支持CPU和内存两种资源，且使用了轻量级资源隔离机制Cgroups进行资源隔离

## YARN通信协议

YARN组件间通过RPC协议进行通信。任何两个需相互通信的组件之间仅有一个RPC协议，而对于任何一个RPC协议，通信双方有一端是Client，另一端是Server，且Client总是主动连接Server的，即拉式（pull-based）通信模型。

主要有以下几个RPC协议：

+ ApplicationClientProtocol：作业提交客户端（JobClient）与RM之间的协议，JobClient通过该RPC协议提交应用程序、查询应用程序状态等

+ ResourceManagerAdministrationProtocol：管理员（Admin）与RM之间的协议，Admin通过该RPC协议更新系统配置文件，比如节点黑白名单、用户队列权限等

+ AppicationMasterProtocol：AM与RM之间的协议，AM通过该RPC协议向RM注册和撤销自己，并为各个任务申请资源

+ ContainerManagementProtocal：AM与NM之间的协议，AM通过该RPC要求NM启动或者停止Container，获取各个Container的使用状态等信息

+ ResourceTracker：NM与RM之间的协议，NM通过该RPC协议向RM注册，并定时发送心跳信息汇报当前结点的资源使用情况和Container运行情况

## YARN工作流程

当用户向YARN中提交一个应用程序后，YARN将分两个阶段运行该应用程序：第一个阶段是启动ApplicationMaster，第二个阶段是由ApplicationMaster创建应用程序，为它申请资源，并监控它的整个运行过程，知道运行完成。具体分为以下几个步骤：

1. 用户向YARN中提交应用程序，其中包括ApplicationMaster程序、启动ApplicationMaster的命令、用户程序等

2. ResourceManager为该应用程序分配一个Container，并与对应的NodeManager通信，要求它在这个Container中启动应用程序的ApplicationMaster

3. ApplicationMaster首先向ResourceManager注册，这样用户可以直接通过ResourceManager查看应用程序的运行状态，然后它将为各个任务申请资源，并监控它的运行状态，知道运行结束，即重复步骤4~7

4. ApplicationMaster采用轮询的方式通过RPC协议向ResourceManager申请和领取资源

5. 一旦ApplicationMaster申请到资源后，便与对应的NodeManager通信，要求它启动任务

6. NodeManager为任务设置好运行环境（包括环境变量、JAR包、二进制程序等后），将任务启动命令写到一个脚本中，并通过运行该脚本启动任务

7. 各个任务通过某个RPC协议向ApplicationMaster汇报自己的状态和进度，以让ApplicationMaster随时掌握各个任务的运行状态，从而可以在任务失败时重新启动任务。在应用程序运行过程中，用户可随时通过RPC向ApplicationMaster查询应用程序的当前运行状态

8. 应用程序运行完成后，ApplicationMaster向ResourceManager注销并关闭自己

运行在YARN上的应用程序主要分为两类：短应用程序和长应用程序。短应用程序是指一定时间内可运行完成并正常退出的应用程序，如MapReduce作业。长应用程序是指不出意外，永不终止运行的应用程序，如Storm Service、HBase Service。

## YARN应用程序

YARN应用程序（Application）是用户编写的处理数据的程序的统称，它从YARN中申请资源以完成自己的计算任务。YARN应用程序主要分为短应用程序（短时间内可运行完成的程序）和长应用程序（永不终止运行的服务）。YARN应用程序编写复杂，一般由专业人员开发，通过回调的形式供其他普通用户使用（如YARN为MapReduce实现了一个可以直接使用的客户端（JobClient）和ApplicationMaster（MRAppMaster）。

开发YARN应用程序通常需要编写两个组件：Client（客户端）和ApplicationMaster。客户端负责向ResourceManager提交ApplicationMaster，并查询应用程序运行状态。ApplicationMaster负责向ResourceManager申请资源（以Container形式表示），并与NodeManager通信以启动计算任务（Container），还负责监控各个任务运行状态及任务容错。

通常而言，编写一个YARN Application会涉及3个RPC协议：

+ ApplicationClientProtocol：用于Client与ResourceManager之间，Client通过该协议可实现将应用程序提交到ResourceManager上、查询应用程序的运行状态或者杀死应用程序等功能

+ ApplicationMasterProtocol：用于ApplicationMaster与ResourceManager之间，ApplicationMaster使用该协议向ResourceManager注册、申请资源、获取各个任务运行情况等

+ ContainerManagementProtocal：用于ApplicationMaster与NodeManager之间，ApplicationMaster使用该协议要求NodeManager启动/撤销Container或者查询Container的运行状态

### 客户端设计

通常而言，客户端向ResourceManager提交ApplicationMaster需经过以下两个步骤：

1. Client通过RPC函数ApplicaitonClientProtocol#getNewApplication从ResourceManager中获取唯一的Application ID

   ```Java
   // 客户端创建一个ApplicationClientProtocol协议的RPC Client，并通过该Client与ResourceManager通信
   private ApplicationClientProtocol rmClient;  // RPC Client
   // rmAddress为服务器端地址，conf为配置对象
   this.rmClient = (ApplicationClientProtocol)rpc.getProxy(ApplicationClientProtocol.class, rmAddress, conf)
   // 调用ApplicationClientProtocol#getNewApplication从ResourceManager上领取唯一的Application ID
   GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
   GetNewApplicationResponse newApp = rmClient.getNewApplication(request);
   ApplicationId appId = newApp.getApplicationId();
   ```

   静态方法Records#newRecord常用于构造一个可序列化对象，具体采用的序列化工厂由参数`yarn.ipc.record.factory.class`指定，默认是`org.apache.hadoop.yarn.factories.impl.pb.RecordFactoryPBImpl`，即构造的是Protocol Buffers序列化对象。`GetNewApplicationRequest`类的对象主要包含两项信息：Application ID和最大可申请资源量

2. Client通过RPC函数ApplicationClientProtocol#submitApplication将ApplicationMaster提交到ResourceManager上

   ```Java
   ApplicaitonSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
   appContext.setApplicationName(appName);  // 设置应用程序名称
   // ... 设置应用程序其他属性，如优先级、队列名称等
   ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class)；  // 构造一个AM启动上下文对象
   // ... 设置AM相关的变量
   amContainer.setLocalResources(localResources);  // 设置AM启动所需的本地资源
   amContainer.setEnvironment(env);  // 设置AM启动所需的环境变量
   appContext.setAMContainerSpce(amContainer);
   appContext.setApplicationId(appId);  // appId是上一步获取的ApplicationId
   SubmitApplicaitonRequest request = Records.newRecord(SubmitApplicationRequest.class);
   request.setApplicationSubmissionContext(appContext.class);
   rmClient.submitApplication(request);  // 将应用程序提交到ResourceManager上
   ```

客户端将启动ApplicationMaster所需的所有信息打包到数据结构ApplicationSubmissionContext中，该数据结构的定义在Protocol Buffers文件yarn_protos.proto中，主要包括以下几个字段：

+ application_id：Application ID
+ applicaiton_name：Application名称
+ priority：Application优先级
+ queue：Application所属队列
+ user：Application所属用户名
+ unmanaged_am：是否由客户端自己启动ApplicationMaster
+ cancel_token_when_complete：当应用程序运行完成时，是否取消Token
+ am_container_spec：启动ApplicationMaster相关的信息，主要包含以下几项
  + user：ApplicationMaster启动用户
  + resource：启动ApplicationMaster所需的资源，当前支持CPU和内存两种
  + localResouce：ApplicationMaster运行所需的本地资源，通常是一些外部文件，如字典等
  + command：ApplicationMaster启动命令（一般为Shell命令）
  + environment：ApplicationMaster运行时所需的环境变量

RPC协议ApplicationClientProtocol：

```Java
public interface ApplicationBaseProtocol {
  // 获取Application运行报告，包括用户、队列、运行状态等信息
  public GetApplicationReportResponse getApplicationReport(GetApplicationReportRequest request) throws YarnException, IOException;

  // 查看当前集群中所有应用程序信息
  public GetApplicationsResponse getApplications(GetApplicationsRequest request) throws YarnException, IOException;

  // ...
}

public interface ApplicationClientProtocol extends ApplicationBaseProtocol {
  // 获取ApplicationId
  public GetNewApplicationResponse getNewApplication(GetNewApplicationRequest request) throws YARNException, IOException;

  // 提交Application
  public SubmitApplicationResponse submitApplication(SubmitApplicationRequest request) throws YarnException, IOException;

  // 终止Application
  public KillApplicationResponse forceKillApplication(KillApplicationRequest request) throws YarnException, IOException;

  // 获取集群统计（metric）信息
  public GetClusterMetricsResponse getClusterMetrics(GetClusterMetricsRequest request) throws YarnException, IOException;

  // 查询当前系统中所有节点信息
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request) throws YarnException, IOException;

  // ...
}
```

客户端与ResourceManager之间的通信对所有类型的应用程序都是一致的，可以做成通用的代码模块，YARN提供了能与ResouceManager交互完成各种操作的编程库`org.apache.hadoop.yarn.client.YarnClient`。为了减轻ResourceManager的负载，一旦应用程序的ApplicationMaster成功启动后，客户端通常直接与ApplicationMaster通信，以查询它的运行状态或者控制它的执行流程，这一部分与ApplicationMaster的设计相关，不同类型的应用程序是不一样的，用户需要针对不同类型的应用程序开发不同的客户端。如对于MapReduce客户端，当用户提交一个MapReduce应用程序时，需通过RPC协议ApplicationClientProtocol与ResourceManager通信，而一旦MapReduce的ApplicationMaster（MRAppMaster）成功启动后，客户端则通过RPC协议MRClientProtocol直接与MRAppMaster通信，以查询应用程序运行状况和控制应用程序的执行。

YarnClient库对常用函数进行了封装，并提供了重试、容错等机制，使用该库可以快速开发一个包含应用程序提交、状态查询和控制等逻辑的YARN客户端，该编程库用法如下：

```Java
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
// ... 其他Java包
private YarnClient client;
// 构造一个YarnClient客户端对象并初始化
this.client = YarnClient.createYarnClient();
client.init(conf);
// 启动YarnClient
yarnClient.start();
// 获取一个新的Application ID
YarnClientApplication app = yarnClient.createApplication();
// 构造ApplicationSubmissionContext，用于提交作业
ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
ApplicationId appId = appContext.getApplicationId();
appContext.setApplicationName(appName);
// ...
yarnClient.submitApplication(appContext);  // 将应用程序提交到resourceManager上
```

### ApplicationMaster设计

分为AM-RM和AM-NM两部分。

ApplicationMaster与ResourceManager之间通信涉及三个步骤：

1. ApplicationMaster通过RPC函数`ApplicationMasterProtocol#registerApplicationMaster`向ResouceManager注册。ApplicationMaster启动时，首先向ResouceManager注册，注册信息封装到Protocol Buffer消息RegisterApplicationMasterRequest中，主要包括以下字段：
   + host：ApplicationMaster本次启动所在的节点host，用户可通过函数`RegisterApplicationMasterRequest#getHost`/`RegisterApplicationMasterRequest#setHost`设置/修改该值
   + rpc_port：ApplicationMaster本次启动对外的RPC端口号，用户可通过函数`RegisterApplicationMasterRequest#getRpcPort`/`RegisterApplicationMasterRequest#setRpcPort`设置/修改该值
   + tracking_url：ApplicationMaster对外提供的追踪Web URL，客户端可通过该tracking_url查询应用程序执行状态，用户可通过函数`RegisterApplicationMasterRequest#getTrackingUrl`/`RegisterApplicationMasterRequest#setTrackingUrl`设置/修改该值。

   ApplicationMaster注册成功后，将收到一个RegisterApplicationMasterResponse类型的返回值，主要包含以下信息：
   + maximumCapability：最大可申请的单个Container占用的资源量，用户可通过函数`RegisterApplicationMasterResponse#getMaximumResouceCapability`获取该值
   + client_to_am_token_master_key：ClientToAMToken master key。用户可通过函数RegisterApplicationMasterResponse#getClientToAMTokenMasterKey获取该值
   + application_ACLs：应用程序访问控制列表，用户可通过函数`RegisterApplicationMasterResponse#getApplicationACLs`获取该值

   ```Java
   // 定义一个ApplicationMasterProtocol协议的RPC Client
   ApplicationMasterProtocol rmClient = （ApplicationMasterProtocol)rpc.getProxy(ApplicationMasterProtocol.class, rmAddress, conf);
   RegisterApplicationMasterRequest request = recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
   synchronized (this) {
     request.setApplicationAttemptId(appAttemptId);
   }
   // 变量赋值
   request.setHost(appHostName);  // 设置所在的host
   request.setRpcPort(appHostPort);  // 设置对外的host端口号
   request.setTrackingUrl(appTrackingUrl);  // 设置tracking URL
   RegisterApplicationMasterResponse response = rmClient.registerApplicationMaster(request);  // 向ResouceManager注册
   ```

2. ApplicationMaster通过RPC函数`ApplicationMasterProtocol#allocate`向ResouceManager申请资源。ApplicationMaster负责将应用程序需要的资源转化成ResourceManager能识别的格式，并通过RPC函数`ApplicationMasterProtocol#allocate`通知ResourceManager。该函数只有一个`AllocateRequest`类型的参数，主要包含以下几个字段：
   + ask：ApplicationMaster请求的资源列表，每个资源请求用`ResourceRequest`表示，用户可使用函数`AllocateRequest#getAskList`/`AllocateRequest#setAskList`获取/设置请求资源列表，`ResourceRequest`包含以下几个字段：
     + priority：资源优先级，为一个正整数，值越小，优先级越高
     + resource_name：期望资源所在的节点或者机架，如果是`*`，表示任何节点上的资源均可以
     + capability：所需的资源量，当前支持CPU和内存两种资源
     + num_containers：需要满足以上条件的资源数目
     + relax_locality：是否松弛本地性，即是否在没有满足节点本地性资源时，自动选择机架本地性资源或者其他资源，默认值是true
   + release：ApplicationMaster释放的container列表，当出现任务运行完成、收到的资源无法使用而主动释放资源或者主动放弃分配的Container等情况时，ApplicationMaster将释放Container。用户可通过函数`AllocateRequest#getReleaseList`/`AllocateRequest#setReleaseList`获取/设置释放的Container列表
   + response_id：本地通信的应答ID，每次通信，该值会加一。用户可使用函数`AllocateRequest#getResponseId`/`AllocateRequest#setResponseId`获取/设置response_id值
   + progress：应用程序执行进度。用户可使用函数`AllocateRequest#getProgress`/`AllocateRequest#setProgress`获取/设置progress值
   + blicklist_request：请求加入/移除黑名单的节点列表，用户可使用函数`AllocateRequest#getResourceBlacklistRequest`/`AllocateRequest#setResourceBlacklistRequest`获取/设置黑名单列表。主要包含以下两个字段
     + blacklist_additions：请求加入黑名单的节点列表
     + blacklist_removals：请求移除黑名单的节点列表

   ApplicationMaster需要周期性调用`ApplicationMasterProtocol#allocate`函数以维持与ResourceManager之间的心跳，同时周期性询问ResourceManager是否存在分配给应用程序的资源，如果有，需主动获取。Application每次调用`ApplicationMasterProtocol@allocate`后，会收到一个`AllocateResponse`类型的返回值，该值包含以下字段：
   + a_m_command：ApplicationMaster需执行的命令，目前主要有`AM_RESYNC`（重启）和`AM_SHUTDOWN`（关闭）两个值。当ResourceManager重启或者应用程序信息出现不一致状态时，可能要求ApplicationMaster重新启动；当节点处于黑名单中时，ResourceManager则让ApplicationMaster关闭，用户可使用`AllocateResponse#getAMCommand`获取该值
   + response_id：本次通信的应答ID，每次通信都会加一。用户可使用函数`AllocateResponse#getResponseId`获取该值
   + allocated_containers：分配给该应用程序的Container列表，ResourceManager将每份可用的资源封装成一个Container，该Container中有关于这份资源的详细信息。ApplicationMaster收到一个Container后，会在这个Container中运行一个任务，用户可使用函数`AllocateResponse#getAllocatedContainers`获取该值
   + completed_container_statuses：运行完成的Container状态列表，该列表中的Container所处的状态可能是运行成功、运行失败和被杀死。用户可使用函数`AllocateResponse#getCompletedContainersStatuses`获取该值
   + limit：目前集群可用的资源总量，可使用函数`AllocateResponse#getAvailableResources`获取该值
   + updated_nodes：当前集群中所有节点运行状态列表，可使用函数`AllocateResponse#getUpdatedNodes`获取该值
   + num_cluster_nodes：当前集群中可用节点总数，可使用函数`AllocateResponse#getNumClusterNodes`获取该值
   + preemt：资源抢占信息，当ResourceManager将要抢占某个应用程序的资源时，会提前发送一个资源列表让ApplicationMaster主动释放这些资源，如果ApplicationMaster在一定时间内未释放这些资源，则强制进行回收。可使用`AllocateResponse#getPreemptionMessage`函数获取该值。preempt中包含以下两类信息：
      + strictContract：必须释放的Container列表，ResourceManager指定要求一定要释放这些Container占用的资源
      + contract：它包含资源总量和Container列表两类信息，ApplicationMaster可释放这些Container占用的资源，或者释放任意几个占用资源总量达到指定资源量的Container
   + nm_tokens：NodeManager Token，可使用函数`AllocateResponse#getNMTokens`获取该值

   ```Java
   // ApplicationMaster向ResourceManager申请资源
   // ... 变量赋值
   while(1) { // 维持与ResourceManager之间的周期性心跳
     synchronized (this) {
       askList = new ArrayList<ResponseRequest>(ask);
       releaseList = new ArrayList<ContainerId>(release);
       allocateRequest = BuilderUtils.newAllocateRequest(appAttemptId, lastResponseId, progressIndicator, askList, releaseList, null);  // 构造一个AllocateRequest对象
     }
     // 向ResourceManager申请资源，同时领取新分配的资源
     allocateResponse = rmClient.allocate(allocateRequest);
     // 根据ResourceManager的应答信息设计接下来的逻辑（如将资源分配任务）
     // ...
     Thread.sleep(1000);
   }
   // ...
   ```

3. ApplicationMaster通过RPC函数`ApplicationMasterProtocol#finishApplicationMaster`告诉ResouceManager应用程序执行完毕，并退出。当ApplicationMaster运行完毕后，它会调用`ApplicationMasterProtocol#finishApplicationMaster`通知ResourceManager，该RPC函数的参数类型为`FinishApplicationMasterRequest`，主要包含以下字段：
   + diagnostics：诊断信息，当ApplicationMaster运行失败时，会记录错误原因以便于后期诊断
   + tracking_url：ApplicationMaster对外提供的追踪Web URL
   + final_application_status：ApplicationMaster最终所处状态，可以是`APP_UNDEFINED`（未定义）、`APP_SUCCEEDED`（运行成功）、`APP_FAILED`（运行失败）、`APP_KILLED`（被杀死）

   成功执行该RPC函数后，ApplicationMaster将收到一个`FinishApplicationMasterResponse`类型的返回值，该返回值目前未包含任何信息

   ```Java
   // ApplicationMaster退出
   FinishApplicationMasterRequest request = recordFactory.newRecordInstance(FinishApplicationMasterRequest.class);
   request.setAppAttemptId(appAttemptId);  // 设置Application ID
   request.setFinishApplicationStatus(appStatus);  // 设置最终状态
   if (appMessage != null) {
     request.setDiagnostice(appMessage);  // 设置诊断信息
   }
   if (appTrackingUrl != null) {
     request.setTrackingUrl(appTrackingUrl);  // 设置trackingURL
   }
   rmClient.finishApplicationMaster(request);  // 通知ResourceManager自己退出    
   ```

YARN为ApplicationMaster与ResourceManager交互部分提供了通用的编程库，阻塞式实现`AMRMClientImpl`（每个函数执行完成后才返回）与非阻塞式实现`AMRMClientAsync`（基于`AMRMClientImpl`实现，ApplicationMaster触发任何一个操作后，`AMRMClientAsync`将之封装成事件放入事件队列后便返回，而事件处理是由一个专门的线程池完成）。当用户需要实现自己的ApplicationMaster时，只需实现回调类`AMRMClientAsync.CallbackHandler`，该类主要提供5个回调函数：

```Java
public interface CallbackHandler {

   // 被调用时机：ResourceManager为ApplicationMaster返回的心跳应答中包含完成的Container信息
   // 如果心跳应答中同时包含完成的Container和新分配的container，则该回调函数将在containersAllocated之前调用
  public void onContainersCompleted(List<ContainerStatus> statuses);

  // 被调用时机：ResourceManager为ApplicationMaster返回的心跳应答中包含新分配的Container信息
  // 如果心跳应答中同时包含完成的Container和新分配的container，则回调函数将在onContainersCompleted之后调用
  public void onContainersAllocated
}
```

ApplicationMaster与NodeManager之间通信涉及三个步骤：

1. ApplicationMaster将申请到的资源二次分配给内部的任务，并通过RPC函数`ContainerManagerProtocol#startContainer`与对应的NodeManager通信以启动Container（包括任务描述、资源描述等信息）该函数的参数类型为StartContainersRequest，主要包含一个类型为`StartContainersRequest`的字段，该类型主要包含以下字段：
   + container_launch_context：封装了Container执行环境，主要包括以下几个字段
      + localResources：Container执行所需的本地资源，如字典文件、JAR包或者可执行文件等，以key/value格式保存
      + tokens：Container执行所需的各种Token
      + service_data：附属服务所需的数据，以key/value格式保存
      + environment：Container执行所需的环境变量，以key/value格式保存
      + command：Container执行命令，需要一条Shell命令
      + application_ACLs：应用程序访问控制列表，以key/value格式保存
   + container_token：Container启动时的安全令牌

   成功执行该函数后，会收到一个`StartContainersResponse`类型的返回值，该值主要包含以下几个字段：
   + services_meta_data：附属服务返回的元数据信息，用户可通过函数`StartContainersResponse#getAllServicesMetaData`获取该值
   + succeeded_requests：成功运行的Container列表，用户可通过函数`StartContainersResponse#getSuccessfullyStartedContainers`获取该值
   + failed_requests：运行失败的Container列表，用户可通过函数`StartContainersResponse#getFailedRequests`获取该值

   ```Java
   // 运行Container
   // ...
   String cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
   InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
   LOG.info("Connecting to ContainerManager at " + cmIpPortStr);
   this.cm = ((ContainerManagementProtocol)rpc.getProxy(ContainerManagementProtocol.class, cmAddress, conf));
   ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
   // 设置ctx变量
   // ...
   StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
   startReq.setContainerLaunchContext(ctx);
   startReq.setContainer(container);
   try {
     cm.startContainer(startReq);
   } catch (YarnRemoteException e) {
     LOG.info("Start container failed for :" + ", containerId=" + container.getId());
     e.printStackTrace();
   }
   ```

2. 为了实时掌握各个Container运行状态，ApplicationMaster可通过RPC函数`ContainerManagerProtocol#getContainerStatus`向NodeManager询问Container运行状态，一旦发现某个Container运行失败，ApplicationMaster可尝试重新为对应的任务申请资源。
3. 一旦一个Container运行完成后，ApplicationMaster可通过RPC函数`ContainerManagementProtocol#stopContainer`释放Container。

另外，在应用程序运行过程中，用户可使用`ApplicationClientProtocol#getApplicationReport`查询应用程序运行状态，也可以使用`ApplicationClientProtocol#forceKillApplication`将应用程序杀死。
