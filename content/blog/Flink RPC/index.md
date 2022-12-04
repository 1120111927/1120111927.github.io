---
title: Flink RPC
date: "2022-12-03"
description: 
tags: 
---

Flink使用Akka作为Flink自身消息的RPC通信框架。

Akka是一个开发并发、容错和可伸缩应用的框架，是Actor模型的一个实现。在Actor模型中，所有的实体被认为是独立的Actor，Actor和其他Actor通过发送异步消息通信（也可以使用同步模式执行同步操作，但是会限制系统的伸缩性），每个Actor都有一个邮箱（Mailbox），用于存储所收到的消息，另外，每一个Actor维护自身单独的状态。每个Actor是一个单一的线程，不断从其邮箱中拉取消息，并且连续不断地处理，对于已经处理过的消息的结果，Actor可以改变它自身的内部状态，或者发送一个新消息，或者孵化一个新的Actor。

## Akka

Akka系统的核心是ActorSystem和Actor，Actor只能通过`ActorSystem#actorOf`和`ActorContext#actorOf`创建，另外，只能通过Actor引用`ActorRef`来与Actor进行通信。

actor是以一种严格的树形结构样式来创建的，沿着子actor到父actor的监管链，一直到actor系统的根存在一条唯一的actor名字序列，被称为路径。一个actor路径都有一个地址组件（"akka"表示Akka本地协议，"akka.tcp"表示Akka远程协议），描述访问这个actor所需要的协议和位置，之后是从根到指定actor所经过的树节点上actor的名字（以"/"分隔），遵循URI结构标准（`[协议名]://[ActorSystem名称]@[主机名]:[端口]/[路径]`）。在路径树的根上是根监管者，所有其他actor都可以通过它找到，其名字是"/"，在第二个层次上是以下这些：
+ "/user" 是所有由用户创建的顶级actor的监管者，用`ActorSystem#actorOf`创建的actor在其下
+ "/system" 是所有由系统创建的顶级actor的监管者
+ "/deadLetters" 是死信actor，所有发往已经终止或不存在的actor的消息会被重定向到这里
+ "/temp"是所有系统创建的短时actor的监管者
+ "/remote" 是一个人造虚拟路径，用来存放所有其监管者是远程actor引用的actor

Akka有两种核心的异步通信方式：
+ tell方式：表示仅仅使用异步方式给某个Actor发送消息，无须等待Actor的响应结果，并且也不会阻塞后续代码的运行，`ActorRef#tell(message, actorRef)`第一个参数为消息，可以是任何可序列化的数据或对象，第二个参数表示发送者（发送消息的Actor的引用，`ActorRef.noSender()`表示无发送者）
+ ask方式：将返回结果包装在`scala.concurrent.Future`中，然后通过异步回调获取返回结果，用于需要从Actor获取响应结果的场景

## RPC消息类型

1. 握手消息
    + RemoteHandshakeMessage：与Actor握手消息
    + HandshakeSuccessMessage：与Actor握手成功消息
2. Fenced消息：用来防止集群的脑裂（Brain Split）问题，每个TaskManager向JobMaster注册之后，都会拿到当前Leader JobMaster的ID作为Fence Token，其他JobMaster发送的消息因为其JobMaster ID与期望的Fence Token不一样就会被忽略掉
    + LocalFencedMessage：本地Fence Token消息，同一个JVM内的调用
    + RemoteFencedMessage：远程Fence Token消息，包括本地JVM和跨节点的JVM调用
3. 调用消息：
    + LocalRpcInvocation：本地RpcEndpoint调用消息，同一个JVM内的调用
    + RemoteRpcInvocation：远程RpcEndpoint调用消息，包括本地不同JVM和跨节点的JVM调用
4. 执行消息：RunAsync，带有Runnable对象的异步执行请求信息。`RpcEndpoint.runAsync`方法调用`RpcService.runAsync`，然后调用`RpcService.scheduleRunAsync`，`RpcService.scheduleRunAsync`调用`AkkaInvocationHandler.tell`方法发送RunAsync消息

## RPC通信组件

**RpcGateway** 远程调用网关，是Flink远程调用的接口协议，提供了行为定义，对外提供可调用的接口，所有实现RPC的组件、类都实现了此接口。

```Java
public interface RpcGateway {
    String getAddress();
    String getHostname();
}
```

接口继承体系：

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
skinparam ArrowThickness 1

interface RpcGateway
interface TaskExecutorGateway
interface FencedRpcGateway
interface JobMasterGateway
interface ResourceManagerGateway
interface DispatcherGateway

RpcGateway <|-- TaskExecutorGateway
RpcGateway <|-- FencedRpcGateway
RpcGateway <|-- JobMasterGateway
RpcGateway <|-- DispatcherGateway
FencedRpcGateway <|-- JobMasterGateway
FencedRpcGateway <|-- ResourceManagerGateway
FencedRpcGateway <|-- DispatcherGateway

@enduml
```

+ `JobMastergateway`接口是JobMaster提供的对外服务接口
+ `TaskExecutorGateway`接口是TaskManager提供的对外服务接口，实现类是`TaskExecutor`
+ `ResourceManagerGateway`接口是ResourceManager资源管理器提供的对外服务接口
+ `DispatcherGateway`是Flink提供的作业提交接口

组件之间的通信行为都是通过RpcGateway进行交互的，JobMaster、ResourceManager、Dispatcher在高可用模式下，由于涉及Leader选举，可能导致集群的脑裂问题，所以都继承了`FencedRpcGateway`。

**RpcEndpoint** 在RpcGateway基础上提供了RPC服务组件的生命周期管理，Flink中所有提供远程调用服务的组件（Dispatcher、JobManager、ResourceManager、TaskExecutor等）都继承自RpcEndpoint。同一个RpcEndpoint中的所有调用只有一个线程处理，称为Endpoint主线程^[与Akka的Actor模型一样，所有对状态数据的修改在同一个线程中执行，所以不存在并发问题]。RpcEndpoint是RpcService、RpcServer的结合之处，在其本身的构造过程中使用`RpcService#startServer()`启动RpcServer，进入可以接收处理请求的状态，最后再将RpcServer绑定到主线程上真正执行起来。

```Java
public abstract class RpcEndpoint implements RpcGateway, AutoCloseableAsync {
    protected RpcEndpoint(final RpcService rpcService, final String endpointId) {
        this.rpcService = checkNotNull(rpcService, "rpcService");
        this.endpointid = checkNotNull(endpointId, "endpointId");
        // 调用RpcService启动Rpcendpoint
        this.rpcServer = rpcService.startServer(this);

        this.mainThreadExecutor = new MainThreadExecutor(rpcServer, this::validateRunsInMainThread);
    }
}
```

**RpcService** 是RpcEndpoint的成员变量，作用如下：
+ 启动和停止RpcServer和连接RpcEndpoint
+ 根据指定的连接地址，连接到RpcServer会返回一个RpcGateway（分为带FencingToken和不带FencingToken的版本）
+ 延迟/立刻调度Runnable、Callable

RpcService会在ClusterEntrypoint（JobMaster）和TaskManagerRunner（TaskExecutor）启动的过程中被初始化并启动。AkkaRpcService是RpcService的唯一实现，AkkaRpcService中包含了一个ActorSystem，保存了ActorRef和RpcEndpoint之间的映射关系，RpcService也提供了获取地址和端口的方法。RpcService会根据RpcEndpoint（Fenced和非Fenced）的类型构建不同的AkkaRpcActor（Fenced和非Fenced），并保存AkkaRpcActor引用和RpcEndpoint的对应关系，创建出来的AkkaRpcActor是底层Akka调用的实际接收者，RPC的请求在客户端被封装成RpcInvocation对象，以Akka消息的形式发送。同时也要完成RpcServer的构建，RpcServer也分为Fenced与非Fenced两类，最终通过Java的动态代理将所有的消息调用转发到InvocationHandler。

```Java
// AkkaRpcService.java
public <C extendsRpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
    // 生成一个包含这些接口的代理，将调用转发到InvocationHandler
    RpcServer server = (RpcServer)Proxy.newProxyInstance(classLoader, implemented RpcGateways.toArray(new Class<?>[implementedRpcGateway.size()]), akkaInvocationHandler)
    return server;
}
```

**RpcServer** 是`RpcEndpoint`的成员变量，负责接收响应远端的RPC消息请求，有`AkkaInvocationHandler`和`FencedAkkaInvocationHandler`两种实现。`RpcServer`的启动实质上是通知底层的AkkaRpcActor切换到START状态，开始处理远程调用请求。

```Java
class AkkaInvocationHandler implements InvocationHandler, AkkaBasedEndpoint, RpcServer {
    @Override
    public void start() {
        rpcEndpoint.tell(ControlMessages.START, ActorRef.noSender());
    }
}
```

**AkkaRpcActor** 是Flink集群内部通信的具体实现，负责处理以下类型消息：
+ LocalRpcInvocation（本地Rpc调用）：LocalRpcInvocation类型的调用指派给`RpcEndpoint`进行处理，如果有响应结果，则将响应结果返还给Sender
+ RunAsync & CallAsync：RunAsync、CallAsync类型的消息带有可以执行的代码，直接在Actor的线程中执行
+ ControlMessage（控制消息）：ControlMessage用来控制Actor的行为，ControlMessage#START启动Actor开始处理消息，ControlMessage#STOP停止处理消息，停止后收到的消息会被丢弃掉

## RPC交互过程

Flink中RPC通信底层的RPC过程分为请求和响应两类。

**RPC请求发送** 在`RpcService`中调用`connect()`方法与对端的`RpcEndpoint`建立连接，`connect()`方法根据给的地址返回`InvocationHandler`（AkkaInvocationHandler或者FencedAkkaInvocationHandler），调用`InvocationHandler`的`invoke`方法并传入RPC调用的方法和参数信息。`AkkaInvocationHandler#invoke()`方法中判断方法所属的类，如果是RPC方法，则调用`invokeRpc`方法，将方法调用封装为RPCInvocation消息。如果是本地则生成LocalRPCInvocation，本地消息不需要序列化，如果是远程调用则创建RemoteRpcInvocation。`invokeRpc`方法中判断远程方法调用是否需要等待结果，如果无须等待，则使用Actor发送tell类型的消息，如果需要返回结果，则向Actor发送ask类型的消息
```Java
class AkkaInvocationHandler implements InvocationHandler, AkkaBaseEndpoint, RpcServer {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Class<?> declaringClass = method.getDeclaringClass();

        Object result;
        // 判断方法所属Class
        if (declaringClass.equals(AkkaBasedEndpoint.class)
            || declaringClass.equals(Object.class)
            || declaringClass.equals(RpcGateway.class)
            || declaringClass.equals(StartStoppable.class)
            || declaringClass.equals(MainThreadExecutable.class)
            || declaringClass.equals(RpcServer.class)) {
            result = method.invoke(this, args);
        } else if (declaringClass.equals(FencedRpcGateway.class)) {
            // ...
        } else {
            // rpc调用
            result = invokeRpc(method, args);
        }
        return result;
    }
    private Object invokeRpc(Method method, Object[] args) throws Exception {
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        Time futureTimeout = extractRpcTimeout(parameterAnnotations, args, timeout);

        final RpcInvocation rpcInvocation = createRpcInvocationMessage(methodName, parameterTypes, args);

        Class<?> returnType = method.getReturnType();
        final Object result;
        if (Objects.equals(returnType, Void.TYPE)) {
            tell(rpcInvocation);
            result = null;
        } else {
            // 异步调用等待返回
            CompletableFuture<?> resultFuture = ask(rpcInvocation, futureTimeout);
        }
        return result;
    }
}
```
**RPC请求响应** AkkaRpcActor是消息接收的入口，AkkaRpcActor在RpcEndpoint中构造生成，负责将消息交给不同的方法进行处理

```Java
// AkkaRpcActor.java
@Override
public Receive createReceive() {
    return ReceiveBuilder.create()
        .match(RemoteHandshakeMessage.class, this::handleHandshakeMessage
        .match(ControlMessages.class, this::handleControlMessage))
        .matchAny(this::handleMessage)
        .build();
}
```

AkkaRpcActor接收到的消息共有3种：
+ 握手消息：在客户端构造时会通过ActorSelection发送过来，收到消息后会检查接口、版本是否匹配，如果一致就返回成功
```Java
// AkkaRpcActor.java
private void handleHandshakeMessage(RemoteHandshakeMessage handshakeMessage) {
    if (!isCompatibleVersion(handshakeMessage.getVersion())) {
        // 版本不兼容异常处理
    } else if (!isGatewaySupported(handshakeMessage.getRpcGateway())) {
        // RpcGateway不匹配异常处理
    } else {
        getSender().tell(new Status.Success(HandshakeSuccessMessage.INSTANCE, getSelf()));
    }
}
```
+ 控制消息：
```Java
// AkkaRpcActor.java
private void handleControlMessage(ControlMessages controlMessage) {
    switch (controlMessage) {
        case START: state = state.start(this); break;
        case STOP: state = state.stop(); break;
        case TERMINATE: state.terminate(this); break;
        default: handleUnknownControlMessage(controlMessage);
    }
}
```
+ RPC消息：通过解析RpcInvocation获取方法名和参数类型，并从RpcEndpoint类中找到Method对象，通过反射调用该方法，如果有返回结果，会以Akka消息的形式返回给发送者
```Java
// AkkaRpcActor.java
private void handleMessage(final Object message) {
    if (state.isRunning()) {
        mainThreadValidator.enterMainThread();
        try {
            handleRpcMessage(message);
        } finally {
            mainThreadValidator.exitMainThread();
        }
    } else {
        // 异常处理
    }
}
```
