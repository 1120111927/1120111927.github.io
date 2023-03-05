---
title: RPC
date: "2020-12-20"
description: 
tags: Hadoop
---

```toc
ordered: true
class-name: "table-of-contents"
```

RPC（Remote Procedure Call，远程过程调用） 是一种通过网络从远程计算机上请求服务的协议，通过TCP、UDP等传输协议为通信程序之间传递访问请求或者应答信息，跨越了OSI网络通信模型中的传输层和应用层，是一种常用的分布式网络通信协议，它允许运行于一台计算机的程序调用另一台计算机的子程序，同时将网络的通信细节隐藏起来，使得用户无须额外地为这个交互作用编程。

RPC框架（Thrift、Protocol Buffer、Avro）均由两部分组成：对象序列化和远程过程调用^[Protocol Buffer官方仅提供了序列化实现，为提供远程调用相关实现，但三方RPC库非常多]。

## RPC通信模型

```bob-svg
.-------------------------.      .--------------------------------.
| Client                  |      | Server                         |
| .----. .------. .----.  | 请求 |  .----. .----. .------. .----. |
| | 客 | | Stub | | 通 |--+------+->| 通 | | 调 | | Stub | | 服 | |
| | 户 | |  程  | | 信 |  |      |  | 信 | | 度 | |  程  | | 务 | |
| | 程 | |  序  | | 模 |  | 应答 |  | 模 | | 程 | |  序  | | 过 | |
| | 序 | |      | | 块 |<-+------+--| 块 | | 序 | |      | | 程 | |
| '----' '------' '----'  |      |  '----' '----' '------' '----' |
'-------------------------'      '--------------------------------'
```

RPC通常采用客户机/服务器模型，请求程序是一个客户机，服务提供程序是一个服务器，一个典型的RPC框架主要包括以下几个部分：
+ 通信模块：两个相互协作的通信模块实现请求-应答协议，它们在客户和服务器之间传递请求和应答消息^[请求-应答协议的实现方式有同步方式和异步方式，同步模式下客户端程序一直阻塞到服务器端发送的应答请求到达本地；异步模式下客户端将请求发送到服务器端后，不必等待应答返回，可以做其他事情，待服务器端处理完请求后，主动通知客户端]
+ Stub程序：客户端和服务端均包含Stub程序，可看做代理程序，使得远程函数调用表现得跟本地调用一样，对用户程序完全透明。在客户端，Stub程序将请求信息通过网络模块发送给服务器端，当服务器发送应答后，它会解码对应结果。在服务器端，Stub程序依次进行解码请求消息中的参数、调用相应的服务过程和编码应答结果的返回值等处理
+ 调度程序：调度程序接收来自通信模块的请求消息，并根据其中的标识选择一个Stub程序进行处理
+ 客户程序：请求的发出者^[如果是单机环境，客户程序可直接通过函数调用访问服务过程，但在分布式环境下，需要考虑网络通信，需要增加通信模块和Stub程序，同时保证函数调用的透明性]
+ 服务过程：请求的处理者

通常而言，一个RPC请求从发送到获取处理结果，经历的步骤如下：
1. 客户程序以本地方式调用系统产生的Stub程序
2. Stub程序将函数调用信息按照网络通信模块的要求封装成消息包，并交给通信模块发送到远程服务器端
3. 远程服务器接收此消息后，将此消息发送给相应的Stub程序
4. Stub程序拆封消息，形成被调过程要求的形式，并调用对应函数
5. 被调用函数按照所获参数执行，并将结果返回给Stub程序
6. Stub程序将此结果封装成消息，通过网络通信模块逐级地传送给客户程序

## RMI

## Hadoop RPC

Hadoop各个系统（HDFS、YARN、MapReduce等）均采用了Master/Slave结构，Master实际上是一个RPC Server，负责处理集群中所有Slave发送的服务请求，Slave实际上是RPC Client。

Hadoop RPC主要分为四个部分：
+ 序列化层：将用户请求中的参数或者应答转化成字节流以便跨机器传输。Protocol Buffers和Apache Avro均可用在序列化层，Hadoop本身也提供了一套序列化框架，一个类只要实现了Writable接口即可支持对象序列化与反序列化
+ 函数调用层：定位要调用的函数并执行该函数，Hadoop RPC采用Java反射机制与动态代理实现了函数调用
+ 网络传输层：描述了Client与Server之间消息传输的方式，Hadoop RPC采用了基于TCP/IP的Socket机制
+ 服务器端处理框架：可被抽象为网络I/O模型，描述了客户端与服务器端间信息交互方式，Hadoop RPC采用了基于Reactor设计模式的事件驱动I/O模型

Hadoop RPC（`org.apache.hadoop.ipc.RPC`）对外主要提供了两种接口：
+ `public static <T> ProtocolProxy <T> getProxy/waitForProxy()`：构造一个客户端代理对象，用于向服务器发送RPC请求
+ `public static Server RPC.Builder(Configuration).build()`：为某个协议实例构造一个服务器对象，用于处理客户端发送的请求

Hadoop RPC主要由三个大类组成，即RPC、Client和Server，分别对应对外编程接口、客户端实现和服务器实现。

**ipc.RPC** 实际上是对底层客户机-服务器网络模型的封装，定义了一系列构建（`getProxy()`、`waitForProxy()`）和销毁（`stopProxy()`）RPC客户端的方法，静态内部类`RPC.Builder`用于构建RPC服务器，该类提供了一系列`setXxx()`方法用于设置RPC协议、RPC协议实现对象、服务器绑定地址、端口号等基本参数，设置完成后，即可通过调用`RPC.Builder#build()`完成一个服务器对象的构建，之后直接调用`Server#start()`方法启动该服务器。Hadoop 2.x提供了Writable（WritableRpcEngine）和Protocol Buffers（ProtocolRpcEngine）两种序列化框架，默认实现方式是Writable方式，可通过调用`RPC#setProtocolEngine()`设置序列化方式。Hadoop RPC的远程过程调用流程为，Hadoop RPC使用了Java动态代理完成对远程方法的调用，用户只需实现`java.lang.reflect.InvocationHandler`接口，并按照自己需求实现`invoke()`方法即可完成动态代理类对象上的方法调用，在invoke方法中，将函数调用信息（函数名、函数参数列表等）打包成可序列化的`WritableRpcEngine.Invocation`（或`ProtocolRpcEngine.Invocation`）对象，并通过网络发送给服务器端，服务器端收到该调用信息后，解析出函数名、函数参数列表等信息，利用Java反射机制完成函数调用

**ipc.Client** 发送远程过程调用信息并接收执行结果，对外提供了一类执行远程调用的接口（名称都一样，只是参数列表不同）。Client类有两个重要的内部类，Call和Connection，Call类封装了一个RPC请求，包含5个成员变量（唯一标识id^[Hadoop RPC Server采用异步方式处理客户端请求，这使得远程过程调用的发生顺序与结果返回顺序无直接关系，Client端通过id识别不同的函数调用]、函数调用信息param、函数执行返回值value、出错或者异常信息error、执行完成标识符done），当客户端向服务器端发送请求时，只需填充id和param两个变量，value、error和done则由服务器端根据函数执行情况填充。Connection类封装了Client与每个Server之间维护的通信连接的基本信息及操作，基本信息包括通信连接唯一标识（remoteId）、与Server端通信的Socket（socket）、网络输入数据流（in）、网络输出数据流（out）、保存RPC请求的哈希表（calls），操作包括addCall（将一个Call对象添加到哈希表中）、sendRPCRequest（向服务器发送RPC请求）、receiveResponse（从服务器端接收已经处理完成的RPC请求）、run（Connection是一个线程类，它的run方法调用了receiveResponse方法，会一直等待接收RPC返回结果）。

当调用`call()`函数执行某个远程方法时，Client端处理流程如下：
1. 创建一个Connection对象，并将远程方法调用信息封装成Call对象，放到Connection对象中的哈希表中
2. 调用Connection类中的`sendRpcRequest()`方法将当前Call对象发送给Server端
3. Server端处理完RPC请求后，将结果通过网络返回给Client端，Client端通过`receiveRpcResponse()`函数获取结果
4. Client检查结果处理状态（成功还是失败），并将对应Call对象从哈希表中删除

**ipc.Server** 接受来自客户端的RPC请求，经过调用相应的函数获取结果后，返回给对应的客户端^[Hadoop采用了Master/Slave结构，Master通过ipc.Server接收并处理所有Slave发送的请求，ipc.Server采用了线程池、事件驱动、Reactor设计模式等提高并发处理能力的技术来实现高并发和可扩展性]。ipc.Server实际上实现了一个典型的Reactor设计模式，分为3个阶段：
+ 接收请求：接收来自各个客户端的RPC请求，并将它们封装成固定的格式（Call类）放到一个共享队列中（callQueue）中，以便进行后续处理。该阶段内部又分为建立连接和接收请求两个子阶段，分别由Listener和Reader两种线程完成。整个Server只有一个Listener线程，统一负责监听来自客户端的连接请求，一旦有新的请求到达，它会采用轮询的方式从线程池中选择一个Reader线程进行处理，而Reader线程可同时存在多个，它们分别负责接收一部分客户端连接的RPC请求，每个Reader线程负责哪些客户端连接由Listener决定，当前Listener只是采用了简单的轮询分配机制。Listener和Reader线程内部各自包含一个Selector对象，分别用于监听SelectionKey.OP_ACCEPT和SelectionKey.OP_READ事件。对于Listener线程，主循环监听是否有新的连接请求到达，并采用轮询策略选择一个Reader线程处理新连接；对于Reader线程，主循环监听它负责的客户端连接中是否有新的RPC请求到达，并将新的RPC请求封装成Call对象，放到共享队列callQueue中
+ 处理请求：从共享队列callQueue中获取Call对象，执行相应的函数调用，并将结果返回给客户端，这全部由Handler线程完成。Server端可同时存在多个Handler线程，它们并行从共享队列中读取Call对象，经执行对应的函数调用后，将尝试着直接将结果返回给对应的客户端
+ 返回结果：当函数调用返回结果很大或者网络速度过慢不能将结果一次性发送到客户端时，Handler将尝试着将后续发送任务交给Responder线程。Server端仅存在一个Responder线程，它的内部包含一个Selector对象，用于监听SelectionKey.OP_WRITE事件。当Handler没能将结果一次性发送到客户端时，会向该Selector对象注册SelectionKey.OP_WRITE事件，进而由Responder线程采用异步方式继续发送未发送完成的结果

```Java
// 使用Hadoop RPC可分为以下4个步骤：
// 1. 定义RPC协议，RPC协议是客户端和服务器端之间的通信接口，定义了服务器端对外提供的服务接口
interface ClientProtocol extends org.apache.hadoop.ipc.VersionedProtocol {
    // 版本号，默认情况下，不同版本号的RPC Client和Server之间不能相互通信
    public static final long versionID = 1L;
    String echo(String value) throws IOException;
    int add(int v1, int v2) throws IOException;
}

// 2. 实现RPC协议，Hadoop RPC协议通常是一个Java接口
public static class ClientProtocolImpl implements ClientProtocol {
    // 获取自定义的协议版本号
    public long getProtocolVersion(String protocol, long clientVersion) {
        return ClientProtocol.versionID;
    }
    // 获取协议签名
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int hashcode) {
        return new ProtocolSignature(ClientProtocol.versionID, null);
    }
    public String echo(String value) throws IOException {
        return value;
    }
    public int add(int v1, int v2) throws IOException {
        return v1 + v2;
    }
}

// 3. 构造并启动RPC Server，直接使用静态类Builder构造一个RPC Server，并调用函数start()启动该Server
Server server = new RPC.Builder(conf)
                       .setProtocol(ClientProtocol.class)
                       .setInstance(new ClientProtocolImpl())
                       .setBindAddress(ADDRESS)           // 服务器host
                       .setPort(0)                        // 服务器监听端口号，0表示由系统随机选择一个端口号
                       .setNumHandlers(5)                 // 服务器端处理请求的县城数目
                       .build();
server.start();         // 服务器处于监听状态，等待客户端请求到达

// 4. 构造RPC Client并发送RPC请求，使用静态方法getProxy构造客户端代理对象，通过代理对象调用远程方法
proxy = (ClientProtocol)RPC.getProxy(ClientProtocol.class, ClientProtocol.versionID, addr, conf);
int result = proxy.add(5, 6);
String echoResult = proxy.echo("result");
```
### Yarn RPC

YARN将Hadoop RPC中的序列化部分剥离开，集成现有的开源RPC框架^[RPC类变成了一个工厂，它将具体的RPC实现授权给RpcEngine实现类，现有开源RPC类只要实现RpcEngine接口，便可以集成到Hadoop RPC中]，配置项`rpc.engine.{protocol}`用于指定协议{protocol}采用的序列化方式。

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
skinparam ArrowThickness 1

class RPC {
    Map<Class, RpcEngine> PROTOCOL_ENGINES
    Map<Class, RpcEngine> PROXY_ENGINES
    Object[][] call()
    Object getProxy()
    RPC.Server getServer()
    stopProxy()
    Object waitForProxy()
    RpcEngine getProtocolEngine()
    RpcEngine getProxyEngine()
    setProtocolEngine()
}

interface RpcEngine {
    Object getProxy()
    Object stopProxy()
    Object[] call()
    RPC.Server getServer()
}

class WritableRpcEngine
class AvroRpcEngine
class ProtobufRpcEngine

RPC o-- RpcEngine
RpcEngine <|-- WritableRpcEngine
RpcEngine <|-- AvroRpcEngine
RpcEngine <|-- ProtobufRpcEngine

@enduml
```

YARN提供的对外类是YarnRPC，只需使用该类便可以构建一个基于Hadoop RPC且采用Protocol Buffers序列化框架的通信协议。YarnRPC是一个抽象类，配置项`yarn.ipc.rpc.class`设置实际的实现，默认值是`org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC`。HadoopYarnProtoRPC通过RPC工厂生成器（工厂设计模式）RpcFactoryProvider生成客户端工厂（由配置项yarn.ipc.client.factory.class设置，默认值是org.apache.hadoop.yarn.factories.impl.pb.RpcClientFactoryPBImpl）和服务器工厂（由配置项yarn.ipc.server.factory.class设置，默认值是org.apache.hadoop.yarn.factories.impl.pb.RpcServerFactoryPBImpl），以根据通信协议的Protocol Buffers定义生成客户端对象和服务器对象

+ RpcClientFactoryPBImpl：根据通信协议接口及Protocol Buffers定义构造RPC客户端句柄^[RpcClientFactoryPBImpl对通信协议的存放位置和类命名有一定要求，假设通信协议接口Xxx所在Java包名为XxxPackage，则客户端实现代码必须位于Java包XxxPackage.impl.pb.client中（在接口包名后面增加.impl.pb.client），切实现类名为PBClientImplXxx（在接口名前面增加前缀PBClientImpl）]
+ RpcServerFactoryPBImpl：根据通信协议接口及Protocol Buffers定义构造RPC服务器句柄^[对通信协议的存放位置和类命名有一定要求。假设通信协议接口Xxx所在Java包名为XxxPackage，则客户端实现代码必须位于Java包 XxxPackage.impl.pb.server中（在接口包名后面增加.impl.pb.server），且实现类名为PBServiceImplXxx（在接口名前面增加前缀PBServiceImpl）]

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml

hide empty members
skinparam ArrowThickness 1

abstract class YarnRPC {
    Object getProxy()
    stopProxy()
    Server getServer()
    YarnRPC create()
}

class HadoopYarnProtoRPC {
    Object getProxy()
    stopProxy()
    Server getServer()
}

class RpcFactoryProvider {
    RpcServerFactory getServerFactory()
    RpcClientFactory getClientFactory()
}

class RpcServerFactoryPBImpl {
    Server getServer()
}

class RpcClientFactoryPBImpl {
    Object getClient()
    stopClient()
}

interface RpcClientFactory {
    Object getClient()
    stopClient()
}

interface RpcServerFactory {
    Server getServer()
}

YarnRPC <|-- HadoopYarnProtoRPC
YarnRPC ..> HadoopYarnProtoRPC

HadoopYarnProtoRPC <.. RpcFactoryProvider

RpcFactoryProvider <.. RpcServerFactoryPBImpl
RpcFactoryProvider <.. RpcClientFactoryPBImpl

RpcServerFactory <|.. RpcServerFactoryPBImpl
RpcClientFactory <|.. RpcClientFactoryPBImpl

@enduml
```

## Flink RPC

Flink使用Akka作为Flink自身消息的RPC通信框架。

Akka是一个开发并发、容错和可伸缩应用的框架，是Actor模型的一个实现。在Actor模型中，所有的实体被认为是独立的Actor，Actor和其他Actor通过发送异步消息通信（也可以使用同步模式执行同步操作，但是会限制系统的伸缩性），每个Actor都有一个邮箱（Mailbox），用于存储所收到的消息，另外，每一个Actor维护自身单独的状态。每个Actor是一个单一的线程，不断从其邮箱中拉取消息，并且连续不断地处理，对于已经处理过的消息的结果，Actor可以改变它自身的内部状态，或者发送一个新消息，或者孵化一个新的Actor。

### Akka

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

### RPC消息类型

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

### RPC通信组件

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

### RPC交互过程

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