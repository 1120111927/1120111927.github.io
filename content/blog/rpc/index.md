---
title: RPC
date: "2021-01-23"
description: RPC是一种常用的分布式网络通信协议，允许运行于一台计算机的程序调用另一台计算机的子程序，同时将网络的通信细节隐藏起来，使得用户无须额外地为这个交互作用编程，大大简化了分布式程序开发
tags: RPC
---

RPC（Remote Procedure Call，远程过程调用）是一种通过网络从远程计算机上请求服务的协议，通过TCP、UDP等传输协议为通信程序之间传递访问请求或者应答信息，跨越了OSI网络通信模型中的传输层和应用层。

RPC通常采用Client/Server模型，一个典型的RPC框架主要包括以下几个部分：

+ 通信模块：两个相互协作的通信模块实现请求-应答协议，在客户和服务器之间请求和应答消息。请求-应答协议的实现方式有同步方式和异步方式两种。同步模式下客户端程序一直阻塞到服务端发送的应答请求到达本地；异步模式下客户端将请求发送到服务端后不必等待应答返回，可以做其他事情，待服务器处理完请求后，主动通知客户端。在高并发应用场景中，一般采用异步模式以降低访问延迟和提高带宽利用率。

+ Stub程序：代理程序，客户端和服务器端均包含Stub程序，它使得远程函数调用表现得跟本地调用一样，对用户程序完全透明。在客户端，它将请求信息通过网络模块发送给服务器端，当服务器发送应答后，它会解码对应结果。在服务器端，它依次进行解码请求消息中的参数、调用相应的服务过程和编码应答结果的返回值等处理

+ 调度程序：调用程序接收来自通信模块的请求信息，并根据其中的标识选择一个Stub程序进行处理

+ 客户程序：请求的发出者

+ 服务过程：请求的处理者

一个RPC请求从发送到获取处理结果，一般经理以下步骤：

1. 客户程序以本地方式调用系统产生的Stub程序

2. Stub程序将函数调用信息按照网络通信模块的要求封装成消息包，并交给通信模块发送到远程服务端

3. 远程服务器端接收到此消息后，将此消息发送给相应的Stub程序

4. Stub程序拆封消息，形成被调过程要求的形式，并调用对应函数

5. 被调用函数按照所获参数执行，并将结果返回给Stub程序

6. Stub程序将此结果封装成消息，通过网络通信模块逐级地传送给客户程序

## Apache Thrift

Apache Thrift采用接口描述语言定义并创建服务，是对IDL（Interface Definition Language，接口描述语言）的一种一种具体实现。

### 数据类型

Thrift脚本可定义的数据类型包括以下几种：

+ 基本类型：
  + `bool`：布尔值，`true`或`false`，对应Java的`boolean`
  + `byte`：8位有符号整数，对应Java的`byte`
  + `i16`：16位有符号整数，对应Java的`short`
  + `i32`：32位有符号整数，对应Java的`int`
  + `i64`：64位有符号整数，对应Java的`long`
  + `double`：64位浮点数，对应Java的`double`
  + `string`：未知编码文本或二进制字符串，对应Java的`String`

+ 结构体类型：
  + `struct`：定义公共的对象，类似C语言中的结构体定义，在Java中是一个JavaBean

+ 容器类型：
  + `list`：对应Java的`ArrayList`
  + `map`：对用Java的`HashMap`

+ 异常类型：
  + `exception`：对应Java的Exception

+ 服务类型：
  + `service`：对应服务的类

### 安装thrift

1. 安装相关依赖包

   ```bash
   sudo apt-get install automake bison flex g++ git libboost-all-dev libevent-dev libssl-dev libtool make pkg-config
   ```

2. 下载源码

   ```bash
   wget https://mirror.bit.edu.cn/apache/thrift/0.13.0/thrift-0.13.0.tar.gz
   ```

3. 编译源码并安装

   ```bash
   tar -xf thrift-0.13.0.tar.gz
   cd thrift-0.13.0
   ./configure && make
   sudo make install
   ```

4. 验证thrift是否安装好

   ```bash
   thrift -version
   ```

### 开发

1. 编写thrift脚本（后缀`.thrift`）
2. 编译thrift脚本

    ```bash
    thfift -r --gen <language> <Thrift filename>
    ```

## gRPC

## Hadoop RPC
