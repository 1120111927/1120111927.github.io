---
title: Reactor Pattern
date: "2020-06-29"
description: 
tags: 
---

传统服务设计：

```bob-svg
.------.            .-------.
|client|            |handler|
`------+            +-------`
        \          /
         \        /
.------.  +------+  .-------.
|client|--|Server|--|handler|
`------`  +------+  `-------`
         /        \
        /          \
.------+            +-------.
|client|            |handler|
`------`            `-------`

handler:

.-------------------------------------------------.
|  .----.  .------.  .-------.  .------.  .----.  |
|  |read|  |decode|  |compute|  |encode|  |send|  |
|  `----`  `------`  `-------`  `------`  `----`  |
`-------------------------------------------------`
```

Reactor是并发编程中的一种基于事件驱动的设计模式，具有以下两个特点：通过派发、分离I/O操作事件提高系统的并发性能；提供了粗粒度的并发控制，使用单线程实现，避免了复杂的同步处理。

典型的Reactor模式中主要包括以下几个角色：
+ Reactor：I/O事件的派发者
+ Acceptor：接受来自Client的连接，建立与Client对应的Handler，并向Reactor注册此Handler
+ Handler：与一个Client通信的实体，并按一定的过程实现业务的处理。Handler内部往往会有更进一步的层次划分，用来抽象诸如read、decode、compute、encode和send等过程。在Reactor模式中，业务逻辑被分散的I/O事件所打破，所以Handler需要有适当的机制在所需的信息还不全的时候保存上下文，并在下一次I/O事件到来的时候能继续上次中断的处理
+ Reader/Sender：Reactor模式一般分离Handler中的读和写两个过程，分别注册成单独的读事件和写事件，并由对应的Reader和Sender线程处理