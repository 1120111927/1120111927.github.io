---
title: Hadoop RPC
date: "2020-12-20"
description: 
tags: Hadoop
---

Protocol Buffer是一种轻便高效的结构化数据存储格式，可以用于结构化数据序列化/反序列化，适合做数据存储或RPC的数据交换格式，常用作通信协议、数据存储等领域的与语言无关、平台无关、可扩展的序列化结构数据格式。

通常编写一个Protocol Buffer应用需要以下三步：

1. 定义消息格式文件，通常以proto作为扩展名

2. 使用Google提供的Protocol Buffer编译器生成特定语言的代码文件

        protoc -I=$SRC_DIR --java_out=$DST_DIR $SRC_DIR/input.proto

3. 使用Protocol Buffer库提供的API读写消息
