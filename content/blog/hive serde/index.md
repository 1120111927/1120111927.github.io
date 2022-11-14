---
title: Hive SerDe
date: "2022-10-11"
description: 
tags: 
---

SerDe构建在数据存储和执行引擎之间，实现数据存储和执行引擎的解耦，用于序列化^[从Java对象序列化为Hadoop Writable对象]/反序列化^[从Hadoop Writable对象反序列化为Java对象]数据（不仅用于对原始数据的读取和最终结果数据的写入，即InputFormat中RecordReader读取数据的解析和最终结果的保存，还用于中间数据的保存和传输，即shuffle过程中内部对象的序列化和反序列化），并提供一个辅助类ObjectInspector用于访问需要序列化/反序列化的对象。

SerDe（Serialize接口和Deserialize接口）实现为抽象类AbstractSerDe，