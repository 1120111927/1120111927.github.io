---
title: Hive函数
date: "2022-04-10"
description: 
tags: 
---

```toc
ordered: true
class-name: "table-of-contents"
```

语法规则定义：

```
function
    :
    functionName
    LPAREN
      (
        (STAR)
        | (KW_DISTINCT | KW_ALL)? (selectExpression (COMMA selectExpression)*)?
      )
    RPAREN (KW_OVER window_specification)?
    ;

window_specification
    :
    (identifier | ( LPAREN identifier? partitioningSpec? window_frame? RPAREN))
    ;

partitioningSpec
    :
    partitionByClause orderByClause?
    | orderByClause
    | distributeByClause sortByClause?
    | sortByClause
    | clusterByClause
    ;

window_frame
    :
    window_range_expression
    | window_value_expression
    ;

window_range_expression
    :
    KW_ROWS window_frame_start_boundary
    | KW_ROWS KW_BETWEEN window_frame_boundary KW_AND window_frame_boundary
    ;

window_value_expression 
    :
    KW_RANGE window_frame_start_boundary
    | KW_RANGE KW_BETWEEN window_frame_boundary KW_AND window_frame_boundary
    ;

window_frame_start_boundary 
    :
    KW_UNBOUNDED KW_PRECEDING
    | KW_CURRENT KW_ROW
    | Number KW_PRECEDING
    ;

window_frame_boundary
    :
    KW_UNBOUNDED (KW_PRECEDING|KW_FOLLOWING)
    | KW_CURRENT KW_ROW
    | Number (KW_PRECEDING | KW_FOLLOWING)
    ;

```

## SerDe

SerDe构建在数据存储和执行引擎之间，实现数据存储和执行引擎的解耦，用于序列化/反序列化数据（不仅用于对原始数据的读取和最终结果数据的写入，即InputFormat中RecordReader读取数据的解析和最终结果的保存，还用于中间数据的保存和传输，即shuffle过程中内部对象的序列化和反序列化），并提供一个辅助类ObjectInspector用于访问需要序列化/反序列化的对象。

Hive SerDe库位于org.apache.hadoop.hive.serde2中，Hive内置了一些SerDe并且支持自定义SerDe。Hive 0.14中新增关键字STORED AS替换三元组(SerDe, InputFormat, OutputFormat)在CreateTable语句中设置SerDe。
Hive使用SerDe和FileFormat来读写表中的行数据，注意“键”部分在读取时会被忽略，而在写入时始终是常数，基本上行对象存储在“值”中：
+ 读过程：HDFS files -> InputFileFormat -> \<key, value> -> Deserializer -> Row object。Hive首先使用InputFormat读取数据记录（由InputFormat的FileReader返回的值对象），然后调用Serde.deserialize()对记录进行反序列化，调用Serde.getObjectInspector()获取ObjectInspector（structObjectInspector 的子类，因为记录本质上是结构类型），最后将反序列化的对象和ObjectInspector传给Operator来从记录中获取需要的数据。
+ 写过程：Row object -> Serializer -> \<key, value> -> OutputFileFormat -> HDFS files。Hive将记录对象转化为OutputFormat指定类型的对象，先将记录的反序列化对象和对应的ObjectInspector传给Serde.serialize()，然后serialize()方法通过传入的ObjectInspector从记录中获取各个字段并将记录转换为指定的类型。

抽象类AbstractSerDe实现了SerDe（Serializer接口和Deserializer接口），Serializer接口包含的`serialize(object, objectInspector)`方法通过ObjectInspector遍历Java对象内部结构将其序列化为Hadoop Writable对象，Deserializer接口包含的`deserialize(writable)`方法将Hadoop Writable对象反序列化为Java对象，`getObjectInspector()`方法返回用于遍历`deserialize()`方法返回Java对象内部结构的ObjectInspector。

Hive使用ObjectInspector分析复杂对象的内部结构，一个ObjectInspector对象描述了对应类型在内存中存储数据的方式，还提供了获取该类型对象内部字段的方法。ObjectInspector接口包含的`getTypeName()`方法返回该ObjectInspector对应的类型名称（int、list<int>、map<int,string>、java类名、用户自定义类型名称）等，`getCategory()`返回ObjectInspector对应的Catagory，包括以下几种：
+ Category.PRIMITIVE：表示ObjectInspector对象继承自PrimitiveObjectInspector
+ Category.LIST：表示ObjectInspector对象继承自ListObjectInspector
+ Category.MAP：表示ObjectInspector对象继承自MapObjectInspector
+ Category.STRUCT：表示ObjectInspector对象继承自StructObjectInspector
+ Category.UNION：表示ObjectInspector对象继承自UnionObjectInspector

ObjectInspector采用工厂模式来保证一种ObjectInspector只有一个实例，创建XxxObjectInspector对象时需使用ObjectInspectorFactory#getXxxObjectInspector()。

SettableObjectorInspector用于设置字段或创建对象。ObjectInspector用于读取字段，但是不能设置字段或创建对象。通过SettableObjectorInspector可以将Integer（对应JavaIntObjectInspector）转化为IntWritable（对应WritableIntObjectInspector）。非基于GenericUDF实现的UDF先使用Java反射获取参数和返回值的类型，然后使用ObjectInspectorUtils.getStandardObjectInspector来推断ObjectInspector。基于GenericUDF/GenericUDAF实现的UDF可以根据传入UDF的对象对应的ObjectInspector和UDF参数类型对应的ObjectorInspector来创建ObjectInspectorConverter对象并通过SettableObjectInspector接口转化对象。

<div class="wrapper" markdown="block">

|数据类型|ObjectInspector|创建实例|方法|备注|
|---|---|---|---|--|
|Primitive基本类型|PrimitiveObjectInspector|使用PrimitiveObjectInspectorFactory的以下方法：<br>getPrimitiveWritableObjectInspector(<br>primitiveCategory)<br>getPrimitiveJavaObjectInspector(<br>primitiveCategory)|getPrimitiveCategory()：返回基本类型对应的PrimitiveCategory<br>getPrimitiveWritableObject(object)：返回基本Writable对象<br>getPrimitiveJavaObject(object)：返回Java基本对象|PrimitiveCategory可选值及对应的PrimitiveObjectInspector实现:<br>PrimitiveCategory.BOOLEAN->BooleanObjectInspector/JavaBooleanObjectInspector/WritableBooleanObjectInspector<br>PrimitiveCategory.INT->IntObjectInspector/JavaIntObjectInspector/WritableIntObjectInspector<br>PrimitiveCategory.STRING->StringObjectInspector/JavaStringObjectInspector/WritableStringObjectInspector<br>...|
|List结构|ListObjectInspector<br>StandardListObjectInspector|使用ObjectInspectorfactory的<br>getStandardListObjectInspector(<br>listElementObjectInspector)|getListElementObjectInspector()：返回List中元素的类型对应的ObjectInspector<br>getListElement(dataObject, index)：返回List指定位置的元素，dataObject为null或index超范围时返回-1<br>getListLength(dataObject)：返回List长度，dataObject为null时返回-1<br>getList(dataObject)：返回List对象| |
|Map结构|MapObjectInspector<br>StandardMapObjectInspector|使用ObjectInspectorFactory的<br>getStandardMapObjectInspector(<br>mapKeyObjectInspector,<br>mapValueObjectInspector)|getMapKeyObjectInspector()：返回Map中Key对应的ObjectInspector<br>getMapValueObjectInspector()：返回Map中Value对应的ObjectInspector<br>getMapValueElement(dataObject, keyObject)：返回Map中指定key对应的值，dataObject为null或keyObject为null时返回null<br>getMapSize(dataObject)：返回Map包含元素数目，dataObject为null时返回-1<br>getMap(dataObject)：返回Map对象| |
|Struct结构|StructObjectInspector<br>StandardStructObjectInspector|使用ObjectInspectorFactory的<br>getStandardStructObjectInspector(<br>    structFieldNameList,<br>    structFieldObjectInspectorList)|getAllStructFieldRefs()：返回所有字段（List<? extends StructField>）<br>getStructFieldRef(fieldName)：返回具有指定名称的字段（StructField对象）<br>getStructFieldData(dataObject, structField)：返回指定字段包含的值，dataObject为null时返回null<br>getStructFieldsDataAsList(dataObject)：以List形式(List<Object>)返回所有字段的值，dataObject为null时返回null|StructField接口表示Struct结构中的字段：<br>  getFieldName()：返回字段名称（总是小写）<br>  getFieldObjectInspector()：返回字段类型对应的ObjectInspector|
|Constant常量|ConstantObjectInspector| |getWritableConstantValue()：返回对应的Writable对象| |

</div>

## 窗口函数

窗口函数基于当前窗口帧的记录执行聚集分析，不减少原表的行数同时具备分组与排序的功能。常用语法为：

```
<window_function> OVER (PARTITION BY <partition_columns> ORDER BY <sort_columns> （ROWS | RANGE) BETWEEN <start> AND <end>)
```

PARTITION BY语句会按照一个或多个指定字段将查询结果集拆分到不同的窗口分区中，并可通过ORDER BY语句按照一定规则排序。如果没有PARTITION BY语句，则整个结果集将作为单个窗口分区。

(ROWS | RANGE) BETWEEN \<start> AND \<end>语句用于定义窗口帧。如果没有ORDER BY语句，则无法定义窗口帧。如果没有定义窗口帧，则默认为RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW。窗口帧用于从窗口分区中选择指定的多条记录供窗口函数处理。start和end可选值为：
+ n PRECEDING：表示往前n行，UNBOUNDED PRECEDING表示该窗口最前面的行
+ n FOLLOWING：表示往后n行，UNBOUNDED FOLLOWING表示该窗口最后面的行
+ CURRENT ROW：当前行

支持如下窗口函数：

<div class="wrapper" markdown="block">

|分类|函数名称|描述|
|---|---|---|
|排序函数|rank()| |
| |dense_rank()| |
| |row_number()| |
|偏移函数|lead()|获取窗口内往后第n行值|
| |lag()|获取窗口内往前第n行值|
| |first_value()|获取所属窗口帧中第一条记录指定字段值|
| |last_value()|获取所属窗口帧中最后一条记录指定字段|
|聚合函数^[聚合函数在窗口函数中是针对自身记录、以及自身记录之上的所有数据进行计算]|sum()| |
| |avg()| |
| |count()| |
| |max()| |
| |min()| |
|其他|ntile()|用于将分组数据按照顺序切分成n片，返回当前记录所在的切片值，如果切片不均匀，<br>默认增加第一个切片的分布。不支持ROWS BETWEEN|

</div>

**窗口函数实现** 分两个步骤，将记录分割成多个分区，然后在各个分区上调用窗口函数。Hive引入了分区表函数（PTF，Partition Table Function）WindowingTableFunction，分区表函数是运行于分区之上、能够处理分区中的记录并输出多行结果的函数，即 输入数据是一张表（也可以是子查询或另一个PTF函数输出），输出数据也是一张表（table-in，table-out）。PTFOperator 会读取已经排好序的数据，创建相应的输入分区；WindowTableFunction则负责管理窗口帧、调用窗口函数（UDAF）、并将结果写入输出分区。

窗口函数在Hive中计算主要分两个阶段（分区和排序相同的窗口函数将一起计算）：
1. 计算除窗口函数以外所有的其他运算（如group by、join、having等）
2. 将上一步的输出作为WindowingTableFunction函数的输入，计算对应的窗口函数值
