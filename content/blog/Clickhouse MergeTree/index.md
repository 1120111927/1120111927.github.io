---
title: ClickHouse MergeTree表引擎
date: "2022-12-09"
description: MergeTree表引擎提供了主键索引、数据分区、数据副本和数据采样等基本能力，其他MergeTree表引擎在此基础上提供了特殊的能力
tags: MergeTree、ReplacingMergeTree、SummingMergeTree、AggregatingMergeTree
---

MergeTree表引擎提供了主键索引、数据分区、数据副本和数据采样等基本能力，其他MergeTree表引擎（ReplacingMergeTree、SummingMergeTree、AggregatingMergeTree、CollapsingMergeTree、VersionedCollapsingMergeTree）在此基础上提供了特殊的能力，如ReplacingMergeTree表引擎具有删除重复数据的特性，SummingMergeTree表引擎则会按照排序键自动聚合数据，带有Replicated前缀的MergeTree系列表引擎支持数据副本。ReplacingMergeTree等MergeTree系列表引擎的特殊逻辑都是在触发合并的过程中被激活的。

MergeTree表引擎最核心的特点是分区目录的合并动作。MergeTree系列表引擎在写入一批数据时，数据总会以数据片段的形式写入磁盘，且数据片段不可修改。为了避免片段过多，ClickHouse会通过后台线程，定期合并这些数据片段，属于相同分区的数据片段会被合并成一个新的片段。这种数据片段往复合并的特点，也正是合并树名称的由来。

## 创建方式

```sql
CTEATE TABLE [IF NOT EXISTS] [db_name.]table_name (
  name [type] [DEFAULT|MATERIALIZED|ALIAS expr],
  ...
) ENGINE = MergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

+ `PARTITION BY`：分区键，用于指定表数据以何种标准进行分区，既可以是单个列字段，也可以通过元组的形式使用多个列字段，还支持列表达式。如果不声明分区键，则ClickHouse会生成一个名为all的分区
+ `ORDER BY`：排序键，用于指定在一个数据片段内数据排序方式，默认情况下主键与排序键相同。排序键既可以是单个列字段，也可以通过元组形式使用多个列字段
+ `PRIMARY KEY`：主键，声明后会依照主键字段生成一级索引，用于加速表查询。与其他数据库不同，MergeTree主键允许存在重复数据（ReplacingMergeTree可以去重），没有唯一键约束
+ `SAMPLE BY`：抽样表达式，用于声明数据已何种方式进行采样，如果使用了此配置项，那么在主键的配置中也需要声明同样的表达式
+ `SETTINGS`:
    + `index_granularity`：表示索引的粒度，默认值为8192（即索引每间隔8192行数据才生成一条索引）
    + `index_granularity_bytes`：19.11版本引入了自适应间隔大小的特性，即根据每一批次写入数据的体量大小，动态划分间隔大小，`index_granularity_bytes`指定了体量大小，默认为10M，为0时表示不启动自适应功能
    + `enable_mixed_granularity_parts`：设置是否开启自适应索引间隔的功能，默认开启
    + `merge_with_ttl_timeout`：数据TTL相关
    + `storage_policy`：多路径存储策略相关

## 存储结构

MergeTree表引擎中的数据拥有物理存储，数据以分区目录的形式进行组织，每个分区独立分开存储。一张数据表的完整物理结构分为3个层级，依次是数据表目录、分区目录及各分区下具体的数据文件：

+ 分区目录（partitioin_n）：各类数据文件都是以分区目录的形式被组织存放的，属于相同分区的数据，最终会被合并到同一个分区目录，而不同分区的数据，永远不会被合并在一起
+ 校验文件（checksums.txt）：使用二进制格式存储，保存了余下各类文件的size大小及size哈希值，用于快速校验文件的完整性和正确性
+ 列信息文件（columns.txt）：使用明文格式存储，用于保存此数据分区下的列字段信息
+ 计数文件（count.txt）：使用明文格式存储，用于记录当前数据分区目录下数据的总行数
+ 一级索引文件（primary.idx）：一级索引文件，使用二进制格式存储，用于存放稀疏索引。
+ 数据文件（[Column].bin）：使用压缩格式存储，默认为LZ4压缩格式，用于存储某一列的数据。每一个列字段都有独立的.bin数据文件，并以列字段名称命名
+ 列字段标记文件（[Column].mrk）：使用二进制格式存储，保存了.bin文件中数据的偏移量信息。标记文件与稀疏索引对齐，又与.bin文件一一对应。MergeTree通过标记文件建立了primary.idx稀疏索引与.bin数据文件之间的映射关系，即首先通过稀疏索引（primary.idx）找到对应数据的偏移量信息（.mrk），再通过偏移量直接从.bin文件中读取数据
+ 列字段标记文件（[Column].mrk2）：使用自适应大小的索引间隔时标记文件会以.mrk2命名
+ partition.dat与minmax_[Column].idx：使用分区键时会生成partition.dat与minmax索引文件，均适用二进制格式存储。partition.dat用于保存当前分区下分区表达式最终生成的值；minmax索引用于记录当前分区下分区字段对应原始数据的最小和最大值。在分区索引的作用下，进行数据查询时能够快速跳过不必要的数据分区目录，从而减少最终需要扫描的数据范围
+ 二级索引文件（skp_idx_[Column].idx）与二级索引标记文件（skp_idx_[Column].mrk）：建表声明二级索引时，会额外生成相应的二级索引与标记文件，使用二进制存储。二级索引也称为跳数索引，包括minmax、set、ngrambf_v1和tokenbf_v1四种类型

## 数据分区

数据在写入时，会对照分区ID落入相应的数据分区。数据分区的规则由分区ID决定，分区ID由分区键的取值决定，分区键支持使用任何一个或一组字段表达式声明，分区ID的生成逻辑目前拥有四种规则：

+ 不指定分区键：默认为all，即所有数据都会被写入all分区
+ 使用整型：将该整型的字符形式作为分区ID
+ 使用日期类型：分区键取值属于日期类型或能够转换为YYYYMMDD格式的整型，将使用按照YYYYMMDD进行格式化后的字符形式作为分区ID
+ 使用其他类型：通过128位Hash算法取其Hash值作为分区ID

如果通过元组的方式使用多个分区字段，则分区ID仍根据上述规则生成，只是多个ID之间通过"_"符号拼接

一个完整分区目录的命名公式：`PartitionID_MinBlockNum_MaxBlockNum_Level`
+ PartitionID：分区ID
+ MinBlockNum/MaxBlockNum：最小数据块编号/最大数据块编号。BlockNum是一个整型的自增长编号，在单张MergeTree数据表内全局累加，从1开始，每当新创建一个分区目录时，编号就会累加1
+ Level：合并的层级，即某个分区被合并过的次数，对于每一个新创建的分区目录而言，其初始值均为0

**分区目录的合并过程**：伴随着每一批数据的写入（一次INSERT语句），MergeTree都会生成一批新的分区目录，对于同一个分区，也会存在多个分区目录的情况。在之后的某个时刻（写入后的10～15分钟，也可以手动执行optimize查询语句），ClickHouse会通过后台任务再将属于相同分区的多个目录合并成一个新的目录。已经存在的旧分区目录并不会立即被删除（不再是激活状态active=0），而是在之后的某个时刻通过后台任务被删除（默认8分钟）。属于同一个分区的多个目录，在合并之后会生成一个全新的目录，目录中的索引和数据文件也会相应地进行合并，新目录名称的合并方式遵循以下规则：
+ MinBlockNum：取同一分区内所有目录中最小的MinBlockNum值
+ MaxBlockNum：取同一分区内所有目录中最大的MaxBlockNum值
+ Level：取同一分区内最大Level值并加1

## 一级索引

定义主键后，MergeTree会依据index_granularity间隔（默认8192），为数据生成一级索引并保存至primary.idx文件内，索引数据按照PRIMARY KEY排序，一般情况下，主键和排序键相同，索引和数据按照完全相同的规则排序。

一级索引采用稀疏索引实现（在稠密索引中每一行索引标记都会对应到一行具体的数据记录；在稀疏索引中每一行索引标记对应的是一段数据），仅需使用少量的索引标记就能够记录大量数据的区间位置信息，占用空间小，常驻内存。

数据按照索引粒度（由index_granularity指定，默认为8192）被标记成多个小的区间。MergeTree使用MarkRange表示一个具体的区间，并通过start和end表示其具体的范围。

MergeTree需要间隔index_granularity行数据才会生成一条索引记录，其索引值会依据声明的主键字段获取。

索引查询其实是两个数值区间的交集判断，一个区间是由基于主键的查询条件转换而来的条件区间，另一个区间是刚才所讲述的与MarkRange对应的数值区间。整个索引查询过程可以大致分为3个步骤：
1. 生成查询条件区间：首先将查询条件转换为条件区间
2. 递归交集判断：以递归的形式，依次对MarkRange的数值区间与条件区间做交集判断，从最大的区间开始：
    1. 如果不存在交集，则直接通过剪枝算法优化此整段MarkRange
    2. 如果存在交集，且MarkRange步长大于8（end-start），则将此区间进一步拆分成8个子区间（由`merge_tree_coarse_index_granularity`指定，默认为8），并重复此规则，继续做递归交集判断
    3. 如果存在交集，且MarkRange不可再分解（步长小于8），则记录MarkRange并返回
3. 合并MarkRange区间：将最终匹配的MarkRange聚在一起，合并它们的范围

MergeTree通过递归的形式持续向下拆分区间，最终将MarkRange定位到最细的粒度，以帮助在后续读取数据的时候，能够最小化扫描数据的范围

## 二级索引

二级索引又称跳数索引，由数据的聚合信息构建而成，用于减少查询时数据扫描的范围。

一张数据表支持同时声明多个跳数索引，跳数索引需要在CREATE语句内定义，支持使用元组和表达式的形式说明，语法如下：

```
INDEX index_name expr TYPE index_type(...) GRANULARITY granularity
```

如果在建表语句中声明了跳数索引，则会额外生成相应的索引（skp_idx_[Column].idx）与标记文件（skp_idx_[Column].mrk）

对于跳数索引，index_granularity定义了数据的粒度，而granularity定义了聚合信息汇总的粒度（即granularity定义了一行跳数索引能够跳过多少个index_granularity区间的数据）。

**跳数索引的生成规则**：首先，按照index_granularity粒度间隔将数据划分成n段，总共有[0, n-1]个区间（n=total_rows/index_granularity，向上取整）。接着，根据索引定义时声明的表达式，从0区间开始，依次按index_granularity粒度从数据中获取聚合信息，每次向前移动1步（n+1），聚合信息逐步累加。最后，当移动granularity次区间时，则汇总并生成一行跳数索引数据

**跳数索引的类型**：MergeTree共支持4种跳数索引
+ minmax：记录了一段数据内的最小和最大极值
+ set：记录了声明字段或表达式的取值（唯一值，无重复），完整形式为set(max_rows)，max_rows是一个阈值，表示在一个index_granularity内，索引最多记录的数据行数，为0时表示无限制
+ ngrambf_v1：记录了数据短语的布隆表过滤器，只支持String和FixedString数据类型，只能提升in、notIn、like、equals和notEquals查询的性能。完整形式为`ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`，`n`为token长度，依据n的长度将数据切割为token短语，`size_of_bloom_filter_in_bytes`为布隆过滤器的大小，`number_of_hash_functions`为布隆过滤器中使用Hash函数的个数，`random_seed`为Hash函数随机种子
+ tokenbf_v1：是ngrambf_v1的变种，也是一种布隆过滤器索引，tokenhf_v1会自动按照非字符的、数字的字符串分割token

## 数据存储

各列独立存储 在MergeTree中，数据按列存储，每个列字段都拥有一个与之对应的.bin数据文件，这些.bin文件承载着数据的物理存储，数据按照ORDER BY的声明排序并经过压缩（LZ4、ZSTD、Multiple和Delta，默认为LZ4）后以压缩数据块的形式写入.bin文件中。数据文件以分区目录的形式被组织存放。按列独立存储的优势：一是可以更好地进行数据压缩；二是能能够最小化数据扫描的范围

压缩数据块 一个压缩数据块由头信息和压缩数据两部分组成，头信息固定使用9位字节表示（1个UInt8整型和2个UInt32整型），分别表示使用的压缩算法类型、压缩后的数据大小和压缩前的数据大小，CompressionMethod_CompressedSize_UncompressedSize，通过ClickHouse提供的clickhouse-compressor工具，能够查询某个.bin文件中压缩数据的统计信息。每个压缩数据块的体积，按照其压缩前的数据字节大小，被严格控制在64KB～1MB，其上下限分别由`min_compress_block_size`（默认65536）与`max_compress_block_size`（默认1048576）参数指定。一个压缩数据块最终的大小，则和一个间隔（`index_granularity`）内数据的实际大小相关。

MergeTree在数据具体的写入过程中，会按照索引粒度，按批次获取数据并进行处理，如果把一批数据的未压缩大小设为size，则整个写入过程遵循以下规则：
+ 单个批次数据 size < 64KB：继续获取下一批数据，直至累积到size >= 64KB时，生成下一个压缩数据块
+ 单个批次数据 64KB <= size <= 1MB：直接生成下一个压缩数据块
+ 单个批次数据 size > 1MB：首先按照1MB大小截断并生成下一个压缩数据块，剩余数据继续依照上述规则执行

一个.bin文件是由1至多个压缩数据块组成的，每个压缩块大小在64KB～1MB之间，多个压缩数据块之间，按照写入顺序首尾相接，紧密地排列在一起。在.bin文件中引入压缩数据块的目的：一是在性能损耗（数据的压缩和解压缩）和压缩率之间寻求一种平衡；二是在不读取整个.bin文件的情况下将读取粒度降低到压缩数据块级别，从而进一步缩小数据读取的范围

## 数据标记

数据标记衔接一级索引和数据，其与索引区间是对齐的，均按照index_granularity的粒度间隔，只需通过索引区间的下标编号就可以直接找到对应的数据标记。数据标记文件与.bin文件一一对应，即每一个列字段[Column].bin文件都有一个与之对应的[Column].mrk数据标记文件，用于记录数据在.bin文件中的偏移量信息。

一行标记数据使用一个元组表示，元组内包含两个整型数值的偏移量信息，分别表示在此段数据区间内，在对应的.bin压缩文件中，压缩数据块的起始偏移量；以及将该数据压缩块解压后，其未压缩数据的起始偏移量

数据标记的工作方式：MergeTree在读取数据时，必须通过标记数据的位置信息才能够找到所需要的数据。整个查找过程大致可以分为读取压缩数据块和读取数据两个步骤
1. 读取压缩数据块：在查询某一列数据时，MergeTree借助标记文件中所保存的压缩文件中的偏移量只加载特定的压缩数据块。上下相邻的两个压缩文件中的起始偏移量，构成了与获取当前标记对应的压缩数据块的偏移量区间，由当前标记数据开始，向下寻找，直到找到不同的压缩文件偏移量为止，此时得到的一组偏移量区间即是压缩数据块在.bin文件中的偏移量。压缩数据块被整个加载到内存之后，会进行解压
2. 读取数据：在读取解压后的数据时，MergeTree借助标记文件中保存的解压数据块中的偏移量可以根据需要以index_granularity的粒度加载特定的一小段。上下相邻两个解压缩数据块中的起始偏移量，构成了与获取当前标记对应的数据的偏移量区间。通过这个区间，能够在它的压缩块被解压之后，依照偏移量按需读取数据

分区、索引、标记和压缩数据的协同总结：

+ 写入过程：数据写入的第一步是生成分区目录，伴随着每一批数据的写入，都会生成一个新的分区目录。在后续的某一时刻，属于相同分区的目录会按照规则合并到一起；接着，按照index_granularity索引粒度，会分别生成primary.idx一级索引（如果声明了二级索引，还会创建二级索引文件）、每一个列字段的.mrk数据标记和.bin压缩数据文件。
+ 查询过程：数据查询的本质，可以看作一个不断减小数据范围的过程，理想情况下，MergeTree首先可以依次借助分区索引、一级索引和二级索引，将数据扫描范围缩至最小。然后再借助数据标记，将需要解压与计算的数据范围缩至最小。如果一条查询语句没有指定任何WHERE条件或是指定了WHERE条件，但条件没有匹配到任何索引（分区索引、一级索引和二级索引），进行数据查询时，MergeTree会扫描所有分区目录，以及目录内索引字段的最大区间。MergeTree能够借助数据标记，以多线程的形式同时读取多个压缩数据块，以提升性能
+ 数据标记与压缩数据块的三种对应关系：压缩数据块的划分与一个间隔（index_granularity）内的数据大小相关（每个压缩数据块的体积都被严格控制在64KB~1MB），一个间隔的数据只会产生一行数据标记，根据一个间隔内数据的实际字节大小，数据标记和压缩数据块之间会产生三种不同的对应关系
    + 多对一：当一个间隔内的数据未压缩大小小于64KB时，多个数据标记对应一个压缩数据块
    + 一对一：当一个间隔内的数据未压缩大小大于等于64KB且小于等于1MB时，一个数据标记对应一个压缩数据块
    + 一对多：当一个间隔内的数据未压缩大小大于1MB时，一个数据标记对应多个压缩数据块

## 数据TTL

在MergeTree中，可以为某个列字段或整张表设置TTL，设置TTL需要依托某个DateTime或Date类型的字段，通过对这个事件字段的INTERVAL操作，来表述TTL的过期时间（TTL time_col + INTERVAL 3 time_unit）。当时间到达时，如果是列字段级别的TTL，则会删除这一列的数据；如果是表级别的TTL，则会删除整张表的数据；如果同时设置了列级别和表级别的TTL，则会以先到期的那个为主。

设置列级别TTL，需要在定义表字段的时候为他们声明TTL表达式，主键字段不能被声明为TTL；设置表级别TTL，需要在MergeTree表参数中增加TTL表达式。

TTL合并频率由参数merge_with_ttl_timeout参数控制，默认为86400秒，即1天。相对于MergeTree的常规合并任务，TTL合并任务性能损耗较大，MergeTree维护了一个专有的TTL任务队列。也可以使用optimize命令强制触发合并。

SYSTEM STOP/START TTL MERGES语句用于控制全局TTL合并任务的启停。

TTL运行原理：
+ MergeTree以分区目录为单位，将TTL信息以JSON形式保存在在ttl.txt文件。columns用于保存列级别TTL信息，table用于保存表级别TTL信息，min和max则保存了当前数据分区内，TTL指定日期字段的最小值、最大值分别与INTERVAL表达式计算后的时间戳
+ 每当写入一批数据时，都会基于INTERVAL表达式的计算结果为这个分区生成ttl.txt文件
+ 只有在MergeTree合并分区时，才会触发删除TTL过期数据的逻辑
+ 在选择删除的分区时会使用贪婪算法，即尽可能找到会最早过期的，同时年纪又是最老的分区（合并次数更多，MaxBlockNum更大的）
+ 如果一个分区内某一列数据因为TTL到期全部被删除了，那么在合并之后生成的新分区目录中，将不会包含这个列字段的数据文件（.bin和.mrk）

## 多路径存储策略

从19.15版本开始，MergeTree实现了自定义存储策略的功能，支持以数据分区为最小移动单元，将分区目录写入多块磁盘目录。有以下存储策略：
+ 默认策略：无须任何配置，所有分区会自动保存到config.xml配置中path指定的路径下
+ JBOD策略：全称Just a Bunch of Disks，一种轮询策略，每执行一次INSERT或者MERGE，所产生的新分区会轮询写入各个磁盘。适合服务器挂载了多块磁盘，但没有做RAID的场景
+ HOT/COLD策略：将存储磁盘分为HOT和COLD两类区域，HOT区域使用SSD这类高性能存储媒介，注重存取性能；COLD区域则使用HDD这类高容量存储媒介，注重存储经济性。数据在写入MergeTree之初，首先会在HOT区域创建分区目录用于保存数据，当分区数据大小累积到阈值时，数据会自行移动到COLD区域。而在每个区域的内部，也支持定义多个磁盘，所以在单个区域的写入过程中，也能应用JBOD策略。适合服务器挂载了不同类型磁盘的场景。

存储配置需要预先定义在config.xml配置文件中，由storage_configuration标签表示，storage_configuration下又包括disks和policies两组标签，分别表示磁盘与存储策略

```xml
<storage_configuration>
  <disks>
    <disk_name_x>
      <path></path>
      <keep_free_space_bytes></keep_free_space_bytes>
    </disk_name_x>
    ...
  <policies>
    <policie_name_x>
      <volumes>
        <volume_name_x>
          <disk></disk>
          ...
          <max_data_part_size_bytes></max_data_part_size_bytes>
        </volume_name_x>
      </volumes>
      <move_factor></move_factor>
    </policie_name_x>
    ...
  </policies>
</disks>
```

+ `<disk_name_*>`：必填，必须全局唯一，表示磁盘的自定义名称
+ `<path>`：必填，用于指定磁盘路径
+ `<keep_free_space_bytes>`：选填，以字节为单位，用于定义磁盘的预留空间
+ `<policie_name_*>`：必填，必须全局唯一，表示策略的自定义名称
+ `<volume_name_*>`：必填，必须全局唯一，表示策略的自定义名称
+ `<disk>`：必填，用于关联`<disks>`配置内的磁盘，可以声明多个disk，MergeTree会按定义的顺序选择disk
+ `<max_data_part_size_bytes>`：选填，以字节为单位，表示在这个卷的单个disk磁盘中，一个数据分区的最大存储阈值，如果单前分区的数据大小超过阈值，则之后的分区会写入下一个disk磁盘
+ `<move_factor>`：选填，默认为0.1；如果当前卷的可用空间小于factor因子，并且定义了多个卷，则数据会向下一个卷移动

## 其他MergeTree表引擎

### ReplacingMergeTree

ReplacingMergeTree能够在合并分区时删除重复的数据，创建表时通过`ENGINE = ReplacingMergeTree(ver)`指定ReplacingMergeTree引擎，ver是选填参数，会指定一个UInt*、Date或者DateTime类型的字段作为版本号，决定了数据去重时所使用的算法。

ReplacingMergeTree处理逻辑如下：
+ 使用ORDER BY排序键作为判断重复数据的唯一键
+ 只有在合并分区的时候才会触发删除重复数据的逻辑
+ 以数据分区为单位删除重复数据，当分区合并时，同一分区内的重复数据会被删除；不同分区之间的重复数据不会被删除
+ 在进行数据去重时，因为分区内的数据已经基于ORDER BY进行了排序，所以能够找到那些相邻的重复数据
+ 数据去重策略有两种：
    + 如果没有设置ver版本号，则保留同一组重复数据中的最后一行
		+ 如果设置了ver版本号，则保留同一组重复数据中ver字段取值最大的那一行

### SummingMergeTree

SummingMergeTree能够在合并分区时按照预先定义的条件汇总数据，将同一组下的多行数据汇总合并成一行。通过`ENGINE = SummingMergeTree((col1,col2,...))`指定SummingMergeTree引擎，col1、col2为columns参数值，选填。

SummingMergeTree处理逻辑如下：
+ 用ORDER BY排序键作为聚合数据的条件Key。如果同时声明了ORDER BY和PRIMARY KEY，MergeTree会强制要求PRIMARY KEY列字段必须是ORDER BY的前缀，修改ORDER BY时只能在现有的基础上减少字段，新增排序字段只能是ALTER ADD COLUMN新增的字段。
+ 只有在合并分区的时候才会触发汇总的逻辑
+ 以数据分区为单位来聚合数据，当分区合并时，同一数据分区内聚合Key相同的数据会被合并汇总，而不同分区之间的数据则不会被汇总
+ 如果在定义引擎时制定了columns汇总列（非主键的数值类型字段），则SUM汇总这些字段；如果未指定，则聚合所有非主键的数值类型字段
+ 在进行数据汇总时，因为分区内的数据已经基于ORDER BY排序，所以能够找到相邻且拥有相同聚合Key的数据
+ 在汇总数据时，同一分区内，相同聚合Key的多行数据会合并成一行，其中，汇总字段会进行SUM计算；对于非汇总字段，则会使用第一行数据的取值
+ 支持嵌套结构，但列字段名称必须以Map后缀结尾。嵌套类型中，默认以第一个字段作为聚合Key。除第一个字段以外，任何名称以Key、Id或Type为后缀结尾的字段，都将和第一个字段一起组成复合Key

### AggregatingMergeTree

AggregatingMergeTree能够在合并分区的时候按照预先定义的条件聚合数据，同时，根据预先定义的聚合函数计算数据并通过二进制的格式存入表内。通过ENGINE = AggregatingMergeTree()指定AggregatingMergeTree引擎，没有额外的设置参数，在分区合并时，在每个数据分区内，会按照order by聚合，通过AggregateFunction数据类型指定聚合函数和被聚合列。AggregateFunction是Clickhouse提供的一种特殊的数据类型，能够以二进制的形式存储中间状态结果，对于AggregateFunction类型的列字段，写入数据时需要调用*State函数；查询数据时需要调用相应的*Merge函数，其中*表示定义时使用的聚合函数。AggregatingMergeTree通常结合物化视图使用，将它作为物化视图的表引擎

AggregatingMergeTree的处理逻辑如下：
+ 用order by排序键作为聚合函数的条件Key
+ 使用AggregateFunction字段类型定义聚合函数的类型以及聚合的字段
+ 只有在合并分区的时候才会触发聚合计算的逻辑
+ 以数据分区为单位来聚合数据，当分区合并时，同一数据分区内聚合key相同的数据会被合并计算，而不同分区之间的数据则不会被计算
+ 在进行数据计算时，因为分区内的数据已经基于order by排序，所以能够找到那些相邻且拥有相同聚合Key的数据
+ 在聚合数据时，同一分区内，相同聚合Key的多行数据会合并成一行，对于那些非主键、非AggregateFunction类型字段，则会使用第一行数据的取值
+ AggregateFunction类型的字段使用二进制存储，在写入数据时，需要调用*State函数，而在查询数据时，需要调用相应的*Merge函数，其中，*表示定义时使用的聚合函数
+ AggregatingMergeTree通常作为物化视图的表引擎，与普通MergeTree搭配使用

### CollapsingMergeTree

CollapsingMergeTree是一种通过以增代删的思路，支持行级数据修改和删除的表引擎，通过定义1个sign标记位字段，记录数据行的状态，如果sign标记为1，则表示这是一行有效的数据；如果sign标记为-1，则表示这行数据需要被删除。当CollapsingMergeTree分区合并时，同一数据分区内，sign标记为1和-1的一组数据会被抵消删除。通过engine = CollapsingMergeTree(sign)指定CollapsingMerge引擎，其中sign用于指定一个Int8类型的标志位字段。

CollapsingMergeTree在折叠数据时，遵循以下规则：
+ 如果sign=1比sign=-1的数据多一行，则保留最后一行sign=1的数据
+ 如果sign=-1比sign=1的数据多一行，则保留第一行sign=1的数据
+ 如果sign=1和sign=-1的数据行一样多，并且最后一行是sign=1，则保留第一行sign=-1和最后一行的数据
+ 如果sign=1和sign=-1的数据行一样多，并且最后一行是sign=-1，则什么也不保留
+ 其余情况，ClickHouse会打印警告日志，但不会报错，在这种情况下，查询结果不可预知

### VersionedCollapsingMergeTree

CollapsingMergeTree对于写入数据的顺序有着严格要求，sign=1和sign=-1的数据必须相邻，而分区内的数据基于ORDER BY排序，要实现sign=1和sign=-1的数据相邻，则只能依靠严格按照顺序写入。VersionedCollapsingMergeTree表引擎的作用与CollapsingMergeTree完全相同，不同之处在于VersionedCollapsingMergeTree对数据的写入顺序没有要求，在同一个分区内，任意顺序的数据都能够完成折叠操作。

VersionedCollapsingMergeTree引擎通过engine = VersionedCollapsingMergeTree(sign, ver)设置，定义ver字段之后，VersionedCollapsingMergeTree会自动将ver作为排序条件并增加到ORDER BY的末端，所以无论写入时数据的顺序如何，在折叠处理时都能回到正确的顺序。

ReplicatedMergeTree在MergeTree能力的基础之上增加了分布式协同的能力，其借助ZooKeeper的消息日志广播功能，实现了副本实例之间的数据同步功能。
