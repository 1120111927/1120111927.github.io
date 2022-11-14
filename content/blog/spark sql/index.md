---
title: Spark SQL
date: "2022-08-21"
description: Spark SQL编译需要经过两个大的阶段，逻辑计划（LogicalPlan）和物理计划（PhysicalPlan）
tags: 逻辑计划（LocialPlan）、物理计划（PhysicalPlan）
---

一般来讲，对于Spark SQL系统，从SQL到Spark中RDD的执行需要经过两个大的阶段，分别是逻辑计划（LogicalPlan）和物理计划（PhysicalPlan）。

```bob-svg
    .----------------.     .----------.        .-----------.
    | SparkSqlParser |     | Analyzer |        | Optimizer |
    '-------+--------'     '-----+----'        '-----+-----'
            | parse              | apply             | apply
            | .------------------+-------------------+--------------------.
            | |  LogicalPlan     |                   |                    |
+--------+  | |  +------------+  |  +-------------+  |  +-------------+   |
|  SQL   |  v |  |Unresolved  |  v  | Analyzed    |  v  | Optimized   |   |
|  Query +--+-+->+LogicalPlan +--+->+ LogicalPlan +--+->+ LogicalPlan +---+--+
+--------+    |  +------------+     +-------------+     +-------------+   |  |
              '-----------------------------------------------------------'  |   plan   .--------------.
              .-----------------------------------------------------------.  +<---------| SparkPlanner |
              |  PhysicalPlan                                             |  |          '--------------'
              |  +-----------+      +-----------+      +---------------+  |  |
      show    |  | Prepared  |      | SparkPlan | next |Iterator       |  |  |
   <----------+--+ SparkPlan +<--+--+           +<-----+[PhysicalPlan] +<-+--+
              |  +-----------+   ^  +-----------+      +---------------+  |
              |                  |                                        |
              '------------------+----------------------------------------'
            Seq[Rule[SparkPlan]] | apply
                      .----------------------.
                      | prepareForExecution  |
                      '----------------------'
```

逻辑计划阶段会将用户所写的SQL语句转换成树型数据结构（逻辑算子树），SQL语句中蕴含的逻辑映射到逻辑算子树的不同节点。逻辑计划阶段生成的逻辑算子树并不会直接提交执行，仅作为中间阶段，最终逻辑算子树的生成过程经历了3个子阶段，分别对应未解析的逻辑算子树（Unresolved LogicalPlan，仅仅是数据结构，不包含任何数据信息等）、解析后的逻辑算子树（Analyzed LogicalPlan，节点中绑定各种信息）和优化后的逻辑算子树（Optimized LogicalPlan，应用各种优化规则对一些低效的逻辑计划进行替换）。

物理计划阶段将上一步逻辑计划阶段生成的逻辑算子树进行进一步转换，生成物理算子树，物理算子树的节点会直接生成RDD或对RDD进行transformation操作^[每个物理计划节点中都实现了对RDD进行转换的execute方法]。物理计划阶段也包含3个子阶段，首先根据逻辑算子树，生成物理算子树的列表`Iterator[PhysicalPlan]`（同样的逻辑算子树可能对应多个物理算子树），然后从列表中按照一定的策略选取最优的物理算子树（SparkPlan），最后，对选取的物理算子树进行提交前的准备工作（如 确保分区操作正确、物理算子树节点重用、执行代码生成等），得到准备后的物理算子树（Prepared SparkPlan）。

经过上述步骤后，物理算子树生成的RDD执行action操作，即可提交执行。从SQL语句的解析一直到提交之前，上述整个转换过程都在Spark集群的Driver端进行，不涉及分布式环境。SparkSession类的sql方法调用SessionState中的各种对象，包括上述不同阶段对应的SparkSqlParser类、Optimizer类和SparkPlanner类等，最后封装成一个QueryExecution对象。

Spark SQL内部实现上述流程中平台无关部分的基础框架称为Catalyst，Catalyst中涉及的重要概念和数据结构主要包括InternalRow体系、TreeNode体系和Expression体系。

数据处理首先需要考虑如何表示数据，对于关系表来讲，通常操作的数据都是以行为单位的，在Spark SQL内部实现中，InternalRow用来表示一行行数据的类^[物理算子树节点产生和转换的RDD类型即为`RDD[InternalRow]`]。InternalRow中的每一列都是Catalyst内部定义的数据类型。InternalRow作为一个抽象类，包含numFields和update方法，以及各列数据对应的get与set方法^[InternalRow中都是根据下标来访问和操作列元素的]，但具体的实现逻辑体现在不同的子类中，包括BaseGenericInternalRow、UnsafeRow、JoinedRow。

+ BaseGenericInternalRow：实现了InternalRow中定义的所有get类型方法，这些方法的实现都通过调用类中定义的GenericGet虚函数进行，该函数的实现在下一级子类中
+ JoinedRow：主要用于Join操作，将两个InternalRow放在一起形成新的InternalRow
+ UnsafeRow：不采用Java对象存储的方式，避免了JVM中垃圾回收的代价，对行数据进行了特定的编码，使得存储更加高效。MutableUnsafeRow和UnsafeRow相关，用来支持对特定的列数据进行修改

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
abstract class InternalRow {
    numFields: Int
    setNull(i: Int): Unit
    update(i: Int, value: Any): Unit
    setBoolean(i: Int, value: Boolean): Unit
    ...
    toSeq(fieldTypes: Seq[DataType]): Seq[Any]
    toSeq(schema: StructType): Seq[Any]
}

class UnsafeRow
class BaseGenericInternalRow
class JoinedRow
class GenericInternalRow
class SpecificInternalRow
class MutableUnsafeRow

InternalRow <|-- UnsafeRow
InternalRow <|-- BaseGenericInternalRow
InternalRow <|-- JoinedRow
BaseGenericInternalRow <|-- GenericInternalRow
BaseGenericInternalRow <|-- SpecificInternalRow
BaseGenericInternalRow <|-- MutableUnsafeRow
@enduml
```

```Java
class GenericInternalRow(val values: Array[Any]) extends BaseGenericInternalRow {
    protected def this() = this(null)
    def this(size: Int) = this(new Array[Any](size))
    override protected def genericGet(ordinal: Int) = values(oridinal)
    override def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = values.clone()
    override def numFields: Int = values.length
    override def setNullAt(i: Int): Unit = { values(i) = value }
    override def copy(): GenericInternalRow = this
}
```

GenericInternalRow构造参数是`Array[Any]`类型，采用对象数据进行底层存储，genericGet也是直接根据下标访问的^[数组是非拷贝的，一旦创建就不允许通过set操作进行改变]，SpecificInternalRow是以`Array[MutableValue]`为构造参数的，允许通过set操作进行修改。

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
abstract class TreeNode {
    children: Seq[BaseType]
    foreach(f: BaseType => Unit): Unit
    foreachUp(f: BaseType => Unit): Unit
}
class Expression
class QueryPlan
class LogicalPlan
class SparkPlan

TreeNode <|-- Expression
TreeNode <|-- QueryPlan
QueryPlan <|-- LogicalPlan
QueryPlan <|-- SparkPlan
@enduml
```

TreeNode体系是Catalyst中的中间数据结构，TreeNode类是Spark SQL中所有树结构的基类，定义了一系列通用的集合操作和树遍历操作接口，TreeNode内部包含一个`Seq[BaseType]`类型的变量children来表示孩子节点，定义了foreach、map、collect等针对节点操作的方法，以及transformUp和transformDown等遍历节点并对匹配节点进行相应转换的方法。TreeNode一直在内存里维护，不会dump到磁盘以文件形式存储，且树的修改都是以替换已有节点的方式进行的。

TreeNode包含两个子类继承体系，QueryPlan和Expression体系，QueryPlan类下又包含逻辑算子树（LogicalPlan）和物理执行算子树（SparkPlan）两个重要子类，其中，逻辑算子树在Catalyst中内置实现，可以剥离出来直接应用到其他系统中；而物理算子树SparkPlan和Spark执行层紧密相关，当Catalyst应用到其他计算模型时，可以进行相应的适配修改。

TreeNode本身提供了最简单和最基本的操作：
+ `collectLeaves`：获取当前TreeNode所有叶子节点
+ `collectFirst`：先序遍历所有节点并返回第一个满足条件的节点
+ `withNewChildren`：将当前节点的子节点替换为新的子节点
+ `transformDown`：用先序遍历方式将规则作用于所有节点
+ `transformUp`：用后序遍历方式将规则作用于所有节点
+ `transformChildren`：递归地将规则作用到所有子节点
+ `treeString`：将TreeNode以树型结构展示

表达式一般指的是不需要出发执行引擎而能够直接进行计算的单元（四则运算、逻辑操作、转换操作、过滤操作等），Expression是Catalyst中的表达式体系，算子执行前通常都会进行绑定操作，将表达式与输入的属性对应起来，同时算子也能够调用各种表达式处理相应的逻辑。Expression类中定义了5各方面的操作，包括基本属性、核心操作、输入输出、字符串表示和等价性判断。核心操作中的`eval`函数实现了表达式对应的处理逻辑，`genCode`和`doGenCode`用于生成表达式对应的Java代码，字符串表示用于查看该Expression的具体内容。

## Spark SQL解析

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members

interface ParseInterface {
    parsePlan(sqlText: String): Logical Plan
    parseExpression(sqlText: String): Expression
    parseTableIdentifier(sqlText: String): TableIdentifier
}

abstract class AbstractSqlParser {
    parseDataType(sqlText: String): DataType
    astBuilder: AstBuilder
}

class CatalystSqlParser {
    astBuilder: AstBuilder
}

class SparkSqlParser {
    astBuilder: SparkSqlAstBuilder
}

class SqlBaseBaseVisitor
class AstBuilder
class SparkSqlAstBuilder

ParseInterface <|.. AbstractSqlParser
AbstractSqlParser <|-- SparkSqlParser
AbstractSqlParser <|-- CatalystSqlParser

SqlBaseBaseVisitor <|-- AstBuilder
AstBuilder <|-- SparkSqlAstBuilder

AstBuilder <- AbstractSqlParser
SparkSqlAstBuilder <- SparkSqlParser

@enduml
```

Catalyst中提供了直接面向用户的ParseInterface接口，该接口中包含了对SQL语句、Expression表达式和TableIdentifier数据表标识符的解析方法，AbstractSqlParser是实现了ParseInterface的虚类，其中定义了返回AstBuilder的函数。CatalystSqlParser仅用于Catalyst内部，而SparkSqlParser用于外部调用。核心是AstBuilder，继承了ANTLR 4生成的默认SqlBaseBaseVisitor，用于生成SQL对应的抽象语法树AST（Unresolved LogicalPlan）；SparkSqlAstBuilder继承AstBuilder，并在其基础上定义了一些DDL语句的访问操作，主要在SparkSqlParser中调用。

当开发新的语法支持时，首先需要改动ANTLR 4文件（在SqlBase.g4中添加文法），重新生成词法分析器（SqlBaseLexer）、语法分析器（SqlBaseParser）和访问者类（SqlBaseVisitor接口与SqlBaseVisitor类），然后在AstBuilder等类中添加相应的访问逻辑，最后添加执行逻辑。

QuerySpecificationContext为根节点所代表的子树中包含了对数据的查询，其子节点包括NamedExpressionSqlContext、FromClauseContext、BooleanDefaultContext、AggregationContext等，NamedExpressionSqlContext为根节点的系列节点代表select表达式中选择的列，FromClauseContext节点为根节点的系列节点对应数据表，BooleanDefaultContext为根节点的系列节点代表where条件中的表达式，AggregationContext为根节点的系列节点代表聚合操作（group by、cube、grouping sets和roll up）的字段。QueryOrganizationContext为根节点所代表的子树中包含了各种对数据组织的操作，如SortItemContext节点代表数据查询之后所进行的排序操作。FunctionCallContext为根节点的系列节点代表函数，其子节点QualifiedNameContext代表函数名，ExpressionContext表示函数的参数表达式。

## Spark SQL逻辑计划（Logical Plan）

逻辑计划阶段，字符串形态的SQL语句转换为树结构形态的逻辑算子树，SQL中包含的各种处理逻辑（过滤、剪裁等）和数据信息都会被整合在逻辑算子树的不同节点中。逻辑计划本质上是一种中间过程表示，与Spark平台无关，后续阶段会进一步将其映射为可执行的物理计划。

Spark SQL逻辑计划在实现层面被定义为LogicalPlan类，从SQL语句经过SparkSqlParser解析生成Unresolved LogicalPlan到最终优化成Optimized LogicalPlan，这个流程主要经过3个阶段：

1. 由SparkSqlParser中的AstBuilder执行节点访问，将语法树的各种Context节点转换成对应的LogicalPlan节点，从而成为一棵未解析的逻辑算子树（Unresolved LogicalPlan），此时逻辑算子树是最初形态，不包含数据信息和列信息等
2. 由Analyzer将一系列规则作用在Unresolved LogicalPlan上，对树上的节点绑定各种数据信息，生成解析后的逻辑算子树（Analyzed LogicalPlan）
3. 由Spark SQL中的优化器（Optimizer）将一系列优化规则作用到上一步生成的逻辑算子树中，在确保结果正确的前提下改写其中的低效结构，生成优化后的逻辑算子树（Optimized LogicalPlan）。Optimized LogicalPlan传递到下一个阶段用于物理执行计划的生成

### LogicalPlan

LogicalPlan属于TreeNode体系，继承自QueryPlan父类，作为数据结构记录了对应逻辑算子树节点的基本信息和基本操作，包括输入输出和各种处理逻辑等。

QueryPlan的主要操作分为6个模块：
+ 输入输出：定义了5个输入输出方法
    + `output`是返回值为`Seq[Attribute]`的虚函数，具体内容由不同子节点实现
    + `outputSet`是将output返回值进行封装，得到`AttributeSet`集合类型的结果
    + `inputSet`是用于获取输入属性的方法，返回值也是`AttributeSet`，节点的输入属性对应所有子节点的输出
    + `producedAttributes`表示该节点所产生的属性
    + `missingInput`表示该节点表达式中涉及的但是其子节点输出中并不包含的属性
+ 基本属性：表示QueryPlan节点中的一些基本信息
    + `schema`对应output输出属性的schema信息
    + `allAttributes`记录节点所涉及的所有属性（Attribute）列表
    + `aliasMap`记录节点与子节点表达式中所有的别名信息
    + `references`表示节点表达式中所涉及的所有属性集合
    + `subqueries`/`innerChildren`默认实现该QueryPlan节点中包含的所有子查询
+ 字符串：这部分方法主要用于输出打印QueryPlan树型结构信息，其中schema信息也会以树状展示。`statePrefix`方法用来表示节点对应计划状态的前缀字符串^[在QueryPlan的默认实现中，如果该计划不可用，则前缀会用感叹号标记]
+ 规范化：在QueryPlan的默认实现中，`canonicalized`直接赋值为当前的QueryPlan类，在`sameResult`方法中会利用`canonicalized`来判断两个QueryPlan的输出数据是否相同
+ 表达式操作：在QueryPlan各个节点中，包含了各种表达式对象，各种逻辑操作一般也都是通过表达式来执行的
    + `expressions`方法能够得到该节点中的所有表达式列表
+ 约束（Constraints）：本质上属于数据过滤条件（Filter）的一种，同样是表达式类型。相对于显式的过滤条件，约束信息可以推导出来^[对于过滤条件a > 5，显然a的属性不能为null，这样就可以对应的构造`isNotNull(a)`约束]。在实际情况下，SQL语句中可能会涉及很复杂的约束条件处理，如约束合并、等价性判断等，在QueryPlan类中，提供了大量方法用于辅助生成constraints表达式集合以支持后续优化操作
    + `validConstraints`方法返回该QueryPlan所有可用的约束条件
    + `constructIsNotNullConstraints`方法会针对特定的列构造isNotNull约束条件

### Unresolved LogicalPlan生成

Spark SQL首先会在ParserDriver中通过调用语法分析器中的`singleStatement()`方法构建整棵语法树，然后通过AstBuilder访问者类对语法树进行访问。AstBuilder访问入口是`visitSingleStatement()`方法，该方法也是访问整棵抽象语法树的启动接口。

```Java
override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
}
```

对根节点的访问操作会递归访问其子节点，这样逐步向下递归调用，直到访问某个子节点时能够构造LogicalPlan，然后传递给父节点。

QuerySpecificationContext节点的执行逻辑可以看作两部分：首先访问FromClauseContext子树，生成名为from的LogicalPlan，接着调用withQuerySpecification方法在from的基础上完成后续扩展。

```Java
override def visitQuerySpecification(ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    val from = OneRowRelation.optional(ctx.fromClause) {
        visitFromClause(ctx.fromClause)
    }
    withQuerySpecification(ctx, from)
}
```

生成UnresolvedLogicalPlan的过程，从访问QuerySpecificationContext节点开始，分为以下3个步骤：
1. 生成数据表对应的LogicalPlan：访问fromClauseContext并递归访问，一直到匹配TableNameContext节点（visitTableName）时，直接根据TableNameContext中的数据信息生成UnresolvedRelation，此时不再递归访问子节点，构造名为from的LogicalPlan并返回
2. 生成加入了过滤逻辑的LogicalPlan：过滤逻辑对应SQL中的where语句，在QuerySpecificationContext中包含了名称为where的BooleanExpressionContext类型，AstBuilder会对该子树进行递归访问，生成expression并返回作为过滤条件，然后基于此过滤条件表达式生成Filter LogicalPlan节点。最后，由此LogicalPlan和1中的UnresolveRelation构造名称为withFilter的LogicalPlan，其中Filter节点为根节点
3. 生成加入列剪裁后的LogicalPlan：列剪裁逻辑对应SQL中select语句对name列的选在操作，AstBuilder在访问过程中会获取QuerySpecificationContext节点所包含的NamedExpressionSeqContext成员，并对其所有子节点对应的表达式进行转换，生成NameExpression列表（namedExpressions）,然后基于namedExpressions生成Project LogicalPlan，最后，由此LogicalPlan和2中的withFilter构造名称为withProject的LogicalPlan，其中Project最终成为整棵逻辑算子树的根节点

### Analyzed LogicalPlan生成

Analyzed LogicalPlan基本上是根据Unresolved LogicalPlan一对一转换过来的。

Analyzer的主要作用就是根据Catalog中的信息将Unresolved LogicalPlan中未被解析的UnresolvedRelation和UnresolvedAttribute两种对象解析成有类型的（Typed）对象。

在Spark SQL系统中，Catalog主要用于各种函数资源信息和原数据信息（数据库、数据表、数据视图、数据分区与函数等）的统一管理。Saprk SQL中的Catalog体系实现以SessionCatalog为主体，通过SparkSession（Spark程序入口）提供给外部调用。一般一个SparkSession对应一个SessionCatalog。本质上，SessionCatalog起到一个代理的作用，对底层的元数据信息、临时表信息、视图信息和函数信息进行了封装。SessionCatalog主要包含以下部分：

+ GlobalTempViewManager（全局的临时视图管理）：是一个线程安全的类，提供了对全局视图的原子操作，包括创建、更新、删除和重命名等，进行跨Session的视图管理。内部实现中主要功能依赖一个mutable类型的HashMap来对视图名和数据源进行映射，其中key为视图名，value为视图所对应的LogicalPlan（一般在创建该视图时生成）
+ FunctionResourceLoader（函数资源加载器）：主要用于加载Jar包、文件两种类型的资源以提供函数^[Spark SQL内置实现的函数、用户自定义函数和Hive中的函数]的调用
+ FunctionRegistry（函数注册接口）：用来实现对函数的注册（Register）、查找（Lookup）和删除（Drop）等功能。在Spark SQl中默认实现是SimpleFunctionRegistry，其中采用Map数据结构注册了各种内置的函数
+ ExternalCatalog（外部系统Catalog）：用来管理数据库（Databases）、数据表（Tables）、数据分区（Partitions）和函数（Functions）的接口。目标是与外部系统交互，并做到上述内容的非临时性存储。ExternalCatalog是一个抽象类，定义了上述4个方面的功能，在Spark SQL中，具体实现有InMemoryCatalog和HiveExternalCatalog两种，InMemoryCatalog将上述信息存储在内存中，HiveExternalCatalog利用Hive元数据库来实现持久化管理

另外，SessionCatalog内部还包含一个mutable类型的HashMap用来管理临时表信息，以及currentDb成员变量用来指代当前操作所对应的数据库名称。

对Unresolved LogicalPlan的操作（绑定、解析、优化等）都是基于规则（Rule）的，通过Scala语言模式匹配机制（Pattern-match）进行树结构的转换或节点改写。Rule是一个抽象类，子类需要复写`apply(plan: TreeType)`方法来制定特定的处理逻辑。在Catalyst中RuleExecutor用来调用这些规则，涉及树型结构的转换过程^[Analyzer逻辑算子树的分析过程，Optimizer逻辑算子树的优化过程，物理算子树的生成过程]都要实施规则匹配和节点处理，都继承自`RuleExecutor[TreeType]`抽象类。RuleExecutor内部提供了一个`Seq[Batch]`，里面定义了该RuleExecutor的处理步骤。每个Batch代表一套规则，配备一个策略，该策略说明了迭代次数（一次还是多次），RuleExecutor的`apple(plan: TreeType)`方法会按照batches顺序和batch内的Rules顺序，对传入的plan里的节点进行迭代处理，处理逻辑由具体Rule子类实现。

```Scala
abstract class Rule[TreeType <: TreeNode[_]] extends Logging {
    val ruleName: String = {
        val className = getClass.getName
        if (className endsWith "$") className.dropRigh(1) else className
    }
    def apply(plan: TreeType): TreeType
    def execute(plan: TreeType): TreeType = {
        var curPlan = plan
        batches.foreach { batch =>
            val batchStartPlan = curPlan
            var iteration = 1
            var lastPlan = curPlan
            var continue = true
            while (continue) {
                curPlan = batch.rules.foldLeft(curPlan) {
                    case (plan, rule) => rule(plan)
                }
                iteration += 1
                if (iteration > batch.strategy.maxIterations) {
                    continue = false
                }
                if (curPlan.fastEquals(lastPlan)) {
                    continue = false
                }
                lastPlan = curPlan
            }
        }
        curPlan
    }
}
```

Analyzer继承自ruleExecutor类，执行过程会调用其父类RuleExecutor中实现的execute方法，Analyzer中重新定义了一系列规则，即RuleExecutor类中的成员变量batches。Analyzer默认定义了6个Batch，共有34条内置的规则外加额外实现的扩展规则：

+ Batch Substitution：对节点进行替换操作，包含4条规则
    + CTESubstitution：用来处理with语句^[CTE对应with语句，在SQL中主要用于子查询模块化]，在遍历逻辑算子树的过程中，当匹配到`with(child, relation)`节点时，将子LogicalPlan替换成解析后的CTE。由于CTE的存在，SparkSqlParser对SQL语句从左向右解析后会产生多个LogicalPlan，这条规则的作用是将多个LogicalPlan合并成一个LogicalPlan
    + WindowsSubstitution：对当前的逻辑算子树进行查找，当匹配到`WithWindowDefinition(windowDefinitions, child)`表达式时，将其子节点中未解析的窗口表达式（Unresolved-WindowExpression）转换成窗口函数表达式（WindowExpression）
    + EliminateUnions：在遍历逻辑算子树过程中，匹配到`Union(children)`且children的数目只有1个时，将`Union(children)`替换为`children.head`节点
    + SubstituteUnresolvedOrdinals：根据`spark.sql.orderByOridinal`和`spark.sql.groupByOrdinal`这两个配置参数将下标替换成UnresolvedOriginal表达式，以映射到对应的列
+ Batch Resolution：包含25条分析规则以及一个extendedresolutionRules扩展规则列表用来支持Analyzer子类在扩展规则列表中添加新的分析规则
    + ResolveTableValuedFunctions：解析可以作为数据表的函数
    + ResolveRelations：解析数据表，当遍历逻辑算子树的过程中匹配到UnresolvedRelation节点时，调用`lookupTableFromCatalog`方法从SessionCatalog中查表，在Catalog查表后，Relation节点上会插入一个别名节点
    + ResolveRederrences：解析列，当遍历逻辑算子树的过程中匹配到UnresolvedAttribute时，会调用LogicalPlan中定义的resolveChildren方法对该表达式进行分析。resolveChildren并不能确保一次分析成功，在分析对应表达式时，需要根据该表达式所处LogicalPlan节点的子节点输出信息进行判断，如果存在处于unresolved状态的子节点，解析操作无法成功，留待下一轮规则调用时再进行解析
    + ResolveCreateNamedStruct：解析结构体创建
    + ResolveDeserializer：解析反序列化操作类
    + ResolveNewInstance：解析新的实例
    + ResolveUpCast：解析类型转换
    + ResolveGroupingAnalytics：解析多维分析
    + ResolvePivot：解析Pivot
    + ResolveOrdinalInOrderByAndGroupBy：解析下标聚合
    + ResolveMissingReferences：解析新的列
    + ExtractGenerator：解析生成器
    + ResolveGenerate：解析生成过程
    + ResolveFunctions：解析函数
    + ResolveAliases：解析别名
    + ResolveSubquery：解析子查询
    + ResolveWindowOrder：解析窗口函数排序
    + ResolveWindowFrame：解析窗口函数
    + ResolveNaturalAndUsingJoin：解析自然join
    + ExtractWindowExpressions：提取窗口函数表达式
    + GlobalAggregates：解析全局聚合
    + ResolveAggregateFunctions：解析聚合函数
    + TimeWindowing：解析时间窗口
    + ResolveInlineTables：解析内联表
    + TypeCoercion.typeCoercionRules：解析强制类型转换。
        + ImplicitTypeCasts规则对逻辑算子树中的BinaryOperator表达式调用`findTightestCommonTypeOfTwo`找到对于左右表达式节点来讲最佳的共同数据类型
    + extendedResolutionRules：扩展规则
+ Batch Nondeterministic：仅包含PullOutNondeterministic一条规则，用来将LogicalPlan中非Project或非Filter算子的nondeterministic（不确定的）表达式提取出来，然后将这些表达式放在内层的Project算子中或最终的Project算子中
+ Batch UDF：主要用来对用户自定义函数进行一些特别的处理，仅有HandleNullInputForUDF一条规则。HandleNullInputForUDF用来处理输入数据为Null的情形，主要思想是从上至下进行表达式的遍历，当匹配到ScalaUDF类型的表达式时，会创建If表达式来进行Null值的检查
+ Batch FixNullability：仅包含FixNullability一条规则，用来统一设定LogicaPlan中表达式的nullable属性。在DataFrame或Dataset等编程接口中，用户代码对于某些列可能会改变其nullability属性，导致后续的判断逻辑中出现异常结果，在FixNullability规则中，对解析后的LogicalPlan执行transformExpressions操作，如果某列来自于其子节点，则其nullability值根据子节点对应的输出信息进行设置
+ Batch Cleanup：仅包含CleanupAliases一条规则，用来删除LogicalPlan中无用的别名信息。一般情况下，逻辑算子树中仅Project、Aggregate或Window算子的最高一层表达式才需要别名，CleanupAliases通过trimAliases方法对表达式执行中的别名进行删除

Unresolved LogicalPlan的解析是一个不断的迭代过程，用户可以通过参数（spark.sql.optimizer.maxIterations）设定RuleExecutor迭代的轮数，默认配置为50轮，对于某些嵌套较深的特殊SQL，可以适当地增加轮数。

QueryExecution类中触发Analyzer执行的是execute方法，即RuleExecutor中的execute方法，该方法会循环地调用规则对逻辑算子树进行分析。

### Optimized LogicalPlan生成

Optimizer继承自RuleExecutor类，执行过程是调用父类RuleExecutor中实现的executor方法，Optimizer中重新定义了一系列规则，即RuleExecutor类中的成员变量batches。在QueryExecution中，Optimizer会对传入的Analyzed LogicalPlan执行execute方法，启动优化过程。

```scala
val optimizedPlan: LogicalPlan = optimizer.execute(analyzed)
```

SparkOptimizer继承自Optimizer，实现了16个Batch（Optimizer自身定义了12个规则Batch，SparkOptimizer类又添加了4个Batch）：

1. Batch Finish Analysis：包含5条规则，都只执行一次
    + EliminateSubqueryAliases：消除子查询别名，直接将SubqueryAlias节点替换为其子节点^[subqueries仅用于提供查询的视角范围信息，一旦Analyzer阶段结束，该节点就可以被移除]
    + ReplaceExpressions：表达式替换，在逻辑算子树中查找匹配RuntimeReplaceable的表达式并将其替换为能够执行的正常表达式，通常用来对其他类型的数据库提供兼容能力，如用`coalesce`替换`nvl`表达式
    + ComputeCurrentTime：计算与当前时间相关的表达式，在同一条SQL语句中可能包含多个计算时间的表达式，即CurrentDate和CurrentTimestamp，且该表达式出现在多个语句中，未避免不一致，ComputeCurrentTime对逻辑算子树中的时间函数计算一次后，将其他同样的函数替换成该计算结果
    + GetCurrentDatabase：执行CurrentDatabase并得到结果，然后用此结果替换所有CurrentDatabase表达式
    + RewriteDistinctAggregates：重写Distinct聚合操作，将包含Distinct算子的聚合语句转换为两个常规的聚合表达式
2. Batch Union：针对Union操作的规则Batch
    + CombineUnions：在逻辑算子树中当相邻的节点都是Union算子时，将这些相邻的Union节点合并为一个Union节点
3. Batch Subquery
    + OptimizeSubqueries：当SQL语句包含子查询时，会在逻辑算子树上生成SubqueryExpression表达式，OptimizeSubqueries优化规则在遇到SubqueryExpression表达式时，进一步递归调用Optimizer对该表达式的子计划并进行优化
4. Batch Replace Operators：主要用来执行算子的替换操作。在SQL语句中，某些查询算子可以直接改写为已有的算子
    + ReplaceIntersectWithSemiJoin：将Intersect操作算子替换为Left-Semi Join操作算子，仅适用于INTERSECT DISTINCT类型的语句，而不适用于INTERSECT ALL语句，该优化规则执行前必须消除重复的属性，避免生成的Join条件不正确
    + ReplaceExceptWithAntiJoin：将Except操作算子替换为Left-Anti Join操作算子，仅适用于EXCEPT DISTINCT类型的语句，而不适用于EXCEPT ALL语句，该优化规则执行之前必须消除重复的属性，避免生成的Join条件不正确
    + ReplaceDistinctWithAggregate：将Distinct算子转换为Aggregate语句，将SQL语句中直接进行Distinct操作的Select语句替换为对应的Group By语句
5. Batch Aggregate：主要用来处理聚合算子中的逻辑
    + RemoveLiteralFromGroupExpressions：用来删除Group By语句中的常数。如果Group By语句中全部是常数，则会将其替换为一个简单的常数0表达式
    + RemoveRepetitionFromGroupExpressions：将重复的表达式从Group By语句中删除
6. Batch Operator Optimizations：包含的优化规则可以分为3个模块，算子下推（Operator Push Down）、算子组合（Operator Combine）、常量折叠与长度消减（Constant Folding and Strength Reduction）
    + 算子下推：主要是将逻辑算子树中上层的算子节点尽量下推，使其靠近叶子节点，能够在不同程度上减少后续处理的数据量甚至简化后续的处理逻辑
        + PushProjectionThroughUnion：列剪裁下推
        + ReorderJoin：Join顺序优化
        + EliminateOuterJoin：OuterJoin消除
        + PushPredicateThroughJoin：谓词下推到Join算子
        + PushDownPredicate：谓词下推
        + LimitPushDown：Limit算子下推，将LocalLimit算子下推到Union All和Outer Join操作算子的下方，减少这两种算子在实际计算过程中需要处理的数据量
        + ColumnPruning：列剪裁
        + InferFiltersFromConstraints：约束条件提取
    + 算子组合：将逻辑算子树中能够进行组合的算子尽量整合在一起，避免多次计算，以提高性能，主要针对的是重分区（repartition）算子、投影（Project）算子、过滤（Filter）算子、Window算子、Limit算子和Union算子
        + CollapseRepartition：重分区组合
        + CollapseProject：投影算子组合
        + CollapseWindow：Window组合
        + CollapseFilters：过滤条件组合
        + CollapseLimits：Limit操作组合
        + CollapseUnions：Union算子组合
    + 常量折叠与长度消减：对于逻辑算子树中涉及某些常量的节点，可以在实际执行之前就完成静态处理
        + NullPropagation：Null提取
        + FoldablePropagation：可折叠算子提取
        + OptimizeIn：In操作优化
        + ConstantFolding：常数折叠，对于能够可折叠（foldable）的表达式会直接在EmptyRow上执行evaluate操作，从而构造新的Literal表达式
        + ReorderAssociativeOperator：重排序关联算子优化
        + LikeSimplification：Like算子简化
        + BooleanSimplification：Boolean算子简化
        + SimplifyConditionals：条件简化
        + RemoveDispensableExpressions：Dispensable表达式消除
        + SimplifyBinaryComparison：比较算子优化
        + PruneFilters：过滤条件裁剪，会详细的分析过滤条件，对总是能够返回true或false的过滤条件进行特别的处理
        + EliminateSorts：排序算子消除
        + SimplifyCasts：Cast算子简化
        + SimplifyCaseConversionExpressions:case表达式简化
        + RewriteCorrelatedScalarSubquery：依赖子查询重写
        + EliminateSerialization：序列化消除
        + RemoveAliasOnlyProject消除别名
7. Batch Check Cartesian Products：只有CheckCartesianProducts这一条优化规则，用来检测逻辑算子树中是否存在笛卡尔积类型的Join操作。必须在ReorderJoin规则执行之后才能执行，确保所有的Join条件收集完毕，当spark.sql.crossJoin.enabled参数设置为true时，该规则会被忽略
8. Batch Decimal Optimizations：只有DicimalAggregates这一条优化规则，用于处理聚合操作中与Decimal类型相关的问题。如果聚合查询中涉及浮点数的精度处理，性能就会受到很大的影响，对于固定精度的Decimal类型，DecimalAggregates规则将其当作unscaled Long类型来执行，这样可以加速聚合操作的速度
9. Batch Typed Filter Optimization：仅包含CombineTypedFilters这一条优化规则，用来对特定情况下的过滤条件进行合并，当逻辑算子树中存在两个TypedFilter过滤条件且针对同类型的对象条件时，CombineTypedFilters优化规则会将它们合并到同一个过滤函数中
10. Batch LocalRelation：主要用来优化与LocalRelation相关的逻辑算子树
    + ConvertToLocalRelation：将LocalRelation上的本地操作转换为另一个LocalRelation，当前仅处理Project投影操作
    + PropagateEmptyRelation：将包含空的LocalRelation进行中折叠
11. Batch OptimizeCodegen：仅包含OptimizeCodegen一条优化规则，用来对生成的代码进行优化，主要针对的是case when语句，当case when语句中的分支数目不超过配置中的最大数据时，该表达式才能执行代码生成
12. Batch RewriteSubquery：主要用来优化子查询
    + RewritePredicateSubquery：将特定的子查询谓词逻辑转换为left-semi/anti join操作，其中EXISTS和NOT EXISTS算子分别对应semi和anti类型的join，过滤条件会被当作Join的条件，IN和NOT IN也分别对应semi和anti类型的Join，过滤条件和选择的列都会被当作join的条件
    + CollapseProject：会将两个相邻的Project算子组合在一起并执行别名替换，整合成一个统一的表达式
13. Batch Optimize Metadata Only Query：只有OptimizeMetadataOnlyQuery这一条规则，用来优化执行过程中只需查找分区级别元数据的语句，适用于扫描的所有列都是分区列且包含聚合算子的情形，并且聚合算子需要满足以下情况之一：聚合表达式是分区列；分区列的聚合函数有DISTINCT算子；分区列的聚合函数中是否有DISTINCT算子不影响结果
14. Batch Extract Python UDF from Aggregate：该Batch仅执行一次，只包含ExtractPythonUDFFromAggregate这一条规则，用来提取出聚合操作中的Python UDF函数。主要针对采用PySpark提交查询的情形，将参与聚合的Python自定义函数提取出来，在聚合操作完成之后执行
15. Batch Prune File Source Table Partitions：该Batch仅执行一次，只包含PruneFileSourcePartitions这一条规则，用来对数据文件中的分区进行裁剪操作，当数据文件中定义了分区信息且逻辑算子树中的LogicalRelation节点上方存在过滤算子时，PruneFileSourcePartitions优化规则会尽可能地将过滤算子下推到存储层，这样可以避免读入无关的数据分区
16. Batch User Provided Optimizers：用于支持用户自定义的优化规则，ExperimentalMethods的extraOptimizations队列默认为空，用户只需要继承Rule`[LogicalPlan]`虚类，实现相应的转化逻辑就可以注册到优化规则队列中应用执行

## Spark SQL物理计划（PhysicalPlan）

物理计划是与底层平台紧密相关的，在此阶段，Spark SQL会对生成的逻辑算子树进行进一步处理，得到物理算子树，并将LogicalPlan节点及其所包含的各种信息映射成Spark Core计算模型的元素，如RDD、Transformation和Action等，以支持其提交执行。

在Spark SQL中，物理计划用SparkPlan表示，从Optimized LogicalPlan传入到Spark SQL物理计划提交并执行，主要经过3个阶段：
1. 由SparkPlanner将各种物理计划策略（Strategy）作用于对应的LogicalPan节点上，生成SparkPlan列表^[一个LogicalPlan可能产生多种SparkPlan]
2. 选取最佳的SparkPlan，Spark 2.1版本中直接在候选列表中直接用`next()`方法获取第一个
3. 提交前进行准备工作，进行一些分区排序方面的处理，确保SparkPlan各节点能够正确执行，这一步通过`prepareForExecution()`方法调用若干规则（Rule）进行转换

### SparkPlan

在Spark SQL中，当逻辑计划处理完毕后，会构造SparkPlanner并执行`plan()`方法对LogicalPlan进行处理，得到对应的物理计划（`Iterator[SparkPlan]`）^[一个逻辑计划可能会对应多个物理计划]。

SparkPlanner继承自SparkStrategies类，SparkStrategies类继承自QueryPlanner基类，`plan()`方法是现在QueryPlanner类中，SparkStrategies类本身不提供任何方法，而是在内部提供一批SparkPlanner会用到的各种策略（Strategy）实现，最后，在SparkPlanner层面将这些策略整合在一起，通过`plan()`方法进行逐个应用。SparkPlanner本身只是一个逻辑的驱动，各种策略的`apply`方法把逻辑执行计划算子映射成物理执行计划算子。SparkLater是一种特殊的SparkPlan，不支持执行（`doExecute()`方法没有实现），仅用于占位，等待后续步骤处理。

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members

class QueryPlanner {
    strategies: Seq[GenericStrategy[PhysicalPlan]]
    plan(plan: LogicalPlan): Iterator[PhysicalPlan]
    collectPlaceholders(plan: PhysicalPlan)：Seq[(PhysicalPlan, LogicalPlan)]
    prunePlans(plans: Iterator[PhysicalPlan])：Iterator[PhysicalPlan]
}

class SparkStrategies

class SparkPlanner {
    numPartitions: Int
    strategies: Seq[String]
    collectPlaceholders(plan: SparkPlan): Seq[(SparkPlan, LogicalPlan)]
    prunePlans(plans: Iterator[SparkPlan]): Iterator[SparkPlan]
    pruneFilterProject(...): SparkPlan
}

QueryPlanner <|-- SparkStrategies
SparkStrategies <|-- SparkPlanner

@enduml
```

生成物理计划的过程：`plan()`方法传入LogicalPlan作为参数，将strategies应用到LogicalPlan，生成物理计划候选集合（Candidates）。如果该集合中存在PlanLater类型的SparkPlan，则通过placeholder中间变量取出对应的LogicalPlan后，递归调用`plan()`方法，将PlanLater替换为子节点的物理计划，最后，对物理计划列表进行过滤，去掉一些不够搞笑的物理计划。

```Scala
def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    val candidates = strategies.iterator.flatMap(_(plan))
    val plans = candidates.flatMap { candidate =>
        val placeholders = collectPlaceholders(candidate)
        if (placeholders.isEmpty) {
            Iterator(candidate)
        } else {
            placeholders.iterator.foldLeft(Iterator(candidate)) {
                case (candidatesWithPlaceholders, (placeholder, LogicalPlan)) => 
                    val childPlans = this.plan(logicalPlan)
                    candidatesWithPlaceholders.flatMap { candidateWithPlaceholders => 
                        childPlans.map { childPlan => 
                            candidateWithPlaceholders.transformUp {
                                case p if p == placeholder => childPlan
                            }
                        }
                    }
            }
        }
    }
    val pruned = prunePlans(plans)
    assert(pruned.hashNext, s"No plan for $plan")
    pruned
}
```

物理执行计划策略都继承自GenericStrategy类，其中定义了`planLater`和`apply`方法，SparkStrategy类继承自GenericStrategy类，对其中的planLater进行了实现，根据传入的LogicalPlan直接生成PlanLater节点。各种具体的Strategy都实现了`apply`方法，将传入的LogicalPlan转换为SparkPlan的列表，如果当前的执行策略无法应用于该LogicalPlan节点，则返回的物理执行计划列表为空。在实现上，各种Strategy会匹配传入的LogicalPlan节点，根据节点或节点组合的不同情形实行一对一的映射或多对一的映射。如BasicOperators中实现了各种基本操作的转换，其中列出了大量的映射关系。多对一的情况涉及对多个LogicalPlan节点进行组合转换，称为逻辑算子树的模式匹配，目前逻辑算子树的节点模式共有4种：

+ ExtractEquiJoinKeys：针对具有相等条件的join操作的算子集合，提取出其中的Join条件、左子节点和右子节点等信息
+ ExtractFiltersAndInnerJoins：收集Inner类型Join操作中的过滤条件，目前仅支持对左子树进行处理
+ PhysicalAggregation：针对聚合操作，提取出聚合算子中的各个部分，并对一些表达式进行初步的转换
+ PhysicalOperation：匹配逻辑算子树中的Project和Filter等节点，返回投影列、过滤条件集合和子节点

在SparkPlanner中默认添加了8种Strategy来生成物理计划：
+ 文件数据源策略（FileSourceStrategy）：面向的是来自文件的数据源，能够匹配PhysicalOperation模式^[Project节点加上Filter节点]加上LogicalRelation节点，在这种情况下，该策略会根据数据文件信息构建FileSourceScanExec物理执行计划，并在此物理执行计划后添加过滤（FilterExec）与列剪裁（ProjectExec）物理计划
+ DataSourcesStrategy：
+ DDL操作策略（DDLStrategy）：仅针对CreateTable和CreateTempViewUsing这两种类型的节点，这两种情况都直接生成ExecutedCommandExec类型的物理计划
+ SpecialLimits：
+ Aggregation：
+ JoinSelection：
+ 内存数据表扫描策略（InMemoryScans）：主要针对InMemoryRelation LogicalPlan节点，能够匹配PhysicalOperation模式加上InMemoryRelation节点，生成InMemoryTableScanExec，并调用SparkPlanner中的pruneFilterProject方法对其进行过滤和列剪裁
+ 基本操作策略（BasicOperators）：专门针对各种基本操作类型的LogicalPlan节点，如排序、过滤等，一般一对一地进行映射即可，如Sort对应SortExec、Union对应UnionExec等

在物理算子树中，叶子类型的SparkPlan节点负责从无到有地创建RDD，每个非叶子类型的SparkPlan节点等价于在RDD上进行一次Transformation，即通过调用`execute()`函数转换成新的RDD，最终执行`collect()`操作触发计算，返回结果给用户。SparkPlan在对RDD做Transformation的过程中除对数据进行操作外，还可能对RDD的分区做调整，另外，SparkPlan除实现execute方法外，还支持直接执行executeBroadcast将数据广播到集群。SparkPlan的主要功能可以划分为3大块：
+ Metadata与Metric体系：将节点元数据（Metadata）与指标（Metric）信息以Key-Value的形式保存在Map数据结构中。元数据和指标信息是性能优化的基础，SparkPlan提供了Map类型的数据结构来存储相关信息，元数据信息Metadata对应Map中的key和value都是字符串类型，一般情况下，元数据主要用于描述数据源的一些基本信息（如数据文件的格式、存储路径等），指标信息Metrics对应Map中的key为字符串类型，而value部分是SQLMetrics类型，在Spark执行过程中，Metrics能够记录各种信息，为应用的诊断和优化提供基础。在对Spark SQL进行定制时，用户可以自定义一些指标，并将这些指标显示在UI上，一方面，定义越多的指标会得到越详细的信息，另一方面，指标信息要随着执行过程而不断更新，会导致额外的计算，在一定程度上影响性能
+ Partitioning与Ordering体系：进行数据分区（Partitioning）与排序（Ordering）处理。requiredChildChildDistribution和requiredChildOrdering分别规定了当前SparkPlan所需的数据分布和数据排序方式列表，本质上是对所有子节点输出数据（RDD）的约束。outputPartitioning定义了当前SparkPlan对输出数据（RDD）的分区操作，outputOrdering则定义了每个数据分区的排序方式
+ 执行操作部分：以execute和executeBroadcast方法为主，支持提交到Spark Core去执行

根据SparkPlan的子节点数目，可以将其大致分为4类：LeafExecNode、UnaryExecNode、BinaryExecNode和其他。

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members

class SparkPlan {
    medadata: Map[String, String]
    metrics: Map[String, SQL Metric]
    resetMetrics: Unit
    longMetric(name: String): SQLMetric
    ---
    outputPartitioning: Partitioning
    requiredChildDistribution: Seq[Distribution]
    outputOrdering: Seq[SortOrder]
    requiredChildOrdering: Seq[Seq[SortOrder]]
    ---
    doExecuteBroadcast[T](): Broadcast[T]
    executeBroadcast[T](): Broadcast[T]
    doExecute(): RDD[InternalRow]
    execute(): RDD[InternalRow]
}
@enduml
```

LeafExecNode类型的SparkPlan负责初始RDD的创建：

+ RangeExec：利用SparkContext中的parallelize方法生成给定范围内的64位数据的RDD
+ LocalTableScanExec
+ ExternalRDDScanExec
+ HiveTableScanExec：会根据Hive数据表存储的HDFS信息直接生成HadoopRDD
+ InMemoryTableScanExec
+ DataSourceScanExec
    + FileSourceScanExec：根据数据表所在的源文件生成FileScanRDD
    + RawDataSourceScanExec
+ RDDScanExec
+ StreamingRelationExec
+ ReusedExchangeExec
+ PlanLater
+ MyPlan

UnaryExecNode类型的SparkPlan负责RDD的转换操作：

+ InputAdapter
+ Exchange：对子节点产生的RDD进行重分区
    + BroadcastExchangeExec
    + ShuffleExchange
+ InsertIntoHiveTable
+ MapGroupsExec
+ ProjectExec：对子节点产生的RDD进行列剪裁
+ ReferenceSort
+ SampleExec：对子节点产生的RDD进行采样
+ ScriptTransformation
+ SortAggregateExec
+ SortExec：按照一定条件对子节点产生的RDD进行排序
+ StateStoreSaveExec
+ SubqueryExec
+ GenerateExec
+ AppendColumnsExec
+ BaseLimitExec
    + LocalLimitExec
    + GlobalLiimitExec
+ ExpandExec
+ CoalesceExec
+ CollectLimitExec
+ DebugExec
+ ObjectConsumerExec
    + AppendColumnsWithObjectExec
    + MapElementsExec
    + MapPartitionsExec
    + SerializeFromObjectExec
+ DeserializeToObjectExec
+ ExceptionInjectingOpeartor
+ FilterExec：对子节点产生的RDD进行行过滤操作
+ FlatMapGroupsInRExec
+ WholeStageCodegenExec：将生成的代码整合成单个Java函数
+ HashAggregateExec
+ TakeOrderedAndProjectExec
+ WindowExec

BinaryExecNode类型的SparkPlan除CoGroupExec外都是不同类型的Join执行计划：
+ BroadcastHashJoinExec
+ BroadcastNestedLoopJoinExec
+ CartesianProductExec
+ CoGroupExec：处理逻辑类似Spark Core中CoGroup操作，将两个要进行合并的左、右子SparkSplan所产生的RDD，按照相同的key值组合到一起，返回的结果中包含两个Iterator，分别代表左子树中的值与右子树中的值
+ ShuffledHashJoinExec
+ SortMergeJoinExec

其他类型的SparkPlan：
+ CodegenSupport
+ UnionExec
+ DummySparkPlan：对每个成员赋予默认值
+ FastOperator
+ MyPlan：用于在Driver端更新Metric信息
+ EventTimeWatermarkExec
+ BatchEvalPythonExec
+ ExecutedCommandExec
+ OutputFakerExec
+ StatefulOperator
+ ObjectProducerExec

在SparkPlan分区体系实现中，Partitioning表示对数据进行分区的操作，Distribution则表示数据的分布，在Spark SQL中，Distribution和Partitioning均被定义为接口，具体实现有多个类。

Distribution定义了查询执行时，同一个表达式下的不同数据元组在集群各个节点上的分布情况，可以用来描述以下两种不同粒度的数据特征：一是节点间（Inner-node）分区信息，即数据元组在集群不同的物理节点上是如何分区的，用来判断某些算子（如Aggregate）能否进行局部计算（Partial Operation），避免全局操作的代价；一是分区数据内（Inner-partition）排序信息，即单个分区内数据是如何分布的。在Spark 2.1中包括以下5种Distribution实现：
+ UnspecifiedDistribution：未指定分布，无需确定数据元组之间的位置关系
+ AllTuples：只有一个分区，所有的数据元组存放在一起（Co-located）
+ BroadcastDistribution：广播分布，数据会被广播到所有节点上，构造参数mode为广播模式（BroadcastMode），广播模式可以为原始数据（IdentityBroadcastMode）或转换为HashedRelation对象（HashedRelationBroadcastMode）
+ ClusteredDistribution：构造参数clustering是`Seq[Expression]`类型，起到了哈希函数的效果，数据经过clustering计算后，相同value的数据元组会被存放在一起（Co-located），如果有多个分区的情况，则相同数据会被存放在同一个分区中；如果只能是单个分区，则相同的数据会在分区内连续存放
+ OrderedDistribution：构造参数ordering是`Seq[SortOrder]`类型，该分布意味着数据元组会根据ordering计算后的结果排序

Partitioning定义了一个物理算子输出数据的分区方式，具体包括子Partitionging之间、目标Partitioning和Distribution之间的关系，描述了SparkPlan中进行分区的操作，类似直接采用API进行RDD的repartition操作。Partitioning接口中包括1个成员变量和3个函数来进行分区操作：
+ `numPartitions`：指定该SparkPlan输出RDD的分区数目
+ `satisfies(required: Distribution)`：当前的Partitioning操作能否得到所需的数据分布，当不满足时一般需要进行repartition操作，对数据进行重新组织
+ `compatibleWith(other：Partitioning)`：当存在多个子节点时，需要判断不同的子节点的分区操作是否兼容，只有当两个Partitioning能够将相同key的数据分发到相同的分区时，才能够兼容
+ `guarantees(other: Partitioning)`：如果`A.gurantees(B)`为真，那么任何A进行分区操作所产生的数据行业能够被B产生，这样，B就不需要再进行重分区操作，该方法主要用来避免冗余的重分区操作带来的性能代价。在默认情况下，一个Partitioning仅能够gurantee（保证）等于它本身的Partitioning（相同的分区数目和相同的分区策略等）

Partitioning接口的具体实现有以下几种：
+ UnknownPartitioning：不进行分区
+ RoundRobinPartitioning：在1-numPartitions范围内轮询式分区
+ HashPartitioning：基于哈希的分区方式
+ RangePartitioning：基于范围的分区方式
+ PartitioningCollection：分区方式的集合，描述物理算子的输出

```scala
case class HashPartitioning(expressions: Seq[Expression], numPartitions: Int) extends Expression with Partitioning with Unevaluable {
    override def satisfies(required: Distribution): Boolean = required match {
        case UnspecifiedDistribution => true
        case ClusteredDistribution(requiredClustering) => expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
    }
    override def comparitableWith(other: Partitioning): Boolean = other match {
        case o: HashPartitioning => this.semanticEquals(o)
        case _ => false
    }
    override def guarantees(other: Partitioning): Boolean = other match {
        case o: HashPartitioning => this.semanticEquals(o)
        case _ => false
    }
}
```

在SparkPlan默认实现中，`outputPartitioning`设置为`UnknownPartitioning(0)`，`requiredChildDistribution`设置为`Seq[UnspecifiedDistribution]`，且在数据有序性和排序操作方面不涉及任何动作。FileSourceScanExec中的分区排序信息会根据数据文件构造的初始的RDD进行设置，如果没有bucket信息，则分区与排序操作将分别为最简单的UnknownPartitioning与Nil；当且仅当输入文件信息中满足特定的条件时，才会构造HashPartitioning与SortOrder类。FilterExec（过滤执行算子）与ProjectExec（列剪裁执行算子）中分区与排序方式仍然沿用其子节点的方式，即不对RDD的分区与排序进行任何的重新操作。通常情况下，LeafExecNode类型的SparkPlan会根据数据源本身的特点（包括分块信息和有序性特征）构造RDD与对应的Partitioning和Ordering方式，UnaryExecNode类型的SparkPlan大部分会沿用其子节点的Partitioning和Ordering方式（SortExec等本身具有排序操作的执行算子例外），而BinaryExecNode往往会根据两个子节点的情况综合考虑。

得到SparkPlan后，还需对树型结构的物理计划进行全局的整合处理和优化。在QueryExecution中，最后阶段由`prepareforExecution`方法对传入的SparkPlan进行处理而生成executedPlan，处理过程仍然基于若干规则，主要包括对Python中UDF的提取、子查询的计划生成等。
+ python.ExtractPythonUDFs：提取Python中的UDF函数
+ PlanSubqueries：处理物理计划中的ScalarSubquery和PredicateSubquery这两种特殊的子查询。Spark 2.0版本及以上支持两种特殊情形的子查询，即Scalar类型和Predicate类型。Scalar类型子查询返回单个值，又分为相关的（Correlated）和不相关的（Uncorrelated）类型，Uncorrelated子查询和主查询不存在相关性，对于所有的数据行都返回相同的值，在主查询执行之前，会首先执行，Correlated子查询包含了外层主查询中的相关属性，会等价转换为Left Join算子。Predicate类型子查询作为过滤谓词使用，可以出现在EXISTS和IN语句中。PlanSubqueries处理这两种特殊子查询的过程为，遍历物理算子树中的所有表达式，碰到ScalarSubquery或PredicateSubquery表达式时，进入子查询中的逻辑，递归得到子查询的物理执行计划（executedPlan），然后封装为ScalarSubquery和InSubquery表达式
    ```scala
    case class PlanSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
        def apply(plan: SparkPlan): SparkPlan = {
            plan.transformAllExpressions {
                case subquery: expressions.ScalarSubquery =>
                    val executedPlan = new QueryExecution(sparkSession, subquery.plan).executedPlan
                    ScalarSubquery(SubqueryExec(s"subquery${subquery.exprId.id}", executePlan), subquery.exprId)
                case expressions.PredicateSubquery(query, Seq(e: Expression), _, exprId) =>
                    val executePlan = new QueryExecution(sparkSession, query).executedPlan
                    InSubquery(e, SubqueryExec(s"subquery${exprId.id}", executedPlan), exprId)
            }
        }
    }
    ```
+ EnsureRequirements：用来确保物理计划能够执行所需要的前提条件，包括对分区和排序逻辑的处理
+ CollapseCodegenStages：代码生成相关
+ ReuseExchange：Exchange节点重用
+ ReuseSubquery：子查询重用

```scala
lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

protected def prepareForexecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apple(sp) }
}
```

EnsureRequirements在遍历SparkPlan的过程中，当匹配到Exchange节点（ShuffleExchange）且其子节点也是Exchange类型时，会检查两者的Partitioning方法，判断能否消除多余的Exchange节点，另外，遍历过程中会逐个调用`ensureDistributionAndOrdering`方法来确保每个节点的分区与排序需求，核心逻辑大致分为以下3步：
1. 添加Exchange节点：Exchange节点是实现数据并行化的重要算子，用于解决数据分布（Distribution）相关问题，有BroadcastExchangeExec和ShuffleExechange两种子类，ShuffleExchange会通过Shuffle操作进行重分区处理，BroadcastExchangeExec则对应广播操作。有两种情形需要添加Exchange节点：数据分布不满足，子节点的物理计划输出数据无法满足（Satisfies）当前物理计划处理逻辑中对数据分布的要求；数据分布不兼容，当前物理计划为BinaryExecNode类型，即存在两个子物理计划时，两个子物理计划的输出数据可能不兼容（Compatile）。在`ensureDistributionAndOrdering`方法中，添加Exchange节点的过程可以细分为两个阶段，分别针对单个节点和多个节点，第一个阶段是判断每个子节点的分区方式是否可以满足（Satisfies）对应所需的数据分布，如果满足，则不需要创建Exchange节点；否则根据是否广播来决定添加何种类型的Exchange节点^[例如，SortMerge类型的Join节点中requiredChildDistribution列表为`[ClusteredDistribution(leftKeys), ClusteredDistribution(rightKeys)]`，假设两个子节点的Partitioning都无法输出该数据分布，那么就会添加两个ShuffleExchange节点]。第二个阶段专门针对多个子节点的情形，如果当前SparkPlan节点需要所有子节点分区方式兼容但并不能满足时，就需要创建ShuffleExchange节点^[例如，SortMerge类型的Join节点就需要两个子节点的Hash计算方式相同，如果所有的子节点outputPartitioning能够保证由最大分区数目创建新的Partitioning，则子节点输出的数据并不需要重新Shuffle，只需要使用已有的outputPartitioning方式即可，没有必要创建新的Exchange节点；否则，至少有一个子节点的输出数据需要重新进行Shuffle操作，重分区的数目（NumPartitions）根据是否所有的子节点输出都需要Shuffle来判断，若是，则采用默认的Shuffle分区配置数目；否则，取子节点中最大的分区数目]。ShuffleExchange执行得到的RDD称为ShuffledRowRDD，ShuffleExchange执行`doExecute`时，首先会创建ShuffleDependency，然后根据ShuffleDependency构造ShuffleRowRDD,ShuffleDependency的创建分为以下两种情况，一是包含ExchangeCoordinator协调器，如果需要ExchangeCoordinator协调ShuffleRowRDD的分区，则需要先提交该ShuffleExchange之前的Stage到Spark集群执行，完成之后确定ShuffleRowRDD的分区索引信息，一是直接创建，直接执行`prepareShuffleDependency`方法来创建RDD的依赖，然后根据ShuffleDependency创建ShuffledRowRDD对象，在这种情况下，ShuffleRowRDD中每个分区的ID与Exchange节点进行shuffle操作后的数据分区是一对一的映射关系。直接创建ShuffleDependency的过程中，首先会根据传入的newPartitioning信息构造对应的partiitoner，然后基于该partitioner生成ShuffleDependency最重要的构造参数`rddWithPartitionIds(RDD[Product2[Int, InternalRow]])`类型，其中`[Int, InternalRow]`代表某行数据及其所在分区的partitionId，其取值范围为`[0, numPartitions-1]`，最终在rddWithPartitionIds基础上创建ShuffleDependency对象
    ```scala
    // 第一阶段
    // defaultNumPreShufflePartitions决定了Shuffle操作过程中分区的数目，是一个用户可配置的参数（spark.sql.shuffle.partitions），默认为200
    // createPartitioning方法会根据数据分布与分区数目创建对应的分区方式，具体对应关系是：AllTuples对应SinglePartition，ClusteredDistribution对应HashPartitioning，OrderedDistribution对应RangePartitioning
    children = children.zip(requiredChildDistributions).map {
        case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
            child
        case (child, BroadcastDistribution(mode)) =>
            BroadcastExchangeExec(mode, child)
        case (child, distribution) =>
            ShuffleExchange(createPartitioning(distribution, defaultNumPreShufflePartitions), child)
    }

    // 第二阶段
    if (children.length > 1 && requiredChildDistributions.exists(requireCompatiblePartitioning) && !Partitioning.allCompatible(children.map(_.outputPartitioning))) {
        val maxChildrenNumPartitions = children.map(_.outputPartitioning.numPartitions).max
        val useExistingPartitioning = children.zip(requiredChildDistributions).forall {..}
        children = if (useExistingPartitioning) {
            children
        } else {
            val numPartitions = {...}
            children.zip(requiredChildDistributions).map {
                case (child, distribution) =>
                    val targetPartitioning = createPartitioning(distribution, numPartitions)
                    if (child.outputPartitioning.guarantees(targetPartitioning)) {
                        child
                    } else {
                        child match {
                            case ShuffleExchange(_, c, _) => ShuffleExchange(targetPartitioning, c)
                            case _ => ShuffleExchange(targetPartitioning, child)
                        }
                    }
            }
        }
    }
    ```
2. 应用ExchangeCoordinator协调分区：ExchangeCoordinator用来确定物理计划生成的Stage之间如何进行Shuffle行为，其作用在于协助ShuffleExchange节点更好地执行。ExchangeCoordinator针对的是一批SparkPlan节点，这些节点需要满足两个条件，一个是Spark SQ的自适应机制开启（`spark.sql.adaptive.enabled`为`true`），一个是这批节点支持协调器（一种情况是至少存在一个ShuffleExchange类型的节点且所有节点的输出分区方式都是HashPartitioning，另一种情况是节点数目大于1且每个节点输出数据的分布都是ClusteredDistribution类型）。当ShuffleExchange中加入了ExchangeCoordinator来协调分区数目时，需要知道子物理计划输出的数据统计信息，因此在协调之前需要将ShuffleExchange之前的Stage提交到集群执行来获取相关信息
3. 添加SortExec节点：排序的处理在分区处理（创建完Exchange）之后，只需要对每个节点单独处理。当且仅当所有子节点的输出数据的排序信息满足当前节点所需时，才不需要添加SortExec节点，否则，需要在当前节点上添加SortExec为父节点。
    ```scala
    children = children.zip(requiredChildOrderings).map {case (child, requiredOrdering) =>
        if (requiredOrdering.nonEmpty) {
            val orderingMatched = if (requiredOrdering.length > child.outputOrdering.length) {false} else {
                requiredOrdering.zip(child.outputOrdering).forall {
                    case (requiredOrder, childOutputOrder) => requiredOrder.semanticEquals(childOutputOrder)
                }
            }
            if (!orderingMatched) {SortExec(requiredOrdering, global = false, child = child)} else {child}
        } else {
            child
        }
    }
    ```

Ensurerequirements规则的处理逻辑结束后，调用TreeNode中的`withNewChildren`将SparkPlan中原有的子节点替换为新的子节点。