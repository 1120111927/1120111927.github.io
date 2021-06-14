---
title: HiveQL编译
date: "2020-04-13"
description: HiveQL编译为MR作业主要分为六个步骤：语法分析、语义分析、生成逻辑计划、逻辑优化、生成物理计划、物理优化。
tags: HiveQL Compiler, Parser, SemanticAnalysis, Plan generation, Task generation, Optimizer, Map/Reduce Execution Engine
---

```bob-svg
                          Semantic      Logical Plan                   Logical
+--------+ Parser +-----+ Analyze +----+ Generator  +---------------+ Optimizer
| HiveQL |------->| AST |-------->| QB |----------->| Operator Tree |-----------+
+--------+        +-----+         +----+            +---------------+           |
                                                                                |
+-------------------------------------------------------------------------------+
|
|                             Physical Plan               Physical
|            +---------------+  Generator   +-----------+ Optimizer +-----------+
+----------->| Operator Tree |------------->| DAG of MR |---------->| DAG of MR |
             +---------------+              +-----------+           +-----------+
```

HiveQL编译过程分为六个阶段：

1. 语法分析（Parser）：将HiveQL转化为AST
2. 语义分析（SemanticAnalyzer）：将AST转化为QueryBlock
3. 生成逻辑计划（Logical Plan Generator）：将QueryBlock转化为OperatorTree
4. 逻辑优化（Logical Optimizer）：进行OperatorTree变换，合并不必要的ReduceSinkOperator，减少shuffle数据量
5. 生成物理计划（Physical Plan Generator）：将OperatorTree转化为DAG of MR
6. 优化物理计划（Physical Optimizer）：优化DAG of MR

类`org.apache.hadoop.hive.ql.Driver`的`compile()`方法中实现了上述过程，主要代码^[本文涉及代码均为hive 2.0.1版本]如下：

```java
public int compile(String command, boolean resetTaskIds) {
    command = new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
            return SessionState.get().getHiveVariables();
        }
    }).substitute(conf, command);
    String queryStr = command;
    // 编辑命令避免日志输出敏感数据
    queryStr = HookUtils.redactLogString(conf, command);

    // 语法解析
    perfLogger.PerfLogBegin(Class_NAME, PerfLogger.PARSE);
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(command, ctx);
    tree = ParseUtils.findRootNonNullToken(tree);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.PARSE);

    // 语义分析
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.ANALYZE);
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);

    sem.analyze(tree, ctx);  // 内部调用具体SemanticAnalyzer的analyzeInternal()方法

    // Record any ACID compliant FileSinkOperators we saw so we can add our transaction ID to them later
    acidSinks = sem.getAcidFileSinks();

    // validate the plan
    sem.validate();
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.ANALYZE);
    
    // 获取输出模式
    schema = getSchema(sem, conf);

    plan = new QueryPlan(queryStr, sem, perfLogger.getStartTime(PerfLogger.DRIVER_RUN), queryId, queryState.getHiveOperation(), schema);

    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, queryStr);

    conf.set("mapreduce.workflow.id", "hive_" + queryId);
    conf.set("mapreduce.workflow.name", queryStr);

    // 初始化FetchTask
    if (plan.getFetchTask() != null) {
        plan.getFetchTask().initialize(queryState, plan, null, ctx.getOpContext());
    }

    // 权限检查
    if (!sem.skipAuthorization()) && HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {
        perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.DO_AUTHORIZATION);
        doAuthorization(sem, command);
        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.DO_AUTHORIZATION);
    }

    // 输出explain信息
    if (conf.getBoolVar(ConfVars.HIVE_LOG_EXPLAIN_OUTPUT)) {
        String explainOutput = getExplainOutput(sem, plan, tree);
        if (explainOutput != null) {
            LOG.info("EXPLAIN output for queryid " + queryId + " : " + explainOutput);
        }
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.COMPILE);
}
```

Hive SQL编译相关代码都在包`org.apache.hadoop.hive.ql.parse`中，使用时添加下述依赖即可：

```gradle
// https://mvnrepository.com/artifact/org.apache.hive/hive-exec
compile group: 'org.apache.hive', name: 'hive-exec', version: '2.3.4', classifier: 'core'
```

## 语法分析

Hive使用Antlr完成SQL的词法、语法解析，将输入SQL转化为抽象语法树（AST，Abstract Syntax Tree），由类`org.apache.hadoop.hive.ql.parse.ParseDriver`负责，主要代码为：

```java
public ASTNode parse(String command, Context ctx, String viewFullyQualifiedName) throws ParseException {
    HiveLexerX lexer = new HiveLexerX(new ANTLRNoCaseStringStream(command));
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    HiveParser parser = new HiveParser(tokens);
    parser.setTreeAdaptor(adaptor);  // adaptor用于使antlr返回ASTNode而不是CommonTree结点，ql.lib中定义的图遍历算法和rules framework都是用于ASTNode的，ASTNode是antlr CommonTree类的包装类并且实现了Node接口
    HiveParser.statement_return r = parser.statement();
    ASTNode tree = (ASTNode) r.getTree();
    tree.setUnknownTokenBoundaries();
    return tree;
}
```

词法文件位于`$hive_src/ql/src/java/org/apache/hadoop/hive/ql/parse`文件夹，采用了composite grammars技术（antlr v3.1引入，允许在逻辑上把一个大语法划分成几大块，独立实现，然后合并在一起，用于解决把所有语法塞入到一个文件里导致编译出来的java文件过大和逻辑过多引起的不易阅读问题）。词法文件定义了关键字、token以及从HiveQL到AST的转换

+ `HiveLexer.g`：词法规则，提供token定义
+ `HiveParser.g`：语法规则，导入了`FromClause.g`、`SelectClause.g`、`IdentifierParser.g`（`import SelectClauseParser, FromClauseParser, IdentifiersParser;`）
+ `FromClauseParser.g`：from从句语法规则，被包含于`HiveParser.g`
+ `SelectClauseParser.g`：select从句语法规则，被包含于`HiveParser.g`
+ `IdentifiersParser.g`：自定义函数语法规则，被包含于`HiveParser.g`。hive中的自定义函数范围很广，各种内建的库函数，包括操作符之类的都被归为自定义函数

整个规则由statement开始，statement由解释语句explainStatement或执行语句execStatement组成：

```antlr
statement
  : explainStatement EOF
  | execStatement EOF
  ;
```

解释语句explainStatement由KW_EXPLAIN开始，接着是可选项KW_EXTENDED、KW_FORMATTED、KW_DEPENDENCY、KW_LOGICAL，后紧跟执行语句：

```antlr
explainStatement
@init { pushMsg("explain statement", state); }
@after { popMsg(state); }
  : KW_EXPLAIN (
      explainOption* execStatement -> ^(TOK_EXPLAIN execStatement explainOption*)
      |
      KW_REWRITE queryStatementExpression[true] -> ^(TOK_EXPLAIN_SQ_REWRITE queryStatementExpression))
  ;

explainOption
@init { msgs.push("explain option"); }
@after { msgs.pop(); }
  : KW_EXTENDED|KW_FORMATTED|KW_DEPENDENCY|KW_LOGICAL|KW_AUTHORIZATION
  ;
```

执行语句execStatement由查询（query）、装载（load）、导出（export）、导入（import）、数据定义（ddl）等语句组成：

```antlr
execStatement
@init { pushMsg("statement", state); }
@after { popMsg(state); }
  : queryStatementExpression[true]
  | loadStatement
  | exportStatement
  | importStatement
  | ddlStatement
  | deleteStatement
  | updateStatement
  | sqlTransactionStatement
  ;
```

装载语句关注路径、表/分区、是否本地、是否覆盖：

```antlr
loadStatement
@init { pushMsg("load statement", state); }
@after { popMsg(state); }
  : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tableOrPartition) -> ^(TOK_LOAD $path $tab $islocal? $isoverwrite?)
  ;
```

导出语句关注表/分区、导出路径

```antlr
exportStatement
@init { pushMsg("export statement", state); }
@after { popMsg(state); }
  : KW_EXPORT KW_TABLE (tab=tableOrPartition) KW_TO (path=StringLiteral) replicationClause? -> ^(TOK_EXPORT $tab $path replicationClause?)
  ;

replicationClause
@init { pushMsg("replication clause", state); }
@after { popMsg(state); }
  : KW_FOR (isMetadataOnly=KW_METADATA)? KW_REPLICATION LPAREN (replId=StringLiteral) RPAREN -> ^(TOK_REPLICATION $replId $isMetadataOnly?)
  ;
```

导入语句关注表/分区、导入路径、表位置、是否外部

```antlr
importStatement
@init { pushMsg("import statement", state); }
@after { popMsg(state); }
  : KW_IMPORT ((ext=KW_EXTERNAL)? KW_TABLE (tab=tableOrPartition))? KW_FROM (path=StringLiteral) tableLocation? -> ^(TOK_IMPORT $path $tab? $ext? tableLocation?)
  ;
```

### antlr v3语法

`@init`表示进入规则时执行后面`{}`中的动作

`@after`表示规则完成后执行后面`{}`中的动作

`->`表示构建抽象语法树`^(rootnode leafnode1 leafenode2 ...)`

`variable=KeyWord`表示variable作为别名引用KeyWord，引用形式为`$variable`

### `ASTNode`类

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
class CommonTree {
    Token token
    int startIndex
    int stopIndex
    CommonTree parent
    int childIndex
    CommonTree()
    CommonTree(CommonTree node)
    CommonTree(Token t)
    Token getToken()
    Tree dupNode()
    boolean isNil()
    int getType()
    String getText()
    int getLine()
    int getCharPositionInLine()
    int getTokenStartIndex()
    void setTokenStartIndex(int index)
    int getTokenStopIndex()
    void setTokenStopIndex(int index)
    void setUnknownTokenBoundaries()
    int getChildIndex()
    Tree getParent()
    void setParent(Tree t)
    void setChildIndex(int index)
    String toString()
}
interface Node {
    List<? extends Node> getChildren()
    String getName()
}
class ASTNode {
    ASTNodeOrigin origin
    int startIndx
    int endIndx
    ASTNode rootNode
    boolean isValidASTStr
    boolean visited
    ASTNode()
    ASTNode(Token t)
    ASTNode(ASTNode node)
    ArrayList<Node> getChildren()
    String getName()
    void setUnknownTokenBoundaries()
    void getRootNodeWithValidASTStr()
    void setParent(Tree t)
    void addChild(Tree t)
    void addChildren(List kids)
    void setChild(int i, Tree t)
    void insertChild(int i, Object t)
    Object deleteChild(int i)
    replaceChildren(int startChildIndex, int stopChildIndex, Object t)
    String toStringTree()
    String toStringTree(ASTNode rootNode)

}

CommonTree <|-- ASTNode
Node <|-- ASTNode
@enduml
```

`Node`：

+ `getChildren()`：返回子结点数组，用于图遍历算法
+ `getName()`：返回结点名称，用于规则匹配

## 语义分析

语义分析功能由`BaseSemanticAnalyzer`及其子类实现。AST比较抽象，不够结构化，也不携带表、字段相关信息，不方便直接翻译为MapReduce任务。语义分析阶段将AST分模块存入QueryBlock并携带对应的元数据信息，来将SQL进一步抽象和结构化，为生成逻辑执行计划做准备。

hive采用了工厂模式（简单工厂）来实现语义模块之间的关系，工厂（SemanticAnalyzerFactory）根据抽象语法树的根结点来生产具体的语法处理器（BaseSemanticAnalyzer子类）。

```Java
public final class SemanticAnalyzerFactory {
  // ...
  public static BaseSemanticAnalyzer get(HiveConf conf, ASTNode tree)
      throws SemanticException {
    if (tree.getToken() == null) {
      throw new RuntimeException("Empty Syntax Tree");
    } else {
      // ...
      switch (tree.getType()) {
      case HiveParser.TOK_EXPLAIN:
        return new ExplainSemanticAnalyzer(conf);
      case HiveParser.TOK_LOAD:
        return new LoadSemanticAnalyzer(conf);
      // ...
      default: {
        SemanticAnalyzer semAnalyzer = HiveConf
            .getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED) ? new CalcitePlanner(conf)
            : new SemanticAnalyzer(conf);
        return semAnalyzer;
      }
      }
    }
  }
  // ...
}
```

各个具体实现类的具体意义：

|具体实现类|功能|对应根结点|
|---|---|---|
|ExplainSemanticAnalyzer|解释语句的语义分析|TOK\_EXPLAIN|
|LoadSemanticAnalyzer|装载语句的语义分析|TOK\_LOAD|
|ExportSemanticAnalyzer|导出语句的语义分析|TOK\_EXPORT|
|ImportSemanticAnalyzer|导入语句的语义分析|TOK\_IMPORT|
|DDLSemanticAnalyzer|`alter table`等数据定义语句（DDL）的语义分析|TOK\_CREATEDATABASE、TOK\_DROPDATABASE、TOK\_SWITCHDATABASE等|
|FunctionSemanticAnalyzer|函数语句的语义分析|TOK\_CREATEFUNCTION、TOK\_DROPFUNCTION|
|ColumnStatsSemanticAnalyzer|列统计语句的语义分析|TOK\_ANALYZE|
|MacroSemanticAnalyzer|宏语句的语义分析|TOK\_CREATEMACRO、TOK\_DROPMACRO|
|UpdateDeleteSemanticAnalyzer|更新删除语句的语义分析|TOK\_UPDATE\_TABLE、TOK\_DELETE\_FORM|
|SemanticAnalyzer|查询（query）、数据操纵语句（DML）及`create table`等数据定义语句（DDL）的语义分析|TOK_START_TRANSACTION、TOK_COMMIT等|

```plantuml
@startuml
!include https://raw.githubusercontent.com/bschwarz/puml-themes/master/themes/sketchy-outline/puml-theme-sketchy-outline.puml
hide empty members
class SemanticAnalyzerFactory {
    BaseSemanticAnalyzer get(HiveConf conf, ASTNode tree)
}
abstract class BaseSemanticAnalyzer {
void analyze(ASTNode ast, Context ctx)
}
class ExplainSemanticAnalyzer
class LoadSemanticAnalyzer
class ExportSemanticAnalyzer
class ImportSemanticAnalyzer
class XxxSemanticAnalyzer
class SemanticAnalyzer {
    boolean genResolvedParseTree(ASTNode ast, PlannerContext plannerCtx)
    void processPositionAlias(ASTNode ast)
    ASTNode analyzeCreateTable(ASTNode ast, QB qb, PlannerContext plannerCtx)
    ASTNode analyzeCreateView(ASTNode ast, QB qb)
}
class CalcitePlanner

SemanticAnalyzerFactory ..> BaseSemanticAnalyzer
BaseSemanticAnalyzer <|-- ExplainSemanticAnalyzer
BaseSemanticAnalyzer <|-- LoadSemanticAnalyzer
BaseSemanticAnalyzer <|-- ExportSemanticAnalyzer
BaseSemanticAnalyzer <|-- ImportSemanticAnalyzer
BaseSemanticAnalyzer <|-- XxxSemanticAnalyzer
BaseSemanticAnalyzer <|-- SemanticAnalyzer
SemanticAnalyzer <|-- CalcitePlanner

@enduml
```

对于查询等语句，配置项`hive.cbo.enable`为`true`时使用`CalcitePlanner`类，否则使用`SemanticAnalyzer`类。

### QueryBlock

QueryBlock是一条SQL最基本的组成单元，包括三个部分：输入源、计算过程和输出，即一个QueryBlock就是一个子查询，由`QB`类实现。

### SemanticAnalyzer

hive编译主要逻辑位于`analyzerInternal()`方法中：

```Java
 void analyzeInternal(ASTNode ast, PlannerContext plannerCtx) throws SemanticException {
    // 1. 从语法树生成解析树
    LOG.info("Starting Semantic Analysis");
    genResolvedParseTree(ast, plannerCtx);

    // 2. 从解析树生成OP Tree
    Operator sinkOp = genOPTree(ast, plannerCtx);  // 内部调用genPlan()

    // 3. Deduce Resultset Schema

    // 4. Generate Parse Context for Optimizer & Physical compiler

    // 5. Take care of view creation

    // 6. Generate table access stats if required

    // 7. Perform Logical optimization

    // 8. Generate column access stats if required - wait until column pruning

    // 9. Optimize Physical op tree & Translate to target execution engine (MR, TEZ..)
    LOG.info("Completed plan generation");

    // 10. put accessed columns to readEntity

    // 11. if desired check we're not going over partition scan limits
  }
```

> 解析树（Parse Tree），也称为具体语法树（Concret Syntax Tree，CST），包含代码所有语法信息的树型结构。而抽象语法树（Abstract Syntax Tree，AST）忽略了一些解析树包含的语法信息，剥离掉一些不重要的细节。

#### genResolvedParseTree

```Java
boolean genResolvedParseTree(ASTNode ast, PlannerContext plannerCtx) throws SemanticException {
  ASTNode child = ast;
  this.ast = ast;
  viewsExpanded = new ArrayList<String>();
  ctesExpanded = new ArrayList<String>();

  // 1. 处理位置别名
  processPositionAlias(ast);

  // 2. 分析建表语句
  if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
    analyzeCreateTable(ast, qb, plannerCtx));
  }

  // 3. 分析创建视图语句
  if (ast.getToken().getType() == HiveParser.TOK_CREATEVIEW
      || (ast.getToken().getType() == HiveParser.TOK_ALTERVIEW && ast.getChild(1).getType() == HiveParser.TOK_QUERY)) {
    child = analyzeCreateView(ast, qb);
    viewSelect = child;
  }

  // 4. 接着分析子AST
  Phase1Ctx ctx_1 = initPhase1Ctx();
  doPhase1(child, qb, ctx_1, plannerCtx));
  LOG.info("Completed phase 1 of Semantic Analysis");

  // 5. Resolve Parse Tree
  getMetaData(qb);
  LOG.info("Completed getting MetaData in Semantic Analysis");

  plannerCtx.setParseTreeAttr(child, ctx_1);

  return true;
}
```

##### 处理位置别名

方法`processPositionAlias()`用于处理group by从句和order by从句中的位置别名。遍历AST，找出同层级的TOK\_SELECT结点和TOK\_GROUPBY（或TOK\_ORDERBY）结点，并将TOK\_GROUPBY（或TOK\_ORDERBY）结点下的位置别名进行替换，替换为TOK\_SELECT结点下与列标号对应的TOK\_SELEEXPR结点的子树。

```Java
private void processPositionAlias(ASTNode ast) throws SemanticException {
  // ...
  // 广度优先遍历AST
  Deque<ASTNode> stack = new ArrayDeque<ASTNode>();
  stack.push(ast);

  while (!stack.isEmpty()) {
    ASTNode next = stack.pop();

    if (next.getChildCount()  == 0) {
      continue;
    }

    boolean isAllCol;
    ASTNode selectNode = null;
    ASTNode groupbyNode = null;
    ASTNode orderbyNode = null;

    // 遍历当前结点子结点获取TOK_SELECT、TOK_GROUPBY、TOK_ORDERBY结点
    int child_count = next.getChildCount();
    for (int child_pos = 0; child_pos < child_count; ++child_pos) {
      ASTNode node = (ASTNode) next.getChild(child_pos);
      int type = node.getToken().getType();
      if (type == HiveParser.TOK_SELECT) {
        selectNode = node;
      } else if (type == HiveParser.TOK_GROUPBY) {
        groupbyNode = node;
      } else if (type == HiveParser.TOK_ORDERBY) {
        orderbyNode = node;
      }
    }

    if (selectNode != null) {
      int selectExpCnt = selectNode.getChildCount();

      // 替换group by中的列别名
      if (groupbyNode != null) {
        for (int child_pos = 0; child_pos < groupbyNode.getChildCount(); ++child_pos) {
          ASTNode node = (ASTNode) groupbyNode.getChild(child_pos);
          if (node.getToken().getType() == HiveParser.Number) {
            int pos = Integer.parseInt(node.getText());
            if (pos > 0 && pos <= selectExpCnt) {
              groupbyNode.setChild(child_pos, selectNode.getChild(pos - 1).getChild(0));
            }
          }
        }
      }

      // 替换order by中的列别名
      if (orderbyNode != null) {
        for (int child_pos = 0; child_pos < orderbyNode.getChildCount(); ++child_pos) {
          ASTNode colNode = (ASTNode) orderbyNode.getChild(child_pos);
          ASTNode node = (ASTNode) colNode.getChild(0);
          if (node.getToken().getType() == HiveParser.Number) {
            int pos = Integer.parseInt(node.getText());
            if (pos > 0 && pos <= selectExpCnt) {
              colNode.setChild(0, selectNode.getChild(pos - 1).getChild(0));
            }
          }
        }
      }
    }

    for (int i = next.getChildren().size() - 1; i >= 0; i--) {
      stack.push((ASTNode)next.getChildren().get(i));
    }
  }
}
```

##### 分析建表语句

方法`analyzeCreateTable()`用于分析建表语句。有三种创建表的情况：一般的create-table语句、create-table-like语句和create-table-as-select语句。对于一般的create-table或create-table-like语句，只影响了元数据，直接创建DDLWork处理，返回true并结束处理。对于create-table-as-select语句，获取SerDe和存储格式等信息并放入QB中，返回false并接着处理select语句。

```Java
ASTNode analyzeCreateTable(ASTNode ast, QB qb, PlannerContext plannerCtx) throws SemanticException {
  // analyzeCreateTable逐一处理TOK_CREATETABLE下面的子结点
  // 从第一个子结点获取表名称
  String[] qualifiedTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
  String dbDotTab = getDotName(qualifiedTabName);

  // 定义存储表属性的变量
  String likeTableName = null;
  List<FieldSchema> cols = new ArrayList<FieldSchema>();
  // ...
  final int CREATE_TABLE = 0; // regular CREATE TABLE
  final int CTLT = 1; // CREATE TABLE LIKE ... (CTLT)
  final int CTAS = 2; // CREATE TABLE AS SELECT ... (CTAS)
  int command_type = CREATE_TABLE;

  LOG.info("Creating table " + dbDotTab + " position=" + ast.getCharPositionInLine());
  int numCh = ast.getChildCount();

  /*
   * 进行简单的语义检查：
   * 1. CTLT和CTAS不能共存
   * 2. CTLT或CTAS不能包含列名
   * 3. CTAS不支持分区（目前）
   */
  for (int num = 1; num < numCh; num++) {
    ASTNode child = (ASTNode) ast.getChild(num);
    switch (child.getToken().getType()) {
    case HiveParser.TOK_IFNOTEXISTS:
      ifNotExists = true;
      break;
    // ...
    case HiveParser.TOK_LIKETABLE:
      if (child.getChildCount() > 0) {
        likeTableName = getUnescapedName((ASTNode) child.getChild(0));
        if (likeTableName != null) {
          if (command_type == CTAS) {
            // CTLT与CTAS不能共存
            throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE
                .getMsg());
          }
          if (cols.size() != 0) {
            // CTLT不能包含列名
            throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE
                .getMsg());
          }
        }
        command_type = CTLT;
      }
      break;
    case HiveParser.TOK_QUERY: // CTAS
      if (command_type == CTLT) {
        // CTLT与CTAS不能共存
        throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
      }
      if (cols.size() != 0) {
        // CTAS不能包含列名
        throw new SemanticException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
      }
      if (partCols.size() != 0 || bucketCols.size() != 0) {
        // CTAS不支持分区、桶
        throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
      }
      if (isExt) {
        // CTAS不支持外部表
        throw new SemanticException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
      }
      selectStmt = child;
      break;
    case HiveParser.TOK_TABCOLLIST:
    // ...
    default:
      throw new AssertionError("Unknown token: " + child.getToken());
    }
  }

  // ...

  // 处理不同的建表语句
  switch (command_type) {

  case CREATE_TABLE: // REGULAR CREATE TABLE DDL
    CreateTableDesc crtTblDesc = new CreateTableDesc(...);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), crtTblDesc), conf));
    break;

  case CTLT: // create table like <tbl_name>
    CreateTableLikeDesc crtTblLikeDesc = new CreateTableLikeDesc(...);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        crtTblLikeDesc), conf));
    break;

  case CTAS: // create table as select
    tableDesc = new CreateTableDesc(...);
    qb.setTableDesc(tableDesc);
    return selectStmt;
  default:
    throw new SemanticException("Unrecognized command.");
  }
  return null;
}
```

##### 分析创建视图语句

方法`analyzeCreateView()`用于分析创建视图语句，得到各列信息，创建视图描述，然后由DDLWork处理，返回TOK_QUERY结点：

```Java
private ASTNode analyzeCreateView(ASTNode ast, QB qb) throws SemanticException {
  String[] qualTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
  String dbDotTable = getDotName(qualTabName);
  List<FieldSchema> cols = null;
  boolean ifNotExists = false;
  // ... 

  LOG.info("Creating view " + dbDotTable + " position=" + ast.getCharPositionInLine());
  int numCh = ast.getChildCount();
  // 逐一处理TOK_CREATEVIEW或TOK_ALTERVIEW结点的子结点，填充相应信息
  for (int num = 1; num < numCh; num++) {
    ASTNode child = (ASTNode) ast.getChild(num);
    switch (child.getToken().getType()) {
    case HiveParser.TOK_IFNOTEXISTS:
      ifNotExists = true;
      break;
    // ...
    default:
      assert false;
    }
  }

  createVwDesc = new CreateViewDesc(dbDotTable, cols, comment, tblProps, partColNames,
    ifNotExists, orReplace, isAlterViewAs);
  rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), createVwDesc), conf));

  return selectStmt;
}
```

##### doPhase1

`doPhase1()`采用先序遍历的方式递归遍历AST，并将其转换为QB：

+ 获取所有表/子查询别名，并存入`aliasToTabs`、`aliasToSubq`
+ 获取目标位置，并将从句命名为inclausei
+ 创建从聚合树的字符串表示到实际聚合操作AST的映射
+ 在destToSelExpr中创建从从句名称到select表达式AST的映射
+ 在aliasToLateralViews中创建从表别名到lateral view AST的映射

```Java
public boolean doPhase1(ASTNode ast, QB qb, Phase1Ctx ctx_1, PlannerContext plannerCtx) throws SemanticException {

  boolean phase1Result = true;
  QBParseInfo qbp = qb.getParseInfo();
  boolean skipRecursion = false;

  if (ast.getToken() != null) {
    skipRecursion = true;
    switch (ast.getToken().getType()) {
    case HiveParser.TOK_SELECTDI:
    // ...
    default:
      skipRecursion = false;
      break;
    }
  }

  if (!skipRecursion) {
    // Iterate over the rest of the children
    int child_count = ast.getChildCount();
    for (int child_pos = 0; child_pos < child_count && phase1Result; ++child_pos) {
      // Recurse
      phase1Result = phase1Result && doPhase1(
          (ASTNode)ast.getChild(child_pos), qb, ctx_1, plannerCtx);
    }
  }
  return phase1Result;
}
```

###### TOK\_SELECTDI/TOK\_SELECT

###### TOK\_WHERE

##### getMetaData

`getMetaData()`把表、字段等元数据信息存入QB

#### genPlan



### CalcitePlanner

## 生成逻辑计划

## 逻辑优化

## 生成物理计划

## 物理优化
