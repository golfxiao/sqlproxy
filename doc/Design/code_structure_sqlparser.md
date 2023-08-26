<i>声明：本文描述的代码和对象结构只是个人的粗浅理解，贴出来帮助后续同事阅读代码，可能会有理解不准确或偏差的地方，如有发现还望随时帮忙补充和修正。</i>

# 代码结构

## parsed_query.go: 
- ParsedQuery: 对SQL语句中占位符位置的解析，包括占位符的起始下标和长度。
- GenerateQuery： 根据传入的bindVars对SQL中的占位符作填充，返回填充后的SQL语句
- normalize.go:
- Normallize: 将SQL语句标准化成占位符模式，凡是数据都用占位符替代，并将数据组织到一个以占位符为key，以值为value的map中； 
- GetBindvars： 获取SQL语句中的占位符，以map形式返回，value是空
- RedactSQLQuery: 返回标准化后的SQL语句
## impossible_query.go:
* FormatImpossibleQuery：不是用于查询数据，而是当没有返回任何数据的时候能返回字段信息，字段信息会作缓存
## analyzer.go:
* Preview: 通过SQL语句的起始关键词来快速识别一个SQL语句是什么类型
* StmtType： 根据Statement整数类型返回字符串类型
* isDML: 判断是否为增删改查语句
* TableName: 获取sql语句的table name
* isValue: 判断是否为值节点
* ExtractSetValues： 如果是set类型的语句，则解析出要Set的字段和值到key-value映射中
## encodable.go: 
* Encodable: 可被编码进SQL中的类型或接口
* InsertValues: 是Insert语句的数据编码器，将多条数据编码成批量插入的SQL
## sql.go: 
使用yacc作语法树解析
## token.go: 
* Tokenizer: 一个标识符或一个值是一个token，语法解析按token关键字进行识别
* Scan: 读取一个token文本串，并对token作规范化处理，例如：去掉无关语义的引号、分号
此方法会逐字符分析
    * scanIdentifier: 读取标识符，将开始字符是字母/@/_的后续字母/数字一次读完作为标识符
    * scanNumber： 读取一串数字
    * scanBindVar:  读取一个占位符
    * consumeNext： 写入当前字符到buffer，读取下一个字符
    * scanLiteralIdentifier: 读取``包围的保留字标识符
    * lastChar: 前一个字符，有些地方需要拿前后字符一起判断，例如：/* 、 <=
* Lex: 读取下一个Token， 给yacc使用
## ast.go: 
* SplitStatement：解析出第一条SQL以及剩余的部分（多条SQL Case）
* SplitStatementToPieces: 将；分隔的一组SQL语句拆分为sql数组
    * 最后一条SQL后面的分号去掉
    * 分号可能在注释中，所以不能用简单的split函数作分割
* Parse： 将一条SQL语句解析成一个语法树，使用了yy方法
* Visit: 语法树中节点遍历的方法签名，不同的语句都有不同的visit方法实现； 
* Append(buf, node): 调用一个节点的format方法将节点的SQL片段追加到buf中
* ReplaceExpr(root, from, to Expr)： 从指定 的root节点查找from，查找到后替换为to
* ExprFromValue： 把具体的值转换为expression: SQLVal
## tracked_buffer.go: 
* TrackedBuffer: 用于从Ast树中重建SQL语句
    * MyPrintf: 对格式化SQL语句的封装，
        * %c: 单字符或字节, 使用WriteByte或WriteRune
        * %s: 字符串或字节数组, 使用WriteString或Write(bytes)
        * %v: sqlNode:调用对应的format函数
        * %a：格式化参数


# 对象结构
## SQLNode
SQL语法树中的一个节点，Statement, Expr、SqlVal等都是一个SQLNode节点。 

节点可能有子节点，一层层节点组成了节点树，每个节点都有两个方法：
* Format: 将此节点的sql片段重新拼装成SQL语句，每个节点都有自己的format实现
* walkSubTree(visit): 所有访问者模式来遍历子树上的所有节点及子节点

在一个父节点上调用Format，它会递归调用所有子节点的Format，当每个子节点的SQL片段拼好后，整条SQL就拼出来了。 

## Statement
代表一条基本的SQL语句，类别包括：
* Select: 所有Select语句需要实现的方法都由SelectStatement 定义
* AddOrder： 添加Order by片段
* AddWhere: 添加where片段
* Addhaving：添加having片段
* SetLimit: 设置Liimit片段
* Union
* left/Right: SelectStatement
* Insert: 包括ignore, replace
* InsertRows: 要插入的行数据
* Update
* Delete
* Stream
* Set: 表示Set语句，是指Mysql环境变量的设置语句，不是Update时的Set
* DBDDL: 数据库层的DDL语句，例如Create database xxx
* DDL: 对表的操作语句，像Alter table， 也包括给表添加索引
* Show: Mysql 的show命令
* Use： use databse
* Begin: 事务开始
* Commit：事务结束
* Rollback：事务回滚
* OtherRead: Describe或explain之类语句的指示器，只是一个指示器，不包含AST语法树
* OtherAdmin: 需要管理员权限的语句，它也只是一个指示器
* ParentSelect ： 括号里的Select语句
## DDL
归类DDL语句，对象结构释义: 
* PartitioinSpec: 对分区的创建和修改
    * PartitionDefinition 分区定义
* TableSpec: 表结构和索引信息描述（来自于建表语句）
    * ColumnDefinition： 一个列的信息
        * ColIdent: 列的名称信息
        * ColumnType: 列的类型和可选项信息，像not null\auto increment\comment
    * IndexDefinition： 建表语句中一个索引的信息
        * IndexInfo：索引名称以及主键、唯一标志
        * IndexColumn：组成索引的列，可能包含多个
        * IndexOption：索引选项，可能是多个
* VIndexSpec: 对索引的增删改查语句
## Expr
表示一个语法片段，是构成语法树的最基本单元。
* SelectExpr：Select表达式
* TableExpr： Table片段，例如：From语句里每个表是一个tableExpr 
    * AliasedTableExpr， 包含可选别名和index hints的table片段
        * RemoveHints：去掉指明的索引信息
    * ParenTableExpr
    * JoinTableExpr
        * Join: Join的表类型，如： Left Join, Right Join Inner join
        * LeftExpr： 左表信息
        * RightExpr: 右表信息
        * JoinCondition：连表信息
* GroupBy： group by 语句
* OrderBy: order by 语句
* Values: Values语句
* Where: where 或 having 表达式
* UpdateExprs： Update语句，包括Update的列-值信息
* OnDup： On duplicate key update语句
* StarExpr：Select中的*
* AliasedExpr： As语句
* ColIdent：大小写不敏感的SQL标识符, 
* FormatID: 需要对标识符作反引号处理
* Columns: 列名信息列表， ColIdent组成的数组
* FindColumn： 找到对应的Colident组成的数组
* GroupConcatExpr： GroupConcat
* AndExpr: And逻辑运算符，有left和right两个需要进行And操作的Expr
* OrExpr： Or逻辑运算符，有Left和right两个需要进行Or操作的expr
* NotExpr: 对一个expr进行Not操作的运算符，
* ParenExpr： 用括号包着的一个返回bool值的条件表达式
* ComparisonExpr： 两个需要进行比较操作的表达式，例如：< > =
* RangeCond: Between…and…表达式
* IsExpr： Is开头的表达式
* ExistsExpr： Exists表达式
* FuncExpr：函数调用表达式
* BinaryExpr: 二进制运算表达式
* UnaryExpr: 一元运算表达式
* ValuesFuncExpr: ？
* SubQuery： 子查询表达式，其实是Select语句外面拼了一组括号
* MatchExpr： match against语句
* CaseExpr： Case when then else  end语句
* SubstrExpr： substr表达式
* ConvertExpr: cast类型强转表达式
* SQLVal: 表示值对象，可以是具体的值，也可以是占位符
* NullVal：Null值对象
* BoolVal： Bool类型的值对象
* ValTuple: Insert  values中的一组值对象，表示一条记录
* ColName: 一个列的名称

## 其它

* TableName: 表名
    * FormatID：可能需要对标识符作反引号处理
* Comments: 注释
* Aggregates: 数组，所有的聚合类算子