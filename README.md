# sqlproxy简介

sqlproxy是一个基于kingshard二次开发的用于信创数据库对接的中间件项目，致力于简化异构数据库的对接工作，目标是让现有基于MySQL的项目无缝迁移到其它类SQL型数据库，例如：Oracle、达梦。

sqlproxy的主要工作是在中间件上对不同DB的语法和协议作转换，以便现有业务项目不改动或尽量少改动，使用原来的MySQL语法和驱动就能操作另一个信创数据库。

## 1. 使用说明

将etc目录下的[etc/sqlproxy_example.yaml](./etc/sqlproxy_example.yaml)改名为sqlproxy.yaml，基于此示例文件来修改自己的配置，需要配的内容下面介绍。

### 1.1 添加后端数据库配置
配置文件sqlproxy.yaml的`nodes`节点用来配置后端DB，支持配置多个后端DB，每个DB支持配置名称、驱动、连接串、连接数，如下：
```
nodes:
  - # db alias name, used to specify db name for `use DB` command and the range of db that users can access.
    name: demodb
    # db driver name
    driver_name: dm

    # default max conns for connection pool
    max_conns_limit: 32

    # the db connection string. 
	# In the context of an Oracle database, the user needs to bind a user to a specific tablespace.
    datasource: dm://demouser:demopwd@192.168.23.216:5236
```

### 1.2 添加用户
配置文件sqlproxy.yaml中的`user_list`节点用来配置允许连接到sqlproxy的用户列表，包括用户名和密码，如下：
```
user_list:
  - user: testuser1
    password: testpwd1
  - user: testuser2
    password: testpwd2
```

### 1.3 配置用户能访问的数据库范围
配置文件sqlproxy.yaml中的`schema_list`节点用来配置每个用户能访问的后端DB范围，可以是多个，如果一个用户没有配置可访问DB范围，则默认为所有DB均可访问。如下：

```
schema_list:
  - user: testuser1
    nodes: [ demodb ]
  - user: testuser2
    nodes: [ demodb ]
```

### 1.4 编译运行
编译：进入sqlproxy项目根目录，运行go build得到程序的可执行文件。
```
go build 
```
运行如下命令启动服务：
```
./sqlproxy -config ./etc/sqlproxy.yaml &
```

### 1.5 应用连接中间件
假如一个应用服务A原来使用的是MySQL数据库，现在需要对接基于oracle语法的达梦数据库，理论上代码层不用作大的改动，只将DB连接串由原来指向MySQL改为指向此sqlproxy中间件：
```
# 原始连接串，假如原先连接的MySQL服务器为192.168.23.215:3306
demouser:demopwd@tcp(192.168.23.215:3306)/pc3?timeout=1000ms&readTimeout=1000ms&writeTimeout=5000ms&charset=utf8

# 对接信创后连接串,假设信创中间件地址为：192.168.23.217:9696, testuser1为中间件上为应用配置的用户名
testuser1:testpwd1@tcp(192.168.23.217:9696)/pc3?timeout=1000ms&readTimeout=1000ms&writeTimeout=5000ms&charset=utf8
```

中间件已经针对达梦数据库做了一部分已知的对接工作，例如：
- replace into语句转换为merge into； 
- on duplidate key update 语句转换为merge into语句； 
- 不兼容的反引号`替换为达梦中支持的双引号"； 
- 不兼容的MySQL转义方式`\'`（斜杠转义）替换为达梦中的转义方式`''`(引号转义)； 
- 达梦驱动中的长文本字段类型DMClob自动转换为通用的string；
- 不兼容的MySQL时间戳零值`0000-00-00 00:00:00`替换为达梦中的`0001-01-01 00:00:00`； 
- 达梦驱动读出的时间戳格式为`2006-01-02T15:04:05.999999999Z07:00`,中间件会根据DB字段定义转换为应用需要的格式； 
- 去掉达梦中不支持的`force index`语法； 
- 去掉Insert语句中达梦不支持的自增列； 

除这些外，可能还会有其它不兼容的语法，可以选择在中间件上做二次开发。

## 2. 二次开发

本项目目前主要是针对达梦数据库作了支持，支持将mysql中的`on duplicate key update`语句转换成达梦中的`merge into`语句，下面就以此为例介绍如何作新数据库以及新语法的扩展。


### 2.1 扩展语法树

关于语法树的定义，有几个接口是我们需要遵循的：
- SQLNode: 表示语法树上的一个节点，不论是一条完整的语句，还是一个数据值或一个列名称都是一个节点； 
- Statement: 表示一条完整的SQL语句，例如`insert into person(id, name) values(1, 'zhangsan')`； 
- Expr: 表示一条语法片段，像And、Or、Exists等都是一个语法片段； 
```
// SQLNode defines the interface for all nodes
// generated by the parser.
type SQLNode interface {
	Format(buf *TrackedBuffer)
	// walkSubtree calls visit on all underlying nodes
	// of the subtree, but not the current one. Walking
	// must be interrupted if visit returns an error.
	walkSubtree(visit Visit) error
}

// Statement represents a statement.
type Statement interface {
	iStatement()
	SQLNode
}

// Expr represents an expression.
type Expr interface {
	iExpr()
	// replace replaces any subexpression that matches
	// from with to. The implementation can use the
	// replaceExprs convenience function.
	replace(from, to Expr) bool
	SQLNode
}
```
关于语法树定义的更详细代码解读，参考：[语法树结构](./doc/Design/code_structure_sqlparser.md)

[sqlparser/ast.go](./sqlparser/ast.go)是一棵MySQL语法树，里面已经支持了MySQL以及标准SQL的主流语法，我们需要做的工作就是扩展这棵语法树，以支持新的语法。
merge into是一个多表数据合并语句，基于联表来实现，语句示例：
```
MERGE INTO demo_table t
USING dual
ON (t.column1 = 634311 AND t.column2 = 131722)
WHEN MATCHED THEN
    UPDATE SET t.column3 = '', t.column4 = 'this is demo column description'
WHEN NOT MATCHED THEN
    INSERT (column1, column2, column3, column4)
    VALUES (634311, 131722, '', 'this a demo column description');
```
这条语句大致可以分为三部分：
- Table： 联表语句及条件
- Matched: 当条件匹配时要执行的操作
- Unmatched: 当条件不匹配时要执行的操作

```
type Merge struct {
	Comments  Comments
	Table     *MergeTableExpr
	Matched   MatchedExpr
	Unmatched *UnmatchedExpr
}

func (node *Merge) iStatement() {}

// Format formats the node.
func (node *Merge) Format(buf *TrackedBuffer) {
	buf.Myprintf("merge %vinto %v %v %v",
		……
  )
}

func (node *Merge) walkSubtree(visit Visit) error {
	……
}
```
上面用到的MergeTableExpr、MatchedExpr、UnmatchedExpr都需要按照`merge into`语法来给出具体的结构定义，有兴趣可以查看[sqlparser/ast_oracle.go](./sqlparser/ast_oracle.go)，这里不再展开描述。

### 2.2 扩展语法转换器
为目标语法定义出语法对象结构后，我们需要写一个转换器，来将原来的MySQL语法转换成目标数据库的语法，语法转换器需要遵循下面的接口定义：
```
// 对于预编译的SQL，如果转换后的占位参数也有变化，则转换器需要将参数也修改并返回
type SQLConverter interface {
	Convert(sql string, args ...interface{}) (string, []interface{}, error)
}
```
项目已经支持了`mysql-to-oracle`语法转换器（见[sqlparser/to_oracle.go](./sqlparser/to_oracle.go)），也可以实现其它数据库的语法转换器，新的语法转换器需要在[sqlparser/convert.go](./sqlparser/convert.go)中构造实例对象，并通过`GetSQLConverter`方法统一对外提供访问，如下：
```
func GetSQLConverter(name string, tableIndexs map[string]map[string][]string) SQLConverter {
	switch name {
	case MYSQL_TO_ORACLE:
		return NewOracleConverter(tableIndexs)
	default:
		return nil
	}
}
```

### 2.3 引入驱动
我们使用Go官方标准的database/sql接口来访问目标数据库，理论上添加新的SQL数据库天然就能支持，只需要引入对应的数据库驱动，如下：
```
import _ "github.com/golfxiao/dm"
```

## 3. 设计原理

请参考：[sqlproxy设计过程](./doc/Design/architecture.md)

## 4. 反馈
感谢您阅读这个教程，如果您觉得对您有所帮助，可以考虑请我喝杯咖啡作为鼓励😊

![a cup of tea](./doc/Design/resources/cup_of_tea.jpg)

如果您在使用sqlproxy的过程中发现BUG或者有新的功能需求，请发邮件至golfxiao@163.com与作者取得联系，或者添加作者微信：

<img src="./doc/Design/resources/weixin_pic.jpg" width="20%" height="20%"/>