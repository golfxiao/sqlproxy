package sqlparser

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
		node.Comments,
		node.Table, node.Matched, node.Unmatched)
}

func (node *Merge) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Comments,
		node.Table,
		node.Matched,
		node.Unmatched,
	)
}

type MatchedExpr UpdateExprs

// Format formats the node.
func (node MatchedExpr) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	buf.Myprintf("when matched then update set %v", UpdateExprs(node))
}

func (node MatchedExpr) walkSubtree(visit Visit) error {
	return Walk(visit, UpdateExprs(node))
}

type UnmatchedExpr struct {
	Columns Columns
	Values  ValuesExpr
}

// Format formats the node.
func (node *UnmatchedExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("when not matched then insert %v %v",
		node.Columns, node.Values)
}

func (node *UnmatchedExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Columns,
		node.Values,
	)
}

type ValuesExpr []*ColName

// Format formats the node.
func (node ValuesExpr) Format(buf *TrackedBuffer) {
	if node == nil {
		return
	}
	prefix := "values ("
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = ", "
	}
	buf.WriteString(")")
}

func (node ValuesExpr) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// FindColumn finds a column in the column list, returning
// the index if it exists or -1 otherwise
func (node ValuesExpr) FindColumn(col ColIdent) int {
	for i, colName := range node {
		if colName.Name.Equal(col) {
			return i
		}
	}
	return -1
}

// MergeTableExpr represents a TableExpr that's a JOIN operation.
type MergeTableExpr struct {
	LeftExpr  TableExpr     // AliasedTableExpr
	RightExpr TableExpr     // VirtualTableExpr
	Condition JoinCondition //
}

func (node *MergeTableExpr) iTableExpr() {}

// Format formats the node.
func (node *MergeTableExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("%v using %v%v", node.LeftExpr, node.RightExpr, node.Condition)
}

func (node *MergeTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.LeftExpr,
		node.RightExpr,
		node.Condition,
	)
}

// using语句拼接的虚拟表定义
type VirtualTableExpr struct {
	Rows      SelectValues // 虚拟表数据，来自于Insert.Rows
	TableName TableIdent   // 表名
	Columns   Columns      // 虚拟表数据声明的列
}

func (node *VirtualTableExpr) iTableExpr() {}

// Format formats the node.
func (node *VirtualTableExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("(%v) %v %v", node.Rows, node.TableName, node.Columns)
}

func (node *VirtualTableExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Rows,
		node.TableName,
		node.Columns,
	)
}

type SelectValues []SelectTuple

func (node SelectValues) Format(buf *TrackedBuffer) {
	prefix := ""
	for _, n := range node {
		buf.Myprintf("%s%v", prefix, n)
		prefix = " union all "
	}
}

func (node SelectValues) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

type SelectTuple Exprs

// Format formats the node.
func (node SelectTuple) Format(buf *TrackedBuffer) {
	buf.Myprintf("select %v", Exprs(node))
}

func (node SelectTuple) walkSubtree(visit Visit) error {
	return Walk(visit, Exprs(node))
}
