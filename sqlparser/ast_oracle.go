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
	Rows    InsertRows
}

// Format formats the node.
func (node *UnmatchedExpr) Format(buf *TrackedBuffer) {
	buf.Myprintf("when not matched then insert %v %v",
		node.Columns, node.Rows)
}

func (node *UnmatchedExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Columns,
		node.Rows,
	)
}

// MergeTableExpr represents a TableExpr that's a JOIN operation.
type MergeTableExpr struct {
	LeftExpr  TableExpr     // AliasedTableExpr
	RightExpr TableExpr     // AliasedTableExpr
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
