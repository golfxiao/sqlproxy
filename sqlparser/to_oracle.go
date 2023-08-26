package sqlparser

import (
	"fmt"
	"log"
	"sqlproxy/core/golog"
	"strconv"
	"strings"
)

type OracleConverter struct {
	replaceChars      map[string]string
	tableUniqueIndexs map[string]map[string][]string
	tableColumns      map[string][]string
	incrementColumns  map[string]map[string]int
}

func NewOracleConverter(tableUniqueIndexs map[string]map[string][]string, tableColumns map[string][]string, incrementColumns map[string]map[string]int) *OracleConverter {
	return &OracleConverter{
		replaceChars: map[string]string{
			"`":                   "\"",
			"0000-00-00 00:00:00": "0001-01-01 00:00:00",
			`\'`:                  `''`,
		},
		tableUniqueIndexs: tableUniqueIndexs,
		incrementColumns:  incrementColumns,
		tableColumns:      tableColumns,
	}
}

// convert sql from mysql to oracle
// 1. parse sql to ast
// 2. check if need to convert
// 3. convert mysql ast to oracle ast
// 4. rebuild oracle sql from ast
func (this *OracleConverter) Convert(sql string, args ...interface{}) (string, []interface{}, error) {
	// TODO only support insert and replace statement?
	// ` -> " , group_concat -> wm_concat
	if !supportConvert(sql) {
		return sql, args, nil
	}
	stmt, err := Parse(sql)
	if err != nil {
		log.Printf("ignoring error parsing sql '%s': %v", sql, err)
		return "", args, err
	}

	oracleStmt, args := this.convertStmtArgs(this.convertStmt(stmt), args...)

	if oracleStmt == nil {
		return this.replaceCommonIdents(sql), args, nil
	}
	buf := NewTrackedBuffer(nil).WriteNode(oracleStmt)
	convertSQL := this.replaceCommonIdents(buf.String())
	golog.Debug("OracleConverter", "Convert", "ConvertSQL", 0, convertSQL)
	return convertSQL, args, nil
}

func (this *OracleConverter) convertStmt(stmt Statement) Statement {
	switch stmt.(type) {
	case *Insert:
		return this.convertInsert(stmt.(*Insert))
	case *Select:
		return this.convertSelect(stmt.(*Select))
	default:
		return stmt
	}
}

func (this *OracleConverter) convertSelect(stmt *Select) Statement {
	for _, expr := range stmt.From {
		if t, ok := expr.(*AliasedTableExpr); ok {
			if t.Hints != nil && t.Hints.Type == "force " {
				t.Hints = nil
			}
		}
	}
	return stmt
}
func (this *OracleConverter) convertStmtArgs(stmt Statement, args ...interface{}) (Statement, []interface{}) {
	if len(args) == 0 {
		return stmt, args
	}
	switch stmt.(type) {
	case *Merge:
		return this.convertMerge(stmt.(*Merge), args...)
	}
	return stmt, args
}

func (this *OracleConverter) convertMerge(stmt *Merge, args ...interface{}) (Statement, []interface{}) {

	newArgs := []interface{}{}
	id := 1
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case *SQLVal:
			n := node.(*SQLVal)
			v := string(n.Val)
			i, _ := strconv.Atoi(strings.ReplaceAll(v, ":v", ""))
			n.Val = []byte(fmt.Sprintf(":v%d", id))
			newArgs = append(newArgs, args[i-1])
			id++
			return true, nil
		default:
			return true, nil
		}
	}
	_ = Walk(visit, stmt)
	return stmt, newArgs
}

func (this *OracleConverter) convertInsert(stmt *Insert) Statement {
	//检查有无自增字段
	stmt = this.convertInsertIncrement(stmt)
	// write a method to walk ast tree, recognize all kinds of expr, and rebuild oracle ast
	if stmt.Action == InsertStr && stmt.OnDup == nil {
		return stmt
	}
	// The case where columns is empty is not currently supported for conversion.
	if len(stmt.Columns) == 0 {
		return stmt
	}

	condcols := this.getUniqueConditionColumns(stmt)
	if len(condcols) == 0 {
		stmt.OnDup = nil
		return stmt
	}
	tableExpr := this.buildMergeTableExpr(stmt, condcols)
	matchedExpr := buildMatchedExpr(stmt, condcols)

	if tableExpr == nil || len(matchedExpr) == 0 {
		return stmt
	}

	// sets the qualifier for columns
	setQualifierForCols(tableExpr)
	setQualifierForCols(matchedExpr)
	log.Printf("condcols: %v, tableExpr: %v, matchedExpr: %v", condcols, tableExpr, matchedExpr)

	return &Merge{
		Comments: stmt.Comments,
		Table:    tableExpr,
		Matched:  matchedExpr,
		Unmatched: &UnmatchedExpr{
			Columns: stmt.Columns,
			Rows:    stmt.Rows,
		},
	}
}

func (this *OracleConverter) convertInsertIncrement(stmt *Insert) *Insert {
	if _, ok := stmt.Rows.(Values); !ok {
		return stmt
	}

	incrementColumns := this.incrementColumns[stmt.Table.Name.String()]
	if incrementColumns == nil || len(incrementColumns) == 0 {
		return stmt
	}

	if len(stmt.Columns) == 0 {
		stmt.Columns = []ColIdent{}
		for _, column := range this.tableColumns[stmt.Table.Name.String()] {
			stmt.Columns = append(stmt.Columns, NewColIdent(column))
		}
	}

	ns := []int{}
	newColumns := []ColIdent{}
	for i, column := range stmt.Columns {
		if _, ok := incrementColumns[column.String()]; ok {
			ns = append(ns, i)
		} else {
			newColumns = append(newColumns, column)
		}
		stmt.Columns = newColumns
	}

	var rows Values
	for _, row := range stmt.Rows.(Values) {
		newRow := ValTuple{}
		for i, v := range row {
			if len(ns) == 0 {
				newRow = append(newRow, v)
			} else {
				for n := range ns {
					if i != n {
						newRow = append(newRow, v)
					}
				}
			}
		}
		rows = append(rows, newRow)
	}
	stmt.Rows = rows
	return stmt
}

func (this *OracleConverter) buildMergeTableExpr(stmt *Insert, condcols [][]string) *MergeTableExpr {
	onCondition := buildJoinConditions(stmt, condcols)
	if onCondition == nil {
		return nil
	}

	return &MergeTableExpr{
		LeftExpr: &AliasedTableExpr{
			Expr: stmt.Table,
			As:   NewTableIdent("t"),
		},
		RightExpr: &AliasedTableExpr{
			Expr: TableName{
				Name: NewTableIdent("dual"),
			},
		},
		Condition: JoinCondition{
			On: onCondition,
		},
	}
}

func (this *OracleConverter) getUniqueConditionColumns(stmt *Insert) [][]string {
	condcols := [][]string{}
	// Case1: If user has configured unique index condcols for the table, use it as condition condcols
	tableIndexs := this.tableUniqueIndexs[stmt.Table.Name.String()]
	for _, iii := range tableIndexs {
		i := 0
		for _, column := range stmt.Columns {
			for _, v := range iii {
				if v == column.String() {
					i++
				}
			}
		}
		if i == len(iii) {
			condcols = append(condcols, iii)
		}
	}

	return condcols
}

func buildMatchedExpr(stmt *Insert, condcols [][]string) MatchedExpr {
	if stmt.OnDup != nil {
		return MatchedExpr(stmt.OnDup)
	}
	vals := getInsertValues(stmt)
	exprs := make([]*UpdateExpr, 0, len(vals))

	allCondcol := []string{}
	for _, condcol := range condcols {
		allCondcol = append(allCondcol, condcol...)
	}
	for i, column := range stmt.Columns {

		if !StringIn(column.String(), allCondcol...) {
			exprs = append(exprs, &UpdateExpr{
				Name: &ColName{Name: column},
				Expr: vals[i],
			})
		}
	}
	return exprs
}

func getInsertValues(stmt *Insert) []*SQLVal {
	vals := make([]*SQLVal, 0, len(stmt.Columns))
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case *SQLVal:
			vals = append(vals, node.(*SQLVal))
			return true, nil
		default:
			return true, nil
		}
	}
	Walk(visit, stmt.Rows)
	return vals
}

// sets the qualifier for columns in the SQLNode.
//
// It takes a SQLNode as a parameter and sets the qualifier of any ColName
// nodes to TableName "t".
func setQualifierForCols(node SQLNode) SQLNode {
	visit := func(node SQLNode) (kcontinue bool, err error) {
		switch node.(type) {
		case *ColName:
			node.(*ColName).Qualifier = TableName{
				Name: NewTableIdent("t"),
			}
			//log.Printf("set qualifier for column: %v", node.(*ColName).Name)
			return true, nil
		default:
			return true, nil
		}
	}
	Walk(visit, node)
	return node
}

func buildJoinConditions(stmt *Insert, condcols [][]string) Expr {
	vals := getInsertValues(stmt)

	exprs := make([][]*ComparisonExpr, 0, len(condcols))
	for _, condcol := range condcols {
		expr := make([]*ComparisonExpr, 0, len(condcol))
		for i, column := range stmt.Columns {
			if StringIn(column.String(), condcol...) {
				expr = append(expr, &ComparisonExpr{
					Operator: EqualStr,
					Left:     &ColName{Name: column},
					Right:    &SQLVal{vals[i].Type, vals[i].Val},
				})
			}
		}
		exprs = append(exprs, expr)
	}

	if len(exprs) == 0 {
		return nil
	}
	var conditions Expr
	for i, expr := range exprs {
		var condition Expr
		for j, comparisonExpr := range expr {
			if j == 0 {
				condition = comparisonExpr
			} else {
				condition = &AndExpr{condition, comparisonExpr}
			}
		}
		if i == 0 {
			conditions = condition
		} else {
			conditions = &OrExpr{conditions, condition}
		}
	}

	return conditions
}

func (this *OracleConverter) replaceCommonIdents(sql string) string {
	for old, new := range this.replaceChars {
		sql = strings.Replace(sql, old, new, -1)
	}
	return sql
}

func supportConvert(sql string) bool {
	switch Preview(sql) {
	case StmtInsert, StmtReplace, StmtDelete, StmtSelect, StmtUpdate:
		return true
	default:
		return false
	}
}
