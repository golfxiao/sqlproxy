package server

import (
	"sqlproxy/core/golog"
	"sqlproxy/mysql"
	"sqlproxy/sqlparser"
	"strings"
)

func (c *ClientConn) handleShow(stmt *sqlparser.Show, sql string, args []interface{}) error {

	switch strings.ToLower(stmt.Type) {
	case "variables":
		return c.ShowVariables()
	case "collation":
		return c.ShowCollation()
	case "warnings":
		return c.ShowEmptyResultset()
	default:
		// 将不支持的show命令统一返回空结果集，以规避java orm中出现的show 命令报错问题
		golog.Warn("ClientConn", "handleShow", "return empty resultset for unsupported type", c.connectionId, "show_type", stmt.Type)
		return c.ShowEmptyResultset()
	}
	// return fmt.Errorf("statement %T not support now", stmt)
}

func (c *ClientConn) ShowCollation() error {

	rowData := [][]interface{}{}

	for _, item := range mysql.GlobalCollation {
		rowData = append(rowData, []interface{}{
			item["Collation"],
			item["Charset"],
			item["Id"],
			item["Default"],
			item["Compiled"],
			item["Sortlen"],
		})
	}

	rs, _ := c.buildResultset(nil, []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}, rowData)
	status := c.status | 0
	return c.writeResultset(status, rs)

}

func (c *ClientConn) ShowVariables() error {
	rowData := [][]interface{}{}

	for k, v := range mysql.GlobalVariable {
		rowData = append(rowData, []interface{}{
			k,
			v,
		})
	}
	rs, _ := c.buildResultset(nil, []string{"Variable_name", "Value"}, rowData)
	status := c.status | 0
	return c.writeResultset(status, rs)
}

func (c *ClientConn) ShowEmptyResultset() error {
	rs := c.newEmptyResultsetForColumns([]string{"Level", "Code", "Message"})
	status := c.status | 0
	return c.writeResultset(status, rs)
}
