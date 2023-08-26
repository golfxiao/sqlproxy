package server

import (
	"fmt"
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
	}
	return fmt.Errorf("statement %T not support now", stmt)
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
