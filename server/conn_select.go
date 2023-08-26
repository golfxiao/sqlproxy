// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"sqlproxy/core/golog"
	"sqlproxy/mysql"
	"sqlproxy/sqlparser"
	"strings"
)

const (
	MasterComment    = "/*master*/"
	SumFunc          = "sum"
	CountFunc        = "count"
	MaxFunc          = "max"
	MinFunc          = "min"
	LastInsertIdFunc = "last_insert_id"
	FUNC_EXIST       = 1
)

var funcNameMap = map[string]int{
	"sum":            FUNC_EXIST,
	"count":          FUNC_EXIST,
	"max":            FUNC_EXIST,
	"min":            FUNC_EXIST,
	"last_insert_id": FUNC_EXIST,
}

func (c *ClientConn) handleUnion(stmt *sqlparser.Union, sql string, args []interface{}) error {

	backend := c.GetBackendDB()
	if backend == nil {
		golog.Fatal("ClientConn", "handleSelect", "backend is nil", c.connectionId, "db", c.db)
		r := c.newEmptyResultset(stmt.Left.(*sqlparser.Select))
		return c.writeResultset(c.status, r)
	}
	rs, err := backend.Query(sql, args...)
	if err != nil {
		golog.Error("ClientConn", "handleSelect", err.Error(), c.connectionId)
		return err
	}
	status := c.status | rs.Status
	if rs.Resultset != nil {
		err = c.writeResultset(status, rs.Resultset)
	} else {
		r := c.newEmptyResultset(stmt.Left.(*sqlparser.Select))
		err = c.writeResultset(status, r)
	}

	return err
}

// 处理select语句
func (c *ClientConn) handleSelect(stmt *sqlparser.Select, sql string, args []interface{}) error {
	if len(stmt.From) == 1 && sqlparser.IsDualTable(stmt.From[0]) && strings.Contains(sql, "@") { //查询环境变量
		return c.handleVariableSelect(stmt)
	}

	backend := c.GetBackendDB()
	if backend == nil {
		golog.Fatal("ClientConn", "handleSelect", "backend is nil", c.connectionId, "db", c.db)
		r := c.newEmptyResultset(stmt)
		return c.writeResultset(c.status, r)
	}
	rs, err := backend.Query(sql, args...)
	if err != nil {
		golog.Error("ClientConn", "handleSelect", err.Error(), c.connectionId)
		return err
	}
	status := c.status | rs.Status
	if rs.Resultset != nil {
		err = c.writeResultset(status, rs.Resultset)
	} else {
		r := c.newEmptyResultset(stmt)
		err = c.writeResultset(status, r)
	}

	return err
}

func (c *ClientConn) handleVariableSelect(stmt *sqlparser.Select) error {

	row := []interface{}{}
	columns := []string{}
	status := c.status | 0
	for _, col := range stmt.SelectExprs {
		colName, aliasName := sqlparser.BuildColumn(col)
		if aliasName == "" {
			columns = append(columns, colName)
		} else {
			columns = append(columns, aliasName)
		}

		colName = strings.ReplaceAll(colName, "@", "")
		if v, ok := mysql.GlobalVariable[colName]; ok {
			row = append(row, v)
			continue
		}
		if v, ok := mysql.SessionVariable[colName]; ok {
			row = append(row, v)
			continue
		}
		r := c.newEmptyResultset(stmt)
		return c.writeResultset(status, r)
	}
	rs, _ := c.buildResultset(nil, columns, [][]interface{}{row})
	return c.writeResultset(status, rs)
}
