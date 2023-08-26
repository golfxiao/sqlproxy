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
	"fmt"
	"runtime"
	"strings"

	"sqlproxy/core/errors"
	"sqlproxy/core/golog"
	"sqlproxy/core/hack"
	"sqlproxy/mysql"
	"sqlproxy/sqlparser"
)

/*处理query语句*/
func (c *ClientConn) handleQuery(sql string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			golog.OutputSql("Error", "err:%v,sql:%s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				golog.Error("ClientConn", "handleQuery",
					err.Error(), c.connectionId,
					"stack", string(buf), "sql", sql)
			}

			err = errors.ErrInternalServer
			return
		}
	}()
	golog.Debug("ClientConn", "handleQuery", sql, c.connectionId)

	sql = strings.TrimRight(sql, ";") //删除sql语句最后的分号

	var stmt sqlparser.Statement
	stmt, err = sqlparser.Parse(sql) //解析sql语句,得到的stmt是一个interface
	if err != nil {
		golog.Error("ClientConn", "handleQuery", err.Error(), c.connectionId /*"hasHandled", hasHandled,*/, "sql", sql)
		return err
	}

	switch v := stmt.(type) {
	case *sqlparser.Show:
		return c.handleShow(v, sql, nil)
	case *sqlparser.Select:
		return c.handleSelect(v, sql, nil)
	case *sqlparser.Insert:
		return c.handleExec(sql, nil)
	case *sqlparser.Update:
		return c.handleExec(sql, nil)
	case *sqlparser.Delete:
		return c.handleExec(sql, nil)
	// case *sqlparser.Replace: // Replace --> Insert
	// 	return c.handleExec(sql, nil)
	case *sqlparser.Set:
		return c.handleSet(v, sql)
	case *sqlparser.Begin:
		return c.handleBegin()
	case *sqlparser.Commit:
		return c.handleCommit()
	case *sqlparser.Rollback:
		return c.handleRollback()
	// case *sqlparser.Admin: // kingshard自己加的指令
	// 	if c.user == "root" {
	// 		return c.handleAdmin(v)
	// 	}
	// 	return fmt.Errorf("statement %T not support now", stmt)
	// case *sqlparser.AdminHelp: // kingshard自己加的指令
	// 	if c.user == "root" {
	// 		return c.handleAdminHelp(v)
	// 	}
	// 	return fmt.Errorf("statement %T not support now", stmt)
	case *sqlparser.Use:
		return c.handleUseDB(v.DBName.String())
	// case *sqlparser.SimpleSelect:
	// 	return c.handleSimpleSelect(v)
	case *sqlparser.DDL: // Modify: Old Truncate --> DDL
		return c.handleExec(sql, nil)
	case *sqlparser.Union:
		return c.handleUnion(v, sql, nil)
	default:
		return fmt.Errorf("statement %T not support now", stmt)
	}
}

func (c *ClientConn) newEmptyResultset(stmt *sqlparser.Select) *mysql.Resultset {
	r := new(mysql.Resultset)
	r.Fields = make([]*mysql.Field, len(stmt.SelectExprs))

	for i, expr := range stmt.SelectExprs {
		r.Fields[i] = &mysql.Field{}
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			r.Fields[i].Name = []byte("*")
		case *sqlparser.AliasedExpr: // Modify: NonStarExpr -> AliasedExpr
			if !e.As.IsEmpty() {
				r.Fields[i].Name = hack.Slice(nstring(e.As))
				r.Fields[i].OrgName = hack.Slice(nstring(e.Expr))
			} else {
				r.Fields[i].Name = hack.Slice(nstring(e.Expr))
			}
		default:
			r.Fields[i].Name = hack.Slice(nstring(e))
		}
	}

	r.Values = make([][]interface{}, 0)
	r.RowDatas = make([]mysql.RowData, 0)

	return r
}

func (c *ClientConn) handleExec(sql string, args []interface{}) error {
	backend := c.GetBackendDB()
	if backend == nil {
		golog.Fatal("ClientConn", "handleExec", "no backend db", c.connectionId)
		return c.writeOK(nil)
	}

	rs, err := backend.Exec(sql, args...)
	if err != nil {
		golog.Error("ClientConn", "handleExec", err.Error(), c.connectionId)
		return err
	}

	// TODO 为何与select不同，没有与c.Status作按位或操作？
	status := rs.Status
	if rs.Resultset != nil {
		err = c.writeResultset(status, rs.Resultset)
	} else {
		err = c.writeOK(rs)
	}

	return err
}
