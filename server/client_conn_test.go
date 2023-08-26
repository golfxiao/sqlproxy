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
	"sqlproxy/mysql"
	"sqlproxy/sqlparser"
	"testing"
)

func TestClientConn_DropTable(t *testing.T) {
	c := testConn
	if err := c.handleQuery(`drop table if exists kingshard_test_proxy_stmt`); err != nil {
		t.Fatal(err)
	}
}

func TestClientConn_CreateTable(t *testing.T) {
	str := `CREATE TABLE IF NOT EXISTS kingshard_test_proxy_stmt (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	c := testConn
	if err := c.handleQuery(str); err != nil {
		t.Fatal(err)
	}
}

func TestClientConn_Insert(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, str, f, e, u, i) values (?, ?, ?, ?, ?, ?)`

	c := testConn

	stmtId := c.stmtId
	err := c.handleStmtPrepare(str)
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ := c.stmts[stmtId]
	if err := c.handlePrepareExec(stmt.s, stmt.sql, []interface{}{1, "a", 3.14, "test1", 255, -127}); err != nil {
		t.Fatal(err)
	}
}

func TestClientConn_Select(t *testing.T) {
	str := `select str, f, e from kingshard_test_proxy_stmt where id = ?`

	c := testConn

	stmtId := c.stmtId
	err := c.handleStmtPrepare(str)
	if err != nil {
		t.Fatal(err)
	}
	stmt, _ := c.stmts[stmtId]

	if err := c.handlePrepareSelect(stmt.s.(*sqlparser.Select), stmt.sql, []interface{}{1}); err != nil {
		t.Fatal(err)
	}
}

func TestClientConn_NULL(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, str, f, e) values (?, ?, ?, ?)`

	c := testConn

	stmtId := c.stmtId
	err := c.handleStmtPrepare(str)
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ := c.stmts[stmtId]
	if err := c.handlePrepareExec(stmt.s, stmt.sql, []interface{}{2, nil, 3.14, nil}); err != nil {
		t.Fatal(err)
	}

	str = `select * from kingshard_test_proxy_stmt where id = ?`
	stmtId = c.stmtId
	err = c.handleStmtPrepare(str)
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ = c.stmts[stmtId]
	if err := c.handlePrepareSelect(stmt.s.(*sqlparser.Select), stmt.sql, []interface{}{2}); err != nil {
		t.Fatal(err)
	}
}

func TestClientConn_Unsigned(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, u) values (?, ?)`

	c := testConn

	stmtId := c.stmtId
	err := c.handleStmtPrepare(str)
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ := c.stmts[stmtId]
	if err := c.handlePrepareExec(stmt.s, stmt.sql, []interface{}{3, uint8(255)}); err != nil {
		t.Fatal(err)
	}

	str = `select u from kingshard_test_proxy_stmt where id = ?`
	stmtId = c.stmtId
	err = c.handleStmtPrepare(str)
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ = c.stmts[stmtId]
	if err := c.handlePrepareSelect(stmt.s.(*sqlparser.Select), stmt.sql, []interface{}{3}); err != nil {
		t.Fatal(err)
	}
}

func TestClientConn_Signed(t *testing.T) {
	str := `insert into kingshard_test_proxy_stmt (id, i) values (?, ?)`

	c := testConn

	stmtId := c.stmtId
	err := c.handleStmtPrepare(str)
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ := c.stmts[stmtId]
	if err := c.handlePrepareExec(stmt.s, stmt.sql, []interface{}{4, 127}); err != nil {
		t.Fatal(err)
	}

	if err := c.handlePrepareExec(stmt.s, stmt.sql, []interface{}{uint64(18446744073709551516), int8(-128)}); err != nil {
		t.Fatal(err)
	}

}

func TestClientConn_Trans(t *testing.T) {
	c := testConn

	if err := c.handleQuery(`insert into kingshard_test_proxy_stmt (id, str) values (1002, "abc")`); err != nil {
		t.Fatal(err)
	}

	err := c.handleBegin()
	if err != nil {
		t.Fatal(err)
	}

	str := `select str from kingshard_test_proxy_stmt where id = ?`

	stmtId := c.stmtId
	err = c.handleStmtPrepare(str)
	if err != nil {
		t.Fatal(err)
	}

	stmt, _ := c.stmts[stmtId]
	if err := c.handlePrepareSelect(stmt.s.(*sqlparser.Select), stmt.sql, []interface{}{1002}); err != nil {
		t.Fatal(err)
	}

	if err := c.handleCommit(); err != nil {
		t.Fatal(err)
	}

	stmt, _ = c.stmts[stmtId]
	if err := c.handlePrepareSelect(stmt.s.(*sqlparser.Select), stmt.sql, []interface{}{1002}); err != nil {
		t.Fatal(err)
	}

	if err := c.handleStmtClose(mysql.Uint32ToBytes(stmtId)); err != nil {
		t.Fatal(err)
	}
}
