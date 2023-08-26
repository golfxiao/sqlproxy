// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"database/sql"
	"fmt"
	"sqlproxy/core/golog"
	"sqlproxy/sqlparser"
	"strings"
	"time"
)

func debugLogQueies(alias string, operaton, query string, t time.Time, err error, args ...interface{}) {
	sub := time.Now().Sub(t) / 1e5
	elsp := float64(int(sub)) / 10.0
	flag := "  OK"
	if err != nil {
		flag = "FAIL"
	}
	if formatQuery, err := sqlparser.Format(query, args); err != nil {
		golog.Warn("BackendProxy", "debugLogQueries", err.Error(), 0, "query", query)
	} else {
		query = formatQuery
		args = make([]interface{}, 0)
	}
	con := fmt.Sprintf("[Queries/%s] - [%11s / %7.1fms] - [%s]", alias, operaton, elsp, query)
	cons := make([]string, 0, len(args))
	for _, arg := range args {
		cons = append(cons, fmt.Sprintf("%v", arg))
	}
	if len(cons) > 0 {
		con += fmt.Sprintf(" - `%s`", strings.Join(cons, "`, `"))
	}
	if err != nil {
		con += " - " + err.Error()
	}
	golog.OutputSql(flag, con)
}

// database query logger struct.
// if dev mode, use logSQLPlugin, or use dbQuerier.
type logSQLPlugin struct {
	db    dbQuerier
	alias string
	// tx    txer
	// txe   txEnder
}

var _ SQLPlugin = new(logSQLPlugin)

// var _ txer = new(logSQLPlugin)
// var _ txEnder = new(logSQLPlugin)

func (d *logSQLPlugin) Prepare(query string) (*sql.Stmt, error) {
	a := time.Now()
	stmt, err := d.db.Prepare(query)
	debugLogQueies(d.alias, "db.Prepare", query, a, err)
	return stmt, err
}

func (d *logSQLPlugin) Exec(query string, args ...interface{}) (sql.Result, error) {
	a := time.Now()
	res, err := d.db.Exec(query, args...)
	debugLogQueies(d.alias, "db.Exec", query, a, err, args...)
	return res, err
}

func (d *logSQLPlugin) Query(query string, args ...interface{}) (*sql.Rows, error) {
	a := time.Now()
	res, err := d.db.Query(query, args...)
	debugLogQueies(d.alias, "db.Query", query, a, err, args...)
	return res, err
}

func (d *logSQLPlugin) QueryRow(query string, args ...interface{}) *sql.Row {
	a := time.Now()
	res := d.db.QueryRow(query, args...)
	debugLogQueies(d.alias, "db.QueryRow", query, a, nil, args...)
	return res
}

func (d *logSQLPlugin) Begin() (*sql.Tx, error) {
	a := time.Now()
	tx, err := d.db.(txer).Begin()
	debugLogQueies(d.alias, "db.Begin", "START TRANSACTION", a, err)
	return tx, err
}

func (d *logSQLPlugin) Commit() error {
	a := time.Now()
	err := d.db.(txEnder).Commit()
	debugLogQueies(d.alias, "tx.Commit", "COMMIT", a, err)
	return err
}

func (d *logSQLPlugin) Rollback() error {
	a := time.Now()
	err := d.db.(txEnder).Rollback()
	debugLogQueies(d.alias, "tx.Rollback", "ROLLBACK", a, err)
	return err
}

func wrapQueryLog(db dbQuerier, alias string) SQLPlugin {
	golog.Info("logSQLPlugin", "wrapQueryLog", "db:"+alias, 0)
	d := new(logSQLPlugin)
	d.db = db
	d.alias = alias
	return d
}
