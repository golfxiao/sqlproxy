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
)

type convertSQLPlugin struct {
	db        dbQuerier
	converter sqlparser.SQLConverter
}

var _ SQLPlugin = new(convertSQLPlugin)

func (d *convertSQLPlugin) Prepare(query string) (*sql.Stmt, error) {
	convertSQL, _, err := d.converter.Convert(query)
	if err != nil {
		golog.Warn("convertSQLPlugin", "Prepare", err.Error(), 0)
		convertSQL = query
	}
	stmt, err := d.db.Prepare(convertSQL)
	return stmt, err
}

func (d *convertSQLPlugin) Exec(query string, args ...interface{}) (sql.Result, error) {
	convertSQL, newArgs, err := d.converter.Convert(query, args...)
	if err != nil {
		golog.Warn("convertSQLPlugin", "Prepare", err.Error(), 0)
		convertSQL = query
	}
	res, err := d.db.Exec(convertSQL, newArgs...)
	return res, err
}

func (d *convertSQLPlugin) Query(query string, args ...interface{}) (*sql.Rows, error) {
	convertSQL, _, err := d.converter.Convert(query)
	if err != nil {
		golog.Warn("convertSQLPlugin", "Prepare", err.Error(), 0)
		convertSQL = query
	}
	res, err := d.db.Query(convertSQL, args...)
	return res, err
}

func (d *convertSQLPlugin) QueryRow(query string, args ...interface{}) *sql.Row {
	convertSQL, _, err := d.converter.Convert(query)
	if err != nil {
		golog.Warn("convertSQLPlugin", "Prepare", err.Error(), 0)
		convertSQL = query
	}
	res := d.db.QueryRow(convertSQL, args...)
	return res
}

func (d *convertSQLPlugin) Begin() (*sql.Tx, error) {
	return d.db.(txer).Begin()
}

func (d *convertSQLPlugin) Commit() error {
	return d.db.(txEnder).Commit()
}

func (d *convertSQLPlugin) Rollback() error {
	return d.db.(txEnder).Rollback()
}

func wrapConverter(db dbQuerier, alias, driverName string, converterName string) (dbQuerier, error) {
	//支持达梦DB，查询表唯一索引和主键，用于 (on duplicate key update)  ->  (merge into ... using dual on ... when matched then update ... when not matched then insert)
	tableUniqueIndexs := map[string]map[string][]string{}
	incrementColumns := map[string]map[string]int{}
	tableColumns := map[string][]string{}
	var err error
	if driverName == "dm" {
		tableUniqueIndexs, err = getTableUniqueIndexs(db, alias)
		if err != nil {
			return nil, err
		}

		tableColumns, incrementColumns, err = getTableColumns(db, alias)
		if err != nil {
			return nil, err
		}
	}

	golog.Info("convertSQLPlugin", "wrapConverter", fmt.Sprintf("alias: %s, converterName: %s", alias, converterName), 0)
	converter := sqlparser.GetSQLConverter(converterName, tableUniqueIndexs, tableColumns, incrementColumns)
	if converter == nil {
		golog.Warn("convertSQLPlugin", "Prepare", "Unsupported converterName:"+converterName, 0)
		return db, nil
	}

	return &convertSQLPlugin{
		db:        db,
		converter: converter,
	}, nil
}

func getTableUniqueIndexs(db dbQuerier, alias string) (map[string]map[string][]string, error) {
	rows, err := db.Query(fmt.Sprintf(`select cc.table_name, cc.constraint_name, cc.column_name from dba_constraints c, dba_cons_columns cc where c.constraint_name = cc.constraint_name and c.owner = '%s' and (c.constraint_type='U' or c.constraint_type='P')`, alias))
	if err != nil {
		return nil, err
	}
	constraints := make(map[string]map[string][]string)
	for rows.Next() {
		var (
			tableName  string
			indexName  string
			columnName string
		)
		if err1 := rows.Scan(&tableName, &indexName, &columnName); err1 != nil {
			return nil, err
		}
		if constraints[tableName] == nil {
			constraints[tableName] = make(map[string][]string)
		}
		if constraints[tableName][indexName] == nil {
			constraints[tableName][indexName] = []string{}
		}
		constraints[tableName][indexName] = append(constraints[tableName][indexName], columnName)
	}
	return constraints, nil
}

func getTableColumns(db dbQuerier, alias string) (map[string][]string, map[string]map[string]int, error) {
	rows, err := db.Query(fmt.Sprintf(`select b.object_name table_name,a.name col_name, a.colid col_id,a.info2 is_incr from syscolumns a, all_objects b where a.id=b.object_id and b.object_type='table' and b.owner='%s' order by a.colid asc`, alias))
	if err != nil {
		return nil, nil, err
	}
	tableColumns := make(map[string][]string)
	incrementColumns := make(map[string]map[string]int)
	for rows.Next() {
		var (
			tableName  string
			columnName string
			colIndex   int
			isIncr     int
		)
		if err1 := rows.Scan(&tableName, &columnName, &colIndex, &isIncr); err1 != nil {
			return nil, nil, err1
		}
		if tableColumns[tableName] == nil {
			tableColumns[tableName] = []string{}
		}
		tableColumns[tableName] = append(tableColumns[tableName], columnName)
		if isIncr&0x01 == 0x01 {
			if incrementColumns[tableName] == nil {
				incrementColumns[tableName] = make(map[string]int)
			}
			incrementColumns[tableName][columnName] = colIndex
		}
	}
	return tableColumns, incrementColumns, nil
}
