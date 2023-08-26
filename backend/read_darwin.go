//go:build darwin
// +build darwin

package backend

import (
	"database/sql"
	"sqlproxy/core/golog"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func readRow(driverName string, columnTypes []*sql.ColumnType, cursor *sql.Rows) ([]sql.RawBytes, error) {
	columnTypes, err := cursor.ColumnTypes()
	if err != nil {
		return nil, err
	}

	columnTypeNames := make([]string, len(columnTypes))
	cols := make([]interface{}, len(columnTypes))
	for i, _ := range cols {
		cols[i] = new(sql.RawBytes)
		columnTypeNames[i] = strings.ToUpper(columnTypes[i].DatabaseTypeName())
	}

	golog.Debug("BackendProxy", "query", "", 0, "columnTypes", columnTypeNames)

	err = cursor.Scan(cols...)
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(cols))
	for i, col := range cols {
		switch col.(type) {
		case *sql.RawBytes:
			values[i] = *(col.(*sql.RawBytes))
		default:
			values[i] = nil
		}
	}
	return values, nil
}
