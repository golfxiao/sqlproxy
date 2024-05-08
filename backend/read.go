//go:build !darwin
// +build !darwin

package backend

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golfxiao/dm"
	"io"
	"sqlproxy/core/golog"
	"strings"
	"time"
)

func readRow(driverName string, columnTypes []*sql.ColumnType, cursor *sql.Rows) ([]sql.RawBytes, error) {
	cols := make([]interface{}, len(columnTypes))
	for i := range cols {
		cols[i] = initializeColValue(driverName, columnTypes[i])
	}

	if err := cursor.Scan(cols...); err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(cols))
	for i, col := range cols {

		switch col.(type) {
		case *interface{}:
			switch (*col.(*interface{})).(type) {
			case time.Time:
				v := (*col.(*interface{})).(time.Time)
				tv := ""
				switch columnTypes[i].DatabaseTypeName() {
				case "DATE":
					tv = v.Format("2006-01-02")
				case "TIMESTAMP":
					tv = v.Format("2006-01-02 15:04:05")
				}
				values[i] = sql.RawBytes(strings.ReplaceAll(tv, "0001-01-01", "0000-00-00"))
			default:
				values[i] = nil
			}
		case *sql.RawBytes:
			values[i] = *(col.(*sql.RawBytes))
		case *dm.DmClob:
			bytes, err := readDmClob(col.(*dm.DmClob))
			if err != nil {
				return nil, err
			}
			values[i] = bytes
		default:
			values[i] = nil
		}
	}

	return values, nil
}

func readDmClob(dmClob *dm.DmClob) (sql.RawBytes, error) {
	if dmClob == nil || !dmClob.Valid {
		return sql.RawBytes{}, nil
	}
	// 获取长度
	len, err := dmClob.GetLength()
	if err != nil {
		return nil, err
	}

	// 读取字符串, 处理空字符串或NULL造成的EOF错误
	str, err := dmClob.ReadString(1, int(len))
	if err == io.EOF {
		return sql.RawBytes{}, nil
	}

	return sql.RawBytes([]byte(str)), nil
}

func initializeColValue(driverName string, columnType *sql.ColumnType) interface{} {
	scanType := driverName + "." + strings.ToUpper(columnType.DatabaseTypeName())
	switch scanType {
	case "dm.DATE", "dm.TIMESTAMP":
		return new(interface{})
	case "dm.CLOB", "dm.TEXT", "dm.LONGTEXT":
		golog.Debug("BackendProxy", "initializeColValue", "DmClob", 0, "scanType", scanType)
		return dm.NewClob("")
	default:
		return new(sql.RawBytes)
	}

}
