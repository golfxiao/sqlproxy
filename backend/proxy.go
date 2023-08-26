package backend

import (
	"database/sql"
	"errors"
	"sqlproxy/config"
	"sqlproxy/core/golog"
	"sqlproxy/mysql"
	"sqlproxy/sqlparser"
	"time"
)

var (
	ErrTxHasBegan    = errors.New("<BackendProxy.Begin> transaction already begin")
	ErrTxDone        = errors.New("<BackendProxy.Commit/Rollback> transaction not begin")
	ErrDbNullPointer = errors.New("BackendProxy>> db is null")
)

type BackendProxy struct {
	cfg  config.NodeConfig
	isTx bool      // 是否在事务中
	db   dbQuerier // 实现了sql.DB接口的对象，可以是sql.DB，也可以是其它包装后的对象
}

func NewBackendProxy(cfg config.NodeConfig) *BackendProxy {
	return &BackendProxy{
		cfg: cfg,
	}
}

func (n *BackendProxy) InitConnectionPool() error {
	pool, err := sql.Open(n.cfg.DriverName, n.cfg.Datasource)
	if err != nil {
		return err
	}
	pool.SetMaxOpenConns(n.cfg.MaxOpenConns)
	pool.SetMaxIdleConns(n.cfg.MaxOpenConns)

	err = pool.Ping()
	if err != nil {
		return err
	}

	db, err := wrapFunctions(pool, n.cfg)
	if err != nil {
		return err
	}
	n.db = db

	err = n.checkAvailable()
	if err != nil {
		return err
	}

	golog.Info("BackendProxy", "InitConnectionPool", "", 0, "cfg", n.cfg)
	return nil
}

func (n *BackendProxy) checkAvailable() error {
	if n.db == nil {
		return ErrDbNullPointer
	}
	if n.cfg.TestSQL == "" {
		return nil
	}
	rs, err := n.Query(n.cfg.TestSQL)
	if err != nil {
		return err
	}
	golog.Debug("BackendProxy", "checkAvailable", n.cfg.Name, 0, "rows", rs.RowNumber(), "columns", rs.FieldNames)
	return nil
}

func (n *BackendProxy) Exec(query string, args ...interface{}) (*mysql.Result, error) {
	if n.db == nil {
		return nil, ErrDbNullPointer
	}
	rs, err := n.db.Exec(query, args...)
	if err != nil {
		return nil, err
	}

	affectedRows, err := rs.RowsAffected()
	if err != nil {
		return nil, err
	}
	insertId, err := rs.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &mysql.Result{
		Status:       0,
		AffectedRows: uint64(affectedRows),
		InsertId:     uint64(insertId),
	}, nil
}

func (n *BackendProxy) query(query string, args ...interface{}) ([][]sql.RawBytes, []*sql.ColumnType, error) {
	if n.db == nil {
		return nil, nil, ErrDbNullPointer
	}

	cursor, err := n.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close()
	golog.Debug("BackendProxy", "query", "db...", 0)

	columnTypes, err := cursor.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	columnTypeNames := make([]string, len(columnTypes))
	for _, column := range columnTypes {
		columnTypeNames = append(columnTypeNames, column.Name())
	}

	golog.Debug("BackendProxy", "query", "", 0, "columnTypes", columnTypeNames)

	rows := make([][]sql.RawBytes, 0)
	for cursor.Next() {
		values, err := readRow(n.cfg.DriverName, columnTypes, cursor)
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, values)
	}
	golog.Debug("BackendProxy", "query", "rows size", 0, len(rows), time.Now().UnixNano())

	return rows, columnTypes, nil
}

func (n *BackendProxy) Query(query string, args ...interface{}) (*mysql.Result, error) {
	rows, columnTypes, err := n.query(query, args...)
	if err != nil {
		return nil, err
	}

	rs, err := mysql.BuildResultset(rows, columnTypes, false)
	if err != nil {
		return nil, err
	}
	golog.Debug("BackendProxy", "Query BuildResultset", "", 0, time.Now().UnixNano())
	return &mysql.Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,
		Resultset:    rs,
	}, nil
}

func (n *BackendProxy) StmtQuery(query string, args ...interface{}) (*mysql.Result, error) {
	rows, columns, err := n.query(query, args...)
	if err != nil {
		return nil, err
	}

	rs, err := mysql.BuildResultset(rows, columns, true)
	if err != nil {
		return nil, err
	}
	golog.Debug("BackendProxy", "Query BuildResultset", "", 0)
	return &mysql.Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,
		Resultset:    rs,
	}, nil
}

func (n *BackendProxy) Begin() (*BackendProxy, error) {
	if n.isTx {
		return nil, ErrTxHasBegan
	}

	tx, err := n.db.(txer).Begin()
	if err != nil {
		return nil, err
	}

	db, err := wrapFunctions(tx, n.cfg)
	if err != nil {
		return nil, err
	}
	// 需要对这个事务连接作一层包装，确保在这个事务上发起的sql语句也能被转换成目标数据库语法
	return &BackendProxy{
		cfg:  n.cfg,
		isTx: true,
		db:   db,
	}, nil
}

func (n *BackendProxy) Commit() error {
	if n.isTx == false {
		return ErrTxDone
	}
	err := n.db.(txEnder).Commit()
	if err == nil {
		n.isTx = false
		n.db = nil
	}
	golog.Debug("BackendProxy", "Commit", "", 0)
	return err

}

func (n *BackendProxy) Rollback() error {
	if n.isTx == false {
		return ErrTxDone
	}
	err := n.db.(txEnder).Rollback()
	if err == nil {
		n.isTx = false
		n.db = nil
	}
	return err
}

// wrapFunctions wraps the given dbQuerier with query logging functionality.
//
// It takes a dbQuerier and a config.NodeConfig as parameters and returns a
// dbQuerier. The returned dbQuerier has query logging enabled, which means
// that all database queries executed through it will be logged.
func wrapFunctions(db dbQuerier, cfg config.NodeConfig) (dbQuerier, error) {
	db = wrapQueryLog(db, cfg.Name)
	switch cfg.DriverName {
	case "oci8", "dm":
		return wrapConverter(db, cfg.Name, cfg.DriverName, sqlparser.MYSQL_TO_ORACLE)
	default:
		return db, nil
	}
}
