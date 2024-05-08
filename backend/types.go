package backend

import (
	"database/sql"
)

// db querier
type dbQuerier interface {
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type dbQuerierWithCtx interface {
	IContext
	dbQuerier
}

// transaction beginner
type txer interface {
	Begin() (*sql.Tx, error)
}

// transaction ending
type txEnder interface {
	Commit() error
	Rollback() error
}

type SQLPlugin interface {
	dbQuerierWithCtx
	txer
	txEnder
}

func dbQuerierToTxer(db dbQuerierWithCtx) txer {
	if wrapper, ok := db.(*PoolWrapper); ok {
		return wrapper.dbQuerier.(txer)
	} else {
		return db.(txer)
	}
}

func dbQuerierToTxEnder(db dbQuerierWithCtx) txEnder {
	if wrapper, ok := db.(*PoolWrapper); ok {
		return wrapper.dbQuerier.(txEnder)
	} else {
		return db.(txEnder)
	}
}
