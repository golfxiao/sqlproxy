package sqlparser

const (
	MYSQL_TO_ORACLE = "mysql-to-oracle"
)

type SQLConverter interface {
	Convert(sql string, args ...interface{}) (string, []interface{}, error)
}

func GetSQLConverter(name string, tableUniqueIndexs map[string]map[string][]string, tableColumns map[string][]string, incrementColumns map[string]map[string]int) SQLConverter {
	switch name {
	case MYSQL_TO_ORACLE:
		return NewOracleConverter(tableUniqueIndexs, tableColumns, incrementColumns)
	default:
		return nil
	}
}
