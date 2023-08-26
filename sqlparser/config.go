package sqlparser

import (
	"log"
)

var (
	// 表名和唯一索引列的配置,用于拼接merge into的查询条件
	// 说明：暂时只支持一个表配置一条唯一索引，一条索引可以有多列
	UniqueColumns = map[string][]string{}
)

func Init(ukCfg map[string][]string) {
	for table, columns := range ukCfg {
		UniqueColumns[table] = columns
	}
	log.Printf("Init sqlparser config: %v", ukCfg)
}
