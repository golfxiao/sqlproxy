package mysql

var GlobalVariable = map[string]string{

	"character_set_client":     "utf8mb4",
	"character_set_connection": "utf8mb4",
	"character_set_results":    "utf8",
	"character_set_server":     "utf8mb4",
	"collation_server":         "utf8mb4_unicode_ci",
	"collation_connection":     "utf8mb4_general_ci",
	"init_connect":             "SET NAMES utf8mb4",
	"interactive_timeout":      "900",
	"license":                  "GPL",
	"lower_case_table_names":   "0",
	"max_allowed_packet":       "16777216",
	"net_buffer_length":        "16384",
	"net_write_timeout":        "60",
	"performance_schema":       "0",
	"query_cache_size":         "67108864",
	"query_cache_type":         "ON",
	"sql_mode":                 "STRICT_TRANS_TABLES",
	"system_time_zone":         "CST",
	"time_zone":                "+08:00",
	"tx_isolation":             "REPEATABLE-READ",
	"wait_timeout":             "30",
}

var SessionVariable = map[string]string{
	"session.auto_increment_increment": "2",
	"session.autocommit":               "1",
	"session.tx_isolation":             "REPEATABLE-READ",
}
