package sqlparser

import (
	"database/sql/driver"
	"sqlproxy/core/hack"
	"strconv"
	"strings"
)

func Format(query string, args []interface{}) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	quoteNum := strings.Count(query, "\"")
	query = strings.Replace(query, "\"", "`", quoteNum)

	var newQuery string
	var err error
	if strings.Count(query, "?") > 0 {
		newQuery, err = FormatIndexArgs(query, args)
	} else if strings.Count(query, ":") > 0 {
		newQuery, err = FormatNameArgs(query, args)
	} else {
		newQuery = query
	}

	newQuery = strings.Replace(newQuery, "`", "\"", quoteNum)
	return newQuery, err

}

// SQL命名参数格式化
func FormatNameArgs(query string, args []interface{}) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	tree, err := Parse(query)
	if err != nil {
		return "", err
	}
	bytes, err := NewParsedQuery(tree).GenerateQueryForArgs(args)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// SQL位置参数格式化
func FormatIndexArgs(query string, args []interface{}) (string, error) {
	if len(args) == 0 {
		return query, nil
	}

	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	buf := make([]byte, 0, len(query)*2)
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int8:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint8:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint16:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint32:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 64)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case string:
			buf = append(buf, '\'')
			buf = append(buf, hack.Slice(v)...)
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return hack.String(buf), nil
}
