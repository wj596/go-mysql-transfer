package sqlutils

import (
	"database/sql"
	"go-mysql-transfer/util/stringutils"
	"strconv"
	"strings"
	"time"

	"go-mysql-transfer/util/dateutils"
	"go-mysql-transfer/util/log"
)

func GetDataSourceName(username, password, host, schema string, port uint32, charset string) string {
	elements := make([]string, 0)
	elements = append(elements, username, ":", password, "@tcp(", host, ":", stringutils.ToString(port), ")/")
	elements = append(elements, schema)
	elements = append(elements, "?timeout=5s")
	if charset != "" {
		elements = append(elements, "&charset=")
		elements = append(elements, charset)
	}
	return strings.Join(elements, "")
}

func RawBytesToInterface(value sql.RawBytes, databaseType string) interface{} {
	if value == nil {
		return nil
	}

	switch databaseType {
	case "BIT":
		if string(value) == "\x01" {
			return int64(1)
		}
		return int64(0)
	case "DATETIME", "TIMESTAMP":
		vt, err := time.Parse(dateutils.DayTimeSecondFormatter, string(value))
		if err != nil || vt.IsZero() { // failed to parse date or zero date
			return nil
		}
		return vt.Format(dateutils.DayTimeSecondFormatter)
	case "DATE":
		vt, err := time.Parse(dateutils.DayFormatter, string(value))
		if err != nil || vt.IsZero() { // failed to parse date or zero date
			return nil
		}
		return vt.Format(dateutils.DayFormatter)
	case "TINYINT", "SMALLINT", "INT", "BIGINT", "YEAR":
		vv, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			log.Errorf("ConvertColumnData error[%s]", err.Error())
			return nil
		}
		return vv
	case "DECIMAL", "FLOAT", "DOUBLE":
		vv, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			log.Errorf("ConvertColumnData error[%s]", err.Error())
			return nil
		}
		return vv
	}

	return string(value)
}
