package parser

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
)

// Parse 解析 SQL 语句
func Parse(sql string) (sqlparser.Statement, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}
	return stmt, nil
}

// StatementType 返回语句类型
func StatementType(stmt sqlparser.Statement) string {
	switch stmt.(type) {
	case *sqlparser.Select:
		return "SELECT"
	case *sqlparser.Insert:
		return "INSERT"
	case *sqlparser.Update:
		return "UPDATE"
	case *sqlparser.Delete:
		return "DELETE"
	case *sqlparser.DDL:
		return "DDL"
	default:
		return "UNKNOWN"
	}
}
