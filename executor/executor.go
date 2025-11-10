package executor

import (
	"fmt"
	"godb/catalog"
	"godb/parser"
	"godb/storage"
	"github.com/xwb1989/sqlparser"
)

// Executor 查询执行器
type Executor struct {
	catalog *catalog.Catalog
	pager   *storage.Pager
}

// NewExecutor 创建执行器
func NewExecutor(catalog *catalog.Catalog, pager *storage.Pager) *Executor {
	return &Executor{
		catalog: catalog,
		pager:   pager,
	}
}

// Execute 执行 SQL 语句
func (e *Executor) Execute(sql string) (string, error) {
	// 解析 SQL
	stmt, err := parser.Parse(sql)
	if err != nil {
		return "", err
	}

	// 根据语句类型分发
	switch stmt := stmt.(type) {
	case *sqlparser.DDL:
		return e.executeDDL(stmt)
	case *sqlparser.Insert:
		return e.executeInsert(stmt)
	case *sqlparser.Select:
		return e.executeSelect(stmt)
	case *sqlparser.Update:
		return e.executeUpdate(stmt)
	case *sqlparser.Delete:
		return e.executeDelete(stmt)
	default:
		return "", fmt.Errorf("unsupported statement type")
	}
}

// executeDDL 执行 DDL 语句（CREATE, DROP 等）
func (e *Executor) executeDDL(stmt *sqlparser.DDL) (string, error) {
	switch stmt.Action {
	case "create":
		return e.executeCreateTable(stmt)
	case "drop":
		return e.executeDropTable(stmt)
	default:
		return "", fmt.Errorf("unsupported DDL action: %s", stmt.Action)
	}
}
