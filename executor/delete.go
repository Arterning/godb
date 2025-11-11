package executor

import (
	"fmt"
	"godb/catalog"

	"github.com/xwb1989/sqlparser"
)

// executeDelete 执行 DELETE 语句
func (e *Executor) executeDelete(stmt *sqlparser.Delete) (string, error) {
	// 获取表名
	tableName := stmt.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	// 获取表定义
	schema, err := e.catalog.GetTable(tableName)
	if err != nil {
		return "", err
	}

	// 创建表存储
	tableStorage, err := catalog.CreateTableStorage(e.pager, schema)
	if err != nil {
		return "", err
	}

	// 读取所有行（包括已删除的，但我们只处理未删除的）
	allRows, err := tableStorage.GetAllRowsWithDeleted(true)
	if err != nil {
		return "", err
	}

	// 过滤并删除行
	deleteCount := 0
	for _, row := range allRows {
		// 跳过已删除的行
		if row.Deleted {
			continue
		}

		// 应用 WHERE 条件
		match := true
		if stmt.Where != nil {
			match, err = e.evaluateCondition(row, stmt.Where.Expr, schema)
			if err != nil {
				return "", err
			}
		}

		if match {
			// 删除索引条目
			columnNames := make([]string, len(schema.Columns))
			for i, col := range schema.Columns {
				columnNames[i] = col.Name
			}
			if err := e.indexManager.DeleteEntry(tableName, row, columnNames); err != nil {
				return "", fmt.Errorf("failed to delete index entry: %w", err)
			}

			// 标记行为删除
			if err := tableStorage.MarkRowDeleted(row.ID); err != nil {
				return "", fmt.Errorf("failed to delete row: %w", err)
			}
			deleteCount++
		}
	}

	return fmt.Sprintf("%d row(s) deleted", deleteCount), nil
}
