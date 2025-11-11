package executor

import (
	"fmt"
	"godb/catalog"
	"godb/storage"
	"godb/types"

	"github.com/xwb1989/sqlparser"
)

// executeUpdate 执行 UPDATE 语句
func (e *Executor) executeUpdate(stmt *sqlparser.Update) (string, error) {
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

	// 解析 SET 子句
	updates := make(map[string]interface{})
	for _, expr := range stmt.Exprs {
		colName := expr.Name.Name.String()

		// 获取列类型
		colType, err := schema.GetColumnType(colName)
		if err != nil {
			return "", err
		}

		// 计算新值
		value, err := e.evalExpr(expr.Expr, colType)
		if err != nil {
			return "", fmt.Errorf("failed to evaluate value for column %s: %w", colName, err)
		}

		updates[colName] = value
	}

	// 过滤并更新行
	updateCount := 0
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
			// 创建新行（复制原行的值）
			newRow := &storage.Row{
				Values: make([]types.Value, len(row.Values)),
			}
			copy(newRow.Values, row.Values)

			// 应用更新
			for colName, value := range updates {
				colIndex := schema.GetColumnIndex(colName)
				if colIndex == -1 {
					return "", fmt.Errorf("column not found: %s", colName)
				}
				newRow.Values[colIndex] = value.(types.Value)
			}

			// 执行更新（标记旧行删除 + 插入新行）
			if err := tableStorage.UpdateRow(row.ID, newRow); err != nil {
				return "", fmt.Errorf("failed to update row: %w", err)
			}

			updateCount++
		}
	}

	return fmt.Sprintf("%d row(s) updated", updateCount), nil
}
