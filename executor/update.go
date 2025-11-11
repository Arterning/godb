package executor

import (
	"fmt"
	"godb/catalog"
	"godb/storage"
	"godb/transaction"
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

	// 获取写锁
	txID := e.getCurrentTxID()
	lockManager := e.txManager.GetLockManager()
	if err := lockManager.AcquireWriteLock(tableName, transaction.TransactionID(txID)); err != nil {
		return "", fmt.Errorf("failed to acquire write lock: %w", err)
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
			// 删除旧行的索引条目
			columnNames := make([]string, len(schema.Columns))
			for i, col := range schema.Columns {
				columnNames[i] = col.Name
			}
			if err := e.indexManager.DeleteEntry(tableName, row, columnNames); err != nil {
				return "", fmt.Errorf("failed to delete old index entry: %w", err)
			}

			// 创建新行（复制原行的值）
			newRow := &storage.Row{
				TxID:   txID, // 设置事务ID
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

			// 保存旧行数据（用于回滚）
			oldRowCopy := &storage.Row{
				ID:      row.ID,
				Deleted: row.Deleted,
				TxID:    row.TxID,
				Values:  make([]types.Value, len(row.Values)),
			}
			copy(oldRowCopy.Values, row.Values)

			// 执行更新（标记旧行删除 + 插入新行）
			if err := tableStorage.UpdateRow(row.ID, newRow); err != nil {
				return "", fmt.Errorf("failed to update row: %w", err)
			}

			// 为新行添加索引条目
			if err := e.indexManager.InsertEntry(tableName, newRow, columnNames); err != nil {
				return "", fmt.Errorf("failed to insert new index entry: %w", err)
			}

			// 记录操作到事务日志（用于回滚）
			if e.currentTx != nil {
				op := &transaction.Operation{
					Type:      transaction.OpUpdate,
					TableName: tableName,
					RowID:     row.ID,
					OldData:   oldRowCopy,
					NewData:   newRow,
				}
				e.currentTx.AddOperation(op)
			}

			updateCount++
		}
	}

	// 如果是自动提交模式，立即释放锁和刷新
	if e.currentTx == nil {
		lockManager.ReleaseLocks(transaction.TransactionID(txID))
		if err := e.pager.FlushAll(); err != nil {
			return "", fmt.Errorf("failed to flush pages: %w", err)
		}
	}

	return fmt.Sprintf("%d row(s) updated", updateCount), nil
}
