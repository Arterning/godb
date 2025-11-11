package executor

import (
	"fmt"
	"godb/catalog"
	"godb/storage"
	"godb/transaction"
	"godb/types"
	"strconv"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser"
)

// executeInsert 执行 INSERT 语句
func (e *Executor) executeInsert(stmt *sqlparser.Insert) (string, error) {
	// 获取表名
	tableName := stmt.Table.Name.String()

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

	// 解析插入的值
	rows, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return "", fmt.Errorf("unsupported insert syntax")
	}

	insertCount := 0
	for _, valTuple := range rows {
		// 检查值的数量
		if len(valTuple) != len(schema.Columns) {
			return "", fmt.Errorf("column count mismatch: expected %d, got %d", len(schema.Columns), len(valTuple))
		}

		// 构造行
		row := &storage.Row{
			TxID:   txID, // 设置事务ID
			Values: make([]types.Value, len(schema.Columns)),
		}

		for i, expr := range valTuple {
			value, err := e.evalExpr(expr, schema.Columns[i].Type)
			if err != nil {
				return "", fmt.Errorf("failed to evaluate value for column %s: %w", schema.Columns[i].Name, err)
			}
			row.Values[i] = value
		}

		// 插入行
		if err := tableStorage.InsertRow(row); err != nil {
			return "", fmt.Errorf("failed to insert row: %w", err)
		}

		// 更新所有相关索引
		columnNames := make([]string, len(schema.Columns))
		for i, col := range schema.Columns {
			columnNames[i] = col.Name
		}
		if err := e.indexManager.InsertEntry(tableName, row, columnNames); err != nil {
			return "", fmt.Errorf("failed to update index: %w", err)
		}

		// 记录操作到事务日志（用于回滚）
		if e.currentTx != nil {
			op := &transaction.Operation{
				Type:      transaction.OpInsert,
				TableName: tableName,
				RowID:     row.ID,
				NewData:   row,
			}
			e.currentTx.AddOperation(op)
		}

		insertCount++
	}

	// 如果是自动提交模式，立即释放锁和刷新
	if e.currentTx == nil {
		lockManager.ReleaseLocks(transaction.TransactionID(txID))
		if err := e.pager.FlushAll(); err != nil {
			return "", fmt.Errorf("failed to flush pages: %w", err)
		}
	}

	return fmt.Sprintf("%d row(s) inserted", insertCount), nil
}

// evalExpr 计算表达式值
func (e *Executor) evalExpr(expr sqlparser.Expr, expectedType types.DataType) (types.Value, error) {
	switch expr := expr.(type) {
	case *sqlparser.SQLVal:
		return e.evalSQLVal(expr, expectedType)
	default:
		return types.Value{}, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evalSQLVal 计算 SQL 值
func (e *Executor) evalSQLVal(val *sqlparser.SQLVal, expectedType types.DataType) (types.Value, error) {
	switch val.Type {
	case sqlparser.IntVal:
		intVal, err := strconv.ParseInt(string(val.Val), 10, 64)
		if err != nil {
			return types.Value{}, err
		}
		// 支持 INT 到 FLOAT 的自动转换
		if expectedType == types.TypeFloat {
			return types.NewFloatValue(float64(intVal)), nil
		}
		if expectedType != types.TypeInt {
			return types.Value{}, fmt.Errorf("type mismatch: expected %s, got INT", expectedType)
		}
		return types.NewIntValue(intVal), nil

	case sqlparser.StrVal:
		strVal := string(val.Val)
		switch expectedType {
		case types.TypeText:
			return types.NewTextValue(strVal), nil
		case types.TypeDate:
			// 解析日期（支持 YYYY-MM-DD 格式）
			date, err := time.Parse("2006-01-02", strVal)
			if err != nil {
				return types.Value{}, fmt.Errorf("invalid date format: %s", strVal)
			}
			return types.NewDateValue(date), nil
		case types.TypeBoolean:
			// 支持 'true'/'false' 字符串
			lowerStr := strings.ToLower(strVal)
			if lowerStr == "true" {
				return types.NewBooleanValue(true), nil
			} else if lowerStr == "false" {
				return types.NewBooleanValue(false), nil
			}
			return types.Value{}, fmt.Errorf("invalid boolean value: %s", strVal)
		default:
			return types.Value{}, fmt.Errorf("type mismatch: expected %s, got TEXT", expectedType)
		}

	case sqlparser.FloatVal:
		floatVal, err := strconv.ParseFloat(string(val.Val), 64)
		if err != nil {
			return types.Value{}, err
		}
		if expectedType != types.TypeFloat {
			return types.Value{}, fmt.Errorf("type mismatch: expected %s, got FLOAT", expectedType)
		}
		return types.NewFloatValue(floatVal), nil

	default:
		return types.Value{}, fmt.Errorf("unsupported value type: %v", val.Type)
	}
}
