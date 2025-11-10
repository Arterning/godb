package executor

import (
	"fmt"
	"godb/catalog"
	"godb/storage"
	"godb/types"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// executeSelect 执行 SELECT 语句
func (e *Executor) executeSelect(stmt *sqlparser.Select) (string, error) {
	// 获取表名
	if len(stmt.From) != 1 {
		return "", fmt.Errorf("only single table select is supported")
	}

	tableName := stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

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

	// 读取所有行
	rows, err := tableStorage.GetAllRows()
	if err != nil {
		return "", err
	}

	// 应用 WHERE 条件过滤
	filteredRows := rows
	if stmt.Where != nil {
		filteredRows, err = e.filterRows(rows, stmt.Where.Expr, schema)
		if err != nil {
			return "", err
		}
	}

	// 选择要显示的列
	selectedColumns, err := e.getSelectedColumns(stmt.SelectExprs, schema)
	if err != nil {
		return "", err
	}

	// 格式化输出
	return e.formatResult(filteredRows, schema, selectedColumns), nil
}

// getSelectedColumns 获取要显示的列索引
func (e *Executor) getSelectedColumns(selectExprs sqlparser.SelectExprs, schema *catalog.TableSchema) ([]int, error) {
	// 检查是否是 SELECT *
	if len(selectExprs) == 1 {
		if _, ok := selectExprs[0].(*sqlparser.StarExpr); ok {
			// SELECT * - 返回所有列
			result := make([]int, len(schema.Columns))
			for i := range result {
				result[i] = i
			}
			return result, nil
		}
	}

	// 解析指定的列
	result := make([]int, 0)
	for _, expr := range selectExprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported select expression")
		}

		colName, ok := aliasedExpr.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, fmt.Errorf("unsupported select expression")
		}

		colIndex := schema.GetColumnIndex(colName.Name.String())
		if colIndex == -1 {
			return nil, fmt.Errorf("column not found: %s", colName.Name.String())
		}

		result = append(result, colIndex)
	}

	return result, nil
}

// filterRows 过滤行（WHERE 条件）
func (e *Executor) filterRows(rows []*storage.Row, whereExpr sqlparser.Expr, schema *catalog.TableSchema) ([]*storage.Row, error) {
	result := make([]*storage.Row, 0)

	for _, row := range rows {
		match, err := e.evaluateCondition(row, whereExpr, schema)
		if err != nil {
			return nil, err
		}
		if match {
			result = append(result, row)
		}
	}

	return result, nil
}

// evaluateCondition 计算条件表达式
func (e *Executor) evaluateCondition(row *storage.Row, expr sqlparser.Expr, schema *catalog.TableSchema) (bool, error) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return e.evalComparison(row, expr, schema)
	case *sqlparser.AndExpr:
		left, err := e.evaluateCondition(row, expr.Left, schema)
		if err != nil {
			return false, err
		}
		right, err := e.evaluateCondition(row, expr.Right, schema)
		if err != nil {
			return false, err
		}
		return left && right, nil
	case *sqlparser.OrExpr:
		left, err := e.evaluateCondition(row, expr.Left, schema)
		if err != nil {
			return false, err
		}
		right, err := e.evaluateCondition(row, expr.Right, schema)
		if err != nil {
			return false, err
		}
		return left || right, nil
	default:
		return false, fmt.Errorf("unsupported condition type: %T", expr)
	}
}

// evalComparison 计算比较表达式
func (e *Executor) evalComparison(row *storage.Row, expr *sqlparser.ComparisonExpr, schema *catalog.TableSchema) (bool, error) {
	// 获取左值（列）
	leftCol, ok := expr.Left.(*sqlparser.ColName)
	if !ok {
		return false, fmt.Errorf("left side of comparison must be a column")
	}

	colIndex := schema.GetColumnIndex(leftCol.Name.String())
	if colIndex == -1 {
		return false, fmt.Errorf("column not found: %s", leftCol.Name.String())
	}

	leftValue := row.Values[colIndex]

	// 获取右值
	rightValue, err := e.evalExpr(expr.Right, leftValue.Type)
	if err != nil {
		return false, err
	}

	// 执行比较
	return e.compareValues(leftValue, rightValue, expr.Operator)
}

// compareValues 比较两个值
func (e *Executor) compareValues(left, right types.Value, operator string) (bool, error) {
	if left.Type != right.Type {
		return false, fmt.Errorf("type mismatch in comparison")
	}

	switch left.Type {
	case types.TypeInt:
		leftInt, _ := left.AsInt()
		rightInt, _ := right.AsInt()
		return e.compareInts(leftInt, rightInt, operator), nil

	case types.TypeText:
		leftText, _ := left.AsText()
		rightText, _ := right.AsText()
		return e.compareStrings(leftText, rightText, operator), nil

	case types.TypeBoolean:
		leftBool, _ := left.AsBoolean()
		rightBool, _ := right.AsBoolean()
		return e.compareBools(leftBool, rightBool, operator), nil

	case types.TypeFloat:
		leftFloat, _ := left.AsFloat()
		rightFloat, _ := right.AsFloat()
		return e.compareFloats(leftFloat, rightFloat, operator), nil

	case types.TypeDate:
		leftDate, _ := left.AsDate()
		rightDate, _ := right.AsDate()
		return e.compareDates(leftDate.Unix(), rightDate.Unix(), operator), nil

	default:
		return false, fmt.Errorf("unsupported type for comparison: %s", left.Type)
	}
}

func (e *Executor) compareInts(left, right int64, operator string) bool {
	switch operator {
	case "=":
		return left == right
	case "!=", "<>":
		return left != right
	case "<":
		return left < right
	case "<=":
		return left <= right
	case ">":
		return left > right
	case ">=":
		return left >= right
	default:
		return false
	}
}

func (e *Executor) compareStrings(left, right, operator string) bool {
	switch operator {
	case "=":
		return left == right
	case "!=", "<>":
		return left != right
	case "<":
		return left < right
	case "<=":
		return left <= right
	case ">":
		return left > right
	case ">=":
		return left >= right
	default:
		return false
	}
}

func (e *Executor) compareBools(left, right bool, operator string) bool {
	switch operator {
	case "=":
		return left == right
	case "!=", "<>":
		return left != right
	default:
		return false
	}
}

func (e *Executor) compareFloats(left, right float64, operator string) bool {
	switch operator {
	case "=":
		return left == right
	case "!=", "<>":
		return left != right
	case "<":
		return left < right
	case "<=":
		return left <= right
	case ">":
		return left > right
	case ">=":
		return left >= right
	default:
		return false
	}
}

func (e *Executor) compareDates(left, right int64, operator string) bool {
	return e.compareInts(left, right, operator)
}

// formatResult 格式化查询结果
func (e *Executor) formatResult(rows []*storage.Row, schema *catalog.TableSchema, selectedColumns []int) string {
	var result strings.Builder

	// 表头
	headers := make([]string, len(selectedColumns))
	for i, colIdx := range selectedColumns {
		headers[i] = schema.Columns[colIdx].Name
	}
	result.WriteString(strings.Join(headers, "\t"))
	result.WriteString("\n")
	result.WriteString(strings.Repeat("-", len(headers)*15))
	result.WriteString("\n")

	// 数据行
	for _, row := range rows {
		values := make([]string, len(selectedColumns))
		for i, colIdx := range selectedColumns {
			values[i] = row.Values[colIdx].String()
		}
		result.WriteString(strings.Join(values, "\t"))
		result.WriteString("\n")
	}

	result.WriteString(fmt.Sprintf("\n%d row(s) returned", len(rows)))

	return result.String()
}
