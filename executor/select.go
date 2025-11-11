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
	// 检查是否是 JOIN 查询
	if len(stmt.From) == 1 {
		if _, isJoin := stmt.From[0].(*sqlparser.JoinTableExpr); isJoin {
			return e.executeJoin(stmt)
		}
	}

	// 单表查询
	if len(stmt.From) != 1 {
		return "", fmt.Errorf("only single table select is supported")
	}

	aliasedTable, ok := stmt.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", fmt.Errorf("invalid FROM clause")
	}

	tableName := aliasedTable.Expr.(sqlparser.TableName).Name.String()

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

	var filteredRows []*storage.Row

	// 尝试使用索引查询
	if stmt.Where != nil {
		indexRows, used, err := e.tryIndexScan(tableName, stmt.Where.Expr, schema, tableStorage)
		if err != nil {
			return "", err
		}
		if used {
			// 成功使用索引
			filteredRows = indexRows
		} else {
			// 回退到全表扫描
			rows, err := tableStorage.GetAllRows()
			if err != nil {
				return "", err
			}
			filteredRows, err = e.filterRows(rows, stmt.Where.Expr, schema)
			if err != nil {
				return "", err
			}
		}
	} else {
		// 没有 WHERE 条件，全表扫描
		rows, err := tableStorage.GetAllRows()
		if err != nil {
			return "", err
		}
		filteredRows = rows
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

// tryIndexScan 尝试使用索引扫描
// 返回: (结果行, 是否使用了索引, 错误)
func (e *Executor) tryIndexScan(tableName string, whereExpr sqlparser.Expr, schema *catalog.TableSchema, tableStorage *storage.TableStorage) ([]*storage.Row, bool, error) {
	// 检查是否是简单的比较表达式
	compExpr, ok := whereExpr.(*sqlparser.ComparisonExpr)
	if !ok {
		// 不是简单比较，无法使用索引
		return nil, false, nil
	}

	// 获取列名
	colName, ok := compExpr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, false, nil
	}

	columnName := colName.Name.String()
	operator := compExpr.Operator

	// 检查该列是否有索引
	idx := e.indexManager.GetIndexByColumn(tableName, columnName)
	if idx == nil {
		// 没有索引
		return nil, false, nil
	}

	// 获取比较值
	colIndex := schema.GetColumnIndex(columnName)
	if colIndex == -1 {
		return nil, false, fmt.Errorf("column not found: %s", columnName)
	}

	colType := schema.Columns[colIndex].Type
	value, err := e.evalExpr(compExpr.Right, colType)
	if err != nil {
		return nil, false, err
	}

	// 使用索引查询
	var rowIDs []storage.RowID
	switch operator {
	case "=":
		rowIDs, err = idx.Search(value)
	case "<", "<=", ">", ">=":
		rowIDs, err = idx.RangeSearch(operator, value)
	default:
		// 不支持的操作符，回退到全表扫描
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	// 根据 RowID 获取行数据
	rows, err := e.getRowsByIDs(tableStorage, rowIDs)
	if err != nil {
		return nil, false, err
	}

	return rows, true, nil
}

// getRowsByIDs 根据 RowID 列表获取行数据
func (e *Executor) getRowsByIDs(tableStorage *storage.TableStorage, rowIDs []storage.RowID) ([]*storage.Row, error) {
	rows := make([]*storage.Row, 0, len(rowIDs))

	for _, rowID := range rowIDs {
		row, err := e.getRowByID(tableStorage, rowID)
		if err != nil {
			return nil, err
		}
		if row != nil && !row.Deleted {
			rows = append(rows, row)
		}
	}

	return rows, nil
}

// getRowByID 根据 RowID 获取单行数据
func (e *Executor) getRowByID(tableStorage *storage.TableStorage, rowID storage.RowID) (*storage.Row, error) {
	page, err := tableStorage.GetPager().GetPage(rowID.PageID)
	if err != nil {
		return nil, err
	}

	rowData, err := page.ReadRow(rowID.RowIndex)
	if err != nil {
		return nil, err
	}

	row, err := storage.DeserializeRow(rowData, tableStorage.GetNumColumns())
	if err != nil {
		return nil, err
	}

	row.ID = rowID
	return row, nil
}
