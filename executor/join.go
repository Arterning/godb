package executor

import (
	"fmt"
	"godb/catalog"
	"godb/storage"
	"godb/types"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// JoinType JOIN 类型
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

// JoinedRow 连接后的行
type JoinedRow struct {
	LeftRow  *storage.Row // 左表行（可能为 nil）
	RightRow *storage.Row // 右表行（可能为 nil）
}

// JoinContext JOIN 上下文
type JoinContext struct {
	LeftTable  string
	RightTable string
	LeftSchema *catalog.TableSchema
	RightSchema *catalog.TableSchema
	JoinType   JoinType
	OnExpr     sqlparser.Expr // JOIN ON 条件
}

// executeJoin 执行 JOIN 查询
func (e *Executor) executeJoin(stmt *sqlparser.Select) (string, error) {
	// 解析 JOIN 信息
	joinCtx, err := e.parseJoinContext(stmt)
	if err != nil {
		return "", err
	}

	// 加载左表数据
	leftStorage, err := catalog.CreateTableStorage(e.pager, joinCtx.LeftSchema)
	if err != nil {
		return "", err
	}
	leftRows, err := leftStorage.GetAllRows()
	if err != nil {
		return "", err
	}

	// 加载右表数据
	rightStorage, err := catalog.CreateTableStorage(e.pager, joinCtx.RightSchema)
	if err != nil {
		return "", err
	}
	rightRows, err := rightStorage.GetAllRows()
	if err != nil {
		return "", err
	}

	// 执行 JOIN
	var joinedRows []*JoinedRow
	switch joinCtx.JoinType {
	case InnerJoin:
		joinedRows, err = e.innerJoin(leftRows, rightRows, joinCtx)
	case LeftJoin:
		joinedRows, err = e.leftJoin(leftRows, rightRows, joinCtx)
	case RightJoin:
		joinedRows, err = e.rightJoin(leftRows, rightRows, joinCtx)
	default:
		return "", fmt.Errorf("unsupported join type")
	}

	if err != nil {
		return "", err
	}

	// 应用 WHERE 过滤
	if stmt.Where != nil {
		joinedRows, err = e.filterJoinedRows(joinedRows, stmt.Where.Expr, joinCtx)
		if err != nil {
			return "", err
		}
	}

	// 选择要显示的列
	selectedColumns, err := e.getJoinedSelectedColumns(stmt.SelectExprs, joinCtx)
	if err != nil {
		return "", err
	}

	// 格式化输出
	return e.formatJoinedResult(joinedRows, selectedColumns, joinCtx), nil
}

// parseJoinContext 解析 JOIN 上下文
func (e *Executor) parseJoinContext(stmt *sqlparser.Select) (*JoinContext, error) {
	if len(stmt.From) != 1 {
		return nil, fmt.Errorf("expected exactly one FROM clause")
	}

	// 解析 JOIN 表达式
	joinExpr, ok := stmt.From[0].(*sqlparser.JoinTableExpr)
	if !ok {
		return nil, fmt.Errorf("not a JOIN expression")
	}

	// 获取左表名
	leftTableExpr, ok := joinExpr.LeftExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("invalid left table expression")
	}
	leftTableName := leftTableExpr.Expr.(sqlparser.TableName).Name.String()

	// 获取右表名
	rightTableExpr, ok := joinExpr.RightExpr.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, fmt.Errorf("invalid right table expression")
	}
	rightTableName := rightTableExpr.Expr.(sqlparser.TableName).Name.String()

	// 获取表定义
	leftSchema, err := e.catalog.GetTable(leftTableName)
	if err != nil {
		return nil, fmt.Errorf("left table not found: %w", err)
	}

	rightSchema, err := e.catalog.GetTable(rightTableName)
	if err != nil {
		return nil, fmt.Errorf("right table not found: %w", err)
	}

	// 确定 JOIN 类型
	var joinType JoinType
	switch strings.ToUpper(joinExpr.Join) {
	case "JOIN", "INNER JOIN":
		joinType = InnerJoin
	case "LEFT JOIN", "LEFT OUTER JOIN":
		joinType = LeftJoin
	case "RIGHT JOIN", "RIGHT OUTER JOIN":
		joinType = RightJoin
	default:
		return nil, fmt.Errorf("unsupported join type: %s", joinExpr.Join)
	}

	return &JoinContext{
		LeftTable:   leftTableName,
		RightTable:  rightTableName,
		LeftSchema:  leftSchema,
		RightSchema: rightSchema,
		JoinType:    joinType,
		OnExpr:      joinExpr.Condition.On,
	}, nil
}

// innerJoin 执行 INNER JOIN
func (e *Executor) innerJoin(leftRows, rightRows []*storage.Row, ctx *JoinContext) ([]*JoinedRow, error) {
	result := make([]*JoinedRow, 0)

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			match, err := e.evaluateJoinCondition(leftRow, rightRow, ctx)
			if err != nil {
				return nil, err
			}
			if match {
				result = append(result, &JoinedRow{
					LeftRow:  leftRow,
					RightRow: rightRow,
				})
			}
		}
	}

	return result, nil
}

// leftJoin 执行 LEFT JOIN
func (e *Executor) leftJoin(leftRows, rightRows []*storage.Row, ctx *JoinContext) ([]*JoinedRow, error) {
	result := make([]*JoinedRow, 0)

	for _, leftRow := range leftRows {
		matched := false
		for _, rightRow := range rightRows {
			match, err := e.evaluateJoinCondition(leftRow, rightRow, ctx)
			if err != nil {
				return nil, err
			}
			if match {
				result = append(result, &JoinedRow{
					LeftRow:  leftRow,
					RightRow: rightRow,
				})
				matched = true
			}
		}

		// 如果没有匹配，添加右表为 nil 的行
		if !matched {
			result = append(result, &JoinedRow{
				LeftRow:  leftRow,
				RightRow: nil,
			})
		}
	}

	return result, nil
}

// rightJoin 执行 RIGHT JOIN
func (e *Executor) rightJoin(leftRows, rightRows []*storage.Row, ctx *JoinContext) ([]*JoinedRow, error) {
	result := make([]*JoinedRow, 0)

	for _, rightRow := range rightRows {
		matched := false
		for _, leftRow := range leftRows {
			match, err := e.evaluateJoinCondition(leftRow, rightRow, ctx)
			if err != nil {
				return nil, err
			}
			if match {
				result = append(result, &JoinedRow{
					LeftRow:  leftRow,
					RightRow: rightRow,
				})
				matched = true
			}
		}

		// 如果没有匹配，添加左表为 nil 的行
		if !matched {
			result = append(result, &JoinedRow{
				LeftRow:  nil,
				RightRow: rightRow,
			})
		}
	}

	return result, nil
}

// evaluateJoinCondition 求值 JOIN 条件
func (e *Executor) evaluateJoinCondition(leftRow, rightRow *storage.Row, ctx *JoinContext) (bool, error) {
	if ctx.OnExpr == nil {
		return true, nil
	}

	// 解析 ON 条件（通常是比较表达式）
	compExpr, ok := ctx.OnExpr.(*sqlparser.ComparisonExpr)
	if !ok {
		return false, fmt.Errorf("unsupported ON condition type")
	}

	// 获取左右列
	leftCol, rightCol, err := e.parseJoinColumns(compExpr, ctx)
	if err != nil {
		return false, err
	}

	// 比较值
	return e.compareJoinValues(leftRow, rightRow, leftCol, rightCol, compExpr.Operator, ctx)
}

// parseJoinColumns 解析 JOIN 的列
func (e *Executor) parseJoinColumns(compExpr *sqlparser.ComparisonExpr, ctx *JoinContext) (leftColInfo, rightColInfo *columnInfo, err error) {
	leftColInfo, err = e.parseColumnInfo(compExpr.Left, ctx)
	if err != nil {
		return nil, nil, err
	}

	rightColInfo, err = e.parseColumnInfo(compExpr.Right, ctx)
	if err != nil {
		return nil, nil, err
	}

	return leftColInfo, rightColInfo, nil
}

type columnInfo struct {
	tableName  string
	columnName string
	colIndex   int
	isLeft     bool
}

// parseColumnInfo 解析列信息
func (e *Executor) parseColumnInfo(expr sqlparser.Expr, ctx *JoinContext) (*columnInfo, error) {
	colName, ok := expr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("expected column name in JOIN condition")
	}

	tableName := ""
	if !colName.Qualifier.IsEmpty() {
		tableName = colName.Qualifier.Name.String()
	}

	columnName := colName.Name.String()

	// 判断是左表还是右表的列
	if tableName == "" || tableName == ctx.LeftTable {
		colIndex := ctx.LeftSchema.GetColumnIndex(columnName)
		if colIndex != -1 {
			return &columnInfo{
				tableName:  ctx.LeftTable,
				columnName: columnName,
				colIndex:   colIndex,
				isLeft:     true,
			}, nil
		}
	}

	if tableName == "" || tableName == ctx.RightTable {
		colIndex := ctx.RightSchema.GetColumnIndex(columnName)
		if colIndex != -1 {
			return &columnInfo{
				tableName:  ctx.RightTable,
				columnName: columnName,
				colIndex:   colIndex,
				isLeft:     false,
			}, nil
		}
	}

	return nil, fmt.Errorf("column not found: %s", columnName)
}

// compareJoinValues 比较 JOIN 的值
func (e *Executor) compareJoinValues(leftRow, rightRow *storage.Row, leftCol, rightCol *columnInfo, operator string, ctx *JoinContext) (bool, error) {
	// 获取左值
	var leftValue types.Value
	if leftCol.isLeft {
		leftValue = leftRow.Values[leftCol.colIndex]
	} else {
		leftValue = rightRow.Values[leftCol.colIndex]
	}

	// 获取右值
	var rightValue types.Value
	if rightCol.isLeft {
		rightValue = leftRow.Values[rightCol.colIndex]
	} else {
		rightValue = rightRow.Values[rightCol.colIndex]
	}

	// 比较
	return e.compareValues(leftValue, rightValue, operator)
}

// filterJoinedRows 过滤连接后的行
func (e *Executor) filterJoinedRows(rows []*JoinedRow, whereExpr sqlparser.Expr, ctx *JoinContext) ([]*JoinedRow, error) {
	result := make([]*JoinedRow, 0)

	for _, row := range rows {
		match, err := e.evaluateJoinedRowCondition(row, whereExpr, ctx)
		if err != nil {
			return nil, err
		}
		if match {
			result = append(result, row)
		}
	}

	return result, nil
}

// evaluateJoinedRowCondition 求值连接行的条件
func (e *Executor) evaluateJoinedRowCondition(joinedRow *JoinedRow, expr sqlparser.Expr, ctx *JoinContext) (bool, error) {
	switch expr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return e.evalJoinedComparison(joinedRow, expr, ctx)
	case *sqlparser.AndExpr:
		left, err := e.evaluateJoinedRowCondition(joinedRow, expr.Left, ctx)
		if err != nil {
			return false, err
		}
		right, err := e.evaluateJoinedRowCondition(joinedRow, expr.Right, ctx)
		if err != nil {
			return false, err
		}
		return left && right, nil
	case *sqlparser.OrExpr:
		left, err := e.evaluateJoinedRowCondition(joinedRow, expr.Left, ctx)
		if err != nil {
			return false, err
		}
		right, err := e.evaluateJoinedRowCondition(joinedRow, expr.Right, ctx)
		if err != nil {
			return false, err
		}
		return left || right, nil
	default:
		return false, fmt.Errorf("unsupported condition type in WHERE: %T", expr)
	}
}

// evalJoinedComparison 求值连接行的比较
func (e *Executor) evalJoinedComparison(joinedRow *JoinedRow, expr *sqlparser.ComparisonExpr, ctx *JoinContext) (bool, error) {
	leftCol, err := e.parseColumnInfo(expr.Left, ctx)
	if err != nil {
		return false, err
	}

	// 获取左值
	var leftValue types.Value
	if leftCol.isLeft {
		if joinedRow.LeftRow == nil {
			return false, nil // NULL 值
		}
		leftValue = joinedRow.LeftRow.Values[leftCol.colIndex]
	} else {
		if joinedRow.RightRow == nil {
			return false, nil // NULL 值
		}
		leftValue = joinedRow.RightRow.Values[leftCol.colIndex]
	}

	// 获取右值
	rightValue, err := e.evalExpr(expr.Right, leftValue.Type)
	if err != nil {
		return false, err
	}

	// 比较
	return e.compareValues(leftValue, rightValue, expr.Operator)
}

// getJoinedSelectedColumns 获取连接后要显示的列
func (e *Executor) getJoinedSelectedColumns(selectExprs sqlparser.SelectExprs, ctx *JoinContext) ([]columnInfo, error) {
	result := make([]columnInfo, 0)

	// 检查是否是 SELECT *
	if len(selectExprs) == 1 {
		if _, ok := selectExprs[0].(*sqlparser.StarExpr); ok {
			// SELECT * - 返回所有列（左表 + 右表）
			for i, col := range ctx.LeftSchema.Columns {
				result = append(result, columnInfo{
					tableName:  ctx.LeftTable,
					columnName: col.Name,
					colIndex:   i,
					isLeft:     true,
				})
			}
			for i, col := range ctx.RightSchema.Columns {
				result = append(result, columnInfo{
					tableName:  ctx.RightTable,
					columnName: col.Name,
					colIndex:   i,
					isLeft:     false,
				})
			}
			return result, nil
		}
	}

	// 解析指定的列
	for _, expr := range selectExprs {
		aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported select expression")
		}

		colInfo, err := e.parseColumnInfo(aliasedExpr.Expr, ctx)
		if err != nil {
			return nil, err
		}

		result = append(result, *colInfo)
	}

	return result, nil
}

// formatJoinedResult 格式化连接结果
func (e *Executor) formatJoinedResult(rows []*JoinedRow, selectedColumns []columnInfo, ctx *JoinContext) string {
	var result strings.Builder

	// 表头
	headers := make([]string, len(selectedColumns))
	for i, col := range selectedColumns {
		headers[i] = fmt.Sprintf("%s.%s", col.tableName, col.columnName)
	}
	result.WriteString(strings.Join(headers, "\t"))
	result.WriteString("\n")
	result.WriteString(strings.Repeat("-", len(headers)*20))
	result.WriteString("\n")

	// 数据行
	for _, row := range rows {
		values := make([]string, len(selectedColumns))
		for i, col := range selectedColumns {
			if col.isLeft {
				if row.LeftRow != nil {
					values[i] = row.LeftRow.Values[col.colIndex].String()
				} else {
					values[i] = "NULL"
				}
			} else {
				if row.RightRow != nil {
					values[i] = row.RightRow.Values[col.colIndex].String()
				} else {
					values[i] = "NULL"
				}
			}
		}
		result.WriteString(strings.Join(values, "\t"))
		result.WriteString("\n")
	}

	result.WriteString(fmt.Sprintf("\n%d row(s) returned", len(rows)))

	return result.String()
}
