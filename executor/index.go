package executor

import (
	"fmt"
	"godb/catalog"
	"godb/storage"
	"regexp"
	"strings"
)

// CreateTableStorage 辅助函数
func CreateTableStorage(pager *storage.Pager, schema *catalog.TableSchema) (*storage.TableStorage, error) {
	return catalog.CreateTableStorage(pager, schema)
}

// executeCreateIndex 执行 CREATE INDEX
// 语法: CREATE INDEX index_name ON table_name (column_name)
func (e *Executor) executeCreateIndex(sql string) (string, error) {
	// 使用正则表达式解析 CREATE INDEX 语句
	// CREATE INDEX index_name ON table_name (column_name)
	pattern := `(?i)CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(\s*(\w+)\s*\)`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(sql)

	if len(matches) != 4 {
		return "", fmt.Errorf("invalid CREATE INDEX syntax, expected: CREATE INDEX index_name ON table_name (column_name)")
	}

	indexName := matches[1]
	tableName := matches[2]
	columnName := matches[3]

	// 在 catalog 中创建索引元数据
	if err := e.catalog.CreateIndex(indexName, tableName, columnName); err != nil {
		return "", err
	}

	// 获取表定义
	schema, err := e.catalog.GetTable(tableName)
	if err != nil {
		return "", err
	}

	// 获取列索引
	colIndex := schema.GetColumnIndex(columnName)
	if colIndex == -1 {
		return "", fmt.Errorf("column not found: %s", columnName)
	}

	// 在索引管理器中创建索引
	columnType := schema.Columns[colIndex].Type
	if err := e.indexManager.CreateIndex(indexName, tableName, columnName, columnType); err != nil {
		return "", err
	}

	// 构建索引：读取表中所有现有数据并插入索引
	tableStorage, err := CreateTableStorage(e.pager, schema)
	if err != nil {
		return "", err
	}

	rows, err := tableStorage.GetAllRows()
	if err != nil {
		return "", err
	}

	// 获取索引
	idx, err := e.indexManager.GetIndex(indexName)
	if err != nil {
		return "", err
	}

	// 为每一行插入索引条目
	for _, row := range rows {
		if err := idx.Insert(row.Values[colIndex], row.ID); err != nil {
			return "", fmt.Errorf("failed to build index: %w", err)
		}
	}

	return fmt.Sprintf("Index '%s' created successfully on %s(%s) with %d entries",
		indexName, tableName, columnName, len(rows)), nil
}

// executeDropIndex 执行 DROP INDEX
// 语法: DROP INDEX index_name
func (e *Executor) executeDropIndex(sql string) (string, error) {
	// 解析 DROP INDEX 语句
	pattern := `(?i)DROP\s+INDEX\s+(\w+)`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(sql)

	if len(matches) != 2 {
		return "", fmt.Errorf("invalid DROP INDEX syntax, expected: DROP INDEX index_name")
	}

	indexName := matches[1]

	// 从索引管理器中删除
	if err := e.indexManager.DropIndex(indexName); err != nil {
		return "", err
	}

	// 从 catalog 中删除
	if err := e.catalog.DropIndex(indexName); err != nil {
		return "", err
	}

	return fmt.Sprintf("Index '%s' dropped successfully", indexName), nil
}

// isCreateIndex 检查是否是 CREATE INDEX 语句
func isCreateIndex(sql string) bool {
	sql = strings.TrimSpace(strings.ToUpper(sql))
	return strings.HasPrefix(sql, "CREATE INDEX")
}

// isDropIndex 检查是否是 DROP INDEX 语句
func isDropIndex(sql string) bool {
	sql = strings.TrimSpace(strings.ToUpper(sql))
	return strings.HasPrefix(sql, "DROP INDEX")
}
