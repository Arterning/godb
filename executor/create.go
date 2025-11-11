package executor

import (
	"fmt"
	"godb/catalog"
	"godb/storage"
	"strings"

	"github.com/xwb1989/sqlparser"
)

// executeCreateTable 执行 CREATE TABLE
func (e *Executor) executeCreateTable(stmt *sqlparser.DDL) (string, error) {
	tableName := stmt.NewName.Name.String()

	// 检查 TableSpec 是否存在
	if stmt.TableSpec == nil {
		return "", fmt.Errorf("invalid CREATE TABLE statement: missing table specification")
	}

	// 解析列定义
	columns := make([]catalog.Column, 0)
	for _, colDef := range stmt.TableSpec.Columns {
		colName := colDef.Name.String()
		colTypeStr := strings.ToUpper(colDef.Type.Type)

		// 解析数据类型
		dataType, err := catalog.ParseDataType(colTypeStr)
		if err != nil {
			return "", fmt.Errorf("unsupported column type: %s", colTypeStr)
		}

		columns = append(columns, catalog.Column{
			Name: colName,
			Type: dataType,
		})
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("table must have at least one column")
	}

	// 创建表存储
	tableStorage, err := storage.NewTableStorage(e.pager, len(columns))
	if err != nil {
		return "", fmt.Errorf("failed to create table storage: %w", err)
	}

	// 在 catalog 中创建表
	err = e.catalog.CreateTable(tableName, columns, tableStorage.GetFirstPageID())
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Table '%s' created successfully", tableName), nil
}

// executeDropTable 执行 DROP TABLE
func (e *Executor) executeDropTable(stmt *sqlparser.DDL) (string, error) {
	tableName := stmt.Table.Name.String()

	err := e.catalog.DropTable(tableName)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Table '%s' dropped successfully", tableName), nil
}
