package catalog

import (
	"encoding/json"
	"fmt"
	"godb/storage"
	"godb/types"
	"os"
	"sync"
)

// Column 列定义
type Column struct {
	Name string          // 列名
	Type types.DataType  // 数据类型
}

// TableSchema 表定义
type TableSchema struct {
	Name        string    // 表名
	Columns     []Column  // 列定义
	FirstPageID uint32    // 第一个数据页 ID
}

// GetColumnIndex 获取列索引
func (t *TableSchema) GetColumnIndex(columnName string) int {
	for i, col := range t.Columns {
		if col.Name == columnName {
			return i
		}
	}
	return -1
}

// GetColumnType 获取列类型
func (t *TableSchema) GetColumnType(columnName string) (types.DataType, error) {
	idx := t.GetColumnIndex(columnName)
	if idx == -1 {
		return 0, fmt.Errorf("column not found: %s", columnName)
	}
	return t.Columns[idx].Type, nil
}

// Catalog 元数据管理器
type Catalog struct {
	tables   map[string]*TableSchema // 表名 -> 表定义
	mu       sync.RWMutex
	metaFile string // 元数据文件路径
}

// NewCatalog 创建元数据管理器
func NewCatalog(metaFile string) (*Catalog, error) {
	catalog := &Catalog{
		tables:   make(map[string]*TableSchema),
		metaFile: metaFile,
	}

	// 加载元数据
	if err := catalog.Load(); err != nil {
		// 如果文件不存在，创建新的
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	return catalog, nil
}

// CreateTable 创建表
func (c *Catalog) CreateTable(name string, columns []Column, firstPageID uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 检查表是否已存在
	if _, exists := c.tables[name]; exists {
		return fmt.Errorf("table already exists: %s", name)
	}

	// 创建表定义
	schema := &TableSchema{
		Name:        name,
		Columns:     columns,
		FirstPageID: firstPageID,
	}

	c.tables[name] = schema

	// 持久化
	return c.save()
}

// GetTable 获取表定义
func (c *Catalog) GetTable(name string) (*TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	schema, exists := c.tables[name]
	if !exists {
		return nil, fmt.Errorf("table not found: %s", name)
	}

	return schema, nil
}

// DropTable 删除表
func (c *Catalog) DropTable(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[name]; !exists {
		return fmt.Errorf("table not found: %s", name)
	}

	delete(c.tables, name)

	// 持久化
	return c.save()
}

// ListTables 列出所有表
func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	return names
}

// save 保存元数据到文件（内部方法，需要调用者持有锁）
func (c *Catalog) save() error {
	data, err := json.MarshalIndent(c.tables, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal catalog: %w", err)
	}

	if err := os.WriteFile(c.metaFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write catalog file: %w", err)
	}

	return nil
}

// Load 从文件加载元数据
func (c *Catalog) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(c.metaFile)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, &c.tables); err != nil {
		return fmt.Errorf("failed to unmarshal catalog: %w", err)
	}

	return nil
}

// ParseDataType 从字符串解析数据类型
func ParseDataType(typeStr string) (types.DataType, error) {
	switch typeStr {
	case "INT", "INTEGER", "BIGINT":
		return types.TypeInt, nil
	case "TEXT", "VARCHAR", "CHAR", "STRING":
		return types.TypeText, nil
	case "BOOLEAN", "BOOL", "TINYINT":
		return types.TypeBoolean, nil
	case "FLOAT", "DOUBLE", "REAL":
		return types.TypeFloat, nil
	case "DATE", "DATETIME", "TIMESTAMP":
		return types.TypeDate, nil
	default:
		return 0, fmt.Errorf("unsupported data type: %s", typeStr)
	}
}

// CreateTableStorage 为表创建存储
func CreateTableStorage(pager *storage.Pager, schema *TableSchema) (*storage.TableStorage, error) {
	return storage.LoadTableStorage(pager, schema.FirstPageID, len(schema.Columns)), nil
}
