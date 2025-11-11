package index

import (
	"fmt"
	"godb/storage"
	"godb/types"
	"sync"
)

// IndexManager 索引管理器
type IndexManager struct {
	indexes map[string]*Index // 索引名 -> 索引
	mu      sync.RWMutex
}

// NewIndexManager 创建索引管理器
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*Index),
	}
}

// CreateIndex 创建索引
func (im *IndexManager) CreateIndex(name, tableName, columnName string, columnType types.DataType) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; exists {
		return fmt.Errorf("index already exists: %s", name)
	}

	idx := NewIndex(name, tableName, columnName, columnType)
	im.indexes[name] = idx

	return nil
}

// DropIndex 删除索引
func (im *IndexManager) DropIndex(name string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; !exists {
		return fmt.Errorf("index not found: %s", name)
	}

	delete(im.indexes, name)
	return nil
}

// GetIndex 获取索引
func (im *IndexManager) GetIndex(name string) (*Index, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	idx, exists := im.indexes[name]
	if !exists {
		return nil, fmt.Errorf("index not found: %s", name)
	}

	return idx, nil
}

// GetIndexesByTable 获取表的所有索引
func (im *IndexManager) GetIndexesByTable(tableName string) []*Index {
	im.mu.RLock()
	defer im.mu.RUnlock()

	result := make([]*Index, 0)
	for _, idx := range im.indexes {
		if idx.TableName == tableName {
			result = append(result, idx)
		}
	}

	return result
}

// GetIndexByColumn 获取指定表和列的索引
func (im *IndexManager) GetIndexByColumn(tableName, columnName string) *Index {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for _, idx := range im.indexes {
		if idx.TableName == tableName && idx.ColumnName == columnName {
			return idx
		}
	}

	return nil
}

// InsertEntry 向所有相关索引插入条目
func (im *IndexManager) InsertEntry(tableName string, row *storage.Row, columnNames []string) error {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for _, idx := range im.indexes {
		if idx.TableName != tableName {
			continue
		}

		// 找到对应的列
		colIndex := -1
		for i, colName := range columnNames {
			if colName == idx.ColumnName {
				colIndex = i
				break
			}
		}

		if colIndex == -1 {
			continue
		}

		// 插入索引
		if err := idx.Insert(row.Values[colIndex], row.ID); err != nil {
			return err
		}
	}

	return nil
}

// DeleteEntry 从所有相关索引删除条目
func (im *IndexManager) DeleteEntry(tableName string, row *storage.Row, columnNames []string) error {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for _, idx := range im.indexes {
		if idx.TableName != tableName {
			continue
		}

		// 找到对应的列
		colIndex := -1
		for i, colName := range columnNames {
			if colName == idx.ColumnName {
				colIndex = i
				break
			}
		}

		if colIndex == -1 {
			continue
		}

		// 删除索引
		if err := idx.Delete(row.Values[colIndex], row.ID); err != nil {
			return err
		}
	}

	return nil
}

// ListIndexes 列出所有索引
func (im *IndexManager) ListIndexes() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()

	names := make([]string, 0, len(im.indexes))
	for name := range im.indexes {
		names = append(names, name)
	}

	return names
}
