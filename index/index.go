package index

import (
	"fmt"
	"godb/storage"
	"godb/types"
	"sync"

	"github.com/google/btree"
)

// IndexEntry B-Tree 索引条目
type IndexEntry struct {
	Key   types.Value    // 索引键值
	RowID storage.RowID  // 行 ID
}

// Less 实现 btree.Item 接口
func (e IndexEntry) Less(than btree.Item) bool {
	other := than.(IndexEntry)

	// 比较键值
	switch e.Key.Type {
	case types.TypeInt:
		leftInt, _ := e.Key.AsInt()
		rightInt, _ := other.Key.AsInt()
		if leftInt != rightInt {
			return leftInt < rightInt
		}
	case types.TypeText:
		leftText, _ := e.Key.AsText()
		rightText, _ := other.Key.AsText()
		if leftText != rightText {
			return leftText < rightText
		}
	case types.TypeFloat:
		leftFloat, _ := e.Key.AsFloat()
		rightFloat, _ := other.Key.AsFloat()
		if leftFloat != rightFloat {
			return leftFloat < rightFloat
		}
	case types.TypeDate:
		leftDate, _ := e.Key.AsDate()
		rightDate, _ := other.Key.AsDate()
		if !leftDate.Equal(rightDate) {
			return leftDate.Before(rightDate)
		}
	case types.TypeBoolean:
		leftBool, _ := e.Key.AsBoolean()
		rightBool, _ := other.Key.AsBoolean()
		if leftBool != rightBool {
			return !leftBool && rightBool
		}
	}

	// 如果键值相等，比较 RowID（确保唯一性）
	if e.RowID.PageID != other.RowID.PageID {
		return e.RowID.PageID < other.RowID.PageID
	}
	return e.RowID.RowIndex < other.RowID.RowIndex
}

// Index B-Tree 索引
type Index struct {
	Name       string            // 索引名称
	TableName  string            // 表名
	ColumnName string            // 列名
	ColumnType types.DataType    // 列类型
	tree       *btree.BTree      // B-Tree
	mu         sync.RWMutex
}

// NewIndex 创建新索引
func NewIndex(name, tableName, columnName string, columnType types.DataType) *Index {
	return &Index{
		Name:       name,
		TableName:  tableName,
		ColumnName: columnName,
		ColumnType: columnType,
		tree:       btree.New(32), // 度数为 32
	}
}

// Insert 插入索引条目
func (idx *Index) Insert(key types.Value, rowID storage.RowID) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if key.Type != idx.ColumnType {
		return fmt.Errorf("key type mismatch: expected %s, got %s", idx.ColumnType, key.Type)
	}

	entry := IndexEntry{
		Key:   key,
		RowID: rowID,
	}

	idx.tree.ReplaceOrInsert(entry)
	return nil
}

// Delete 删除索引条目
func (idx *Index) Delete(key types.Value, rowID storage.RowID) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	entry := IndexEntry{
		Key:   key,
		RowID: rowID,
	}

	idx.tree.Delete(entry)
	return nil
}

// Search 等值查询
func (idx *Index) Search(key types.Value) ([]storage.RowID, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if key.Type != idx.ColumnType {
		return nil, fmt.Errorf("key type mismatch: expected %s, got %s", idx.ColumnType, key.Type)
	}

	result := make([]storage.RowID, 0)

	// 创建查找的最小条目
	searchEntry := IndexEntry{
		Key:   key,
		RowID: storage.RowID{PageID: 0, RowIndex: 0},
	}

	// 使用 AscendGreaterOrEqual 查找所有匹配的条目
	idx.tree.AscendGreaterOrEqual(searchEntry, func(item btree.Item) bool {
		entry := item.(IndexEntry)

		// 检查键是否相等
		if !valuesEqual(entry.Key, key) {
			return false // 停止迭代
		}

		result = append(result, entry.RowID)
		return true // 继续迭代
	})

	return result, nil
}

// RangeSearch 范围查询
// operator: "<", "<=", ">", ">="
func (idx *Index) RangeSearch(operator string, key types.Value) ([]storage.RowID, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if key.Type != idx.ColumnType {
		return nil, fmt.Errorf("key type mismatch: expected %s, got %s", idx.ColumnType, key.Type)
	}

	result := make([]storage.RowID, 0)

	searchEntry := IndexEntry{
		Key:   key,
		RowID: storage.RowID{PageID: 0, RowIndex: 0},
	}

	switch operator {
	case "<":
		// 从最小值开始，到 key 之前
		idx.tree.Ascend(func(item btree.Item) bool {
			entry := item.(IndexEntry)
			if compareValues(entry.Key, key) < 0 {
				result = append(result, entry.RowID)
				return true
			}
			return false
		})

	case "<=":
		// 从最小值开始，到 key（包含）
		idx.tree.Ascend(func(item btree.Item) bool {
			entry := item.(IndexEntry)
			cmp := compareValues(entry.Key, key)
			if cmp < 0 || cmp == 0 {
				result = append(result, entry.RowID)
				return true
			}
			return false
		})

	case ">":
		// 从 key 之后开始，到最大值
		idx.tree.AscendGreaterOrEqual(searchEntry, func(item btree.Item) bool {
			entry := item.(IndexEntry)
			if compareValues(entry.Key, key) > 0 {
				result = append(result, entry.RowID)
			}
			return true
		})

	case ">=":
		// 从 key（包含）开始，到最大值
		idx.tree.AscendGreaterOrEqual(searchEntry, func(item btree.Item) bool {
			entry := item.(IndexEntry)
			result = append(result, entry.RowID)
			return true
		})

	default:
		return nil, fmt.Errorf("unsupported operator: %s", operator)
	}

	return result, nil
}

// GetCount 获取索引条目数量
func (idx *Index) GetCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.tree.Len()
}

// valuesEqual 判断两个值是否相等
func valuesEqual(v1, v2 types.Value) bool {
	return compareValues(v1, v2) == 0
}

// compareValues 比较两个值
// 返回：-1 (v1 < v2), 0 (v1 == v2), 1 (v1 > v2)
func compareValues(v1, v2 types.Value) int {
	if v1.Type != v2.Type {
		return 0
	}

	switch v1.Type {
	case types.TypeInt:
		left, _ := v1.AsInt()
		right, _ := v2.AsInt()
		if left < right {
			return -1
		} else if left > right {
			return 1
		}
		return 0

	case types.TypeText:
		left, _ := v1.AsText()
		right, _ := v2.AsText()
		if left < right {
			return -1
		} else if left > right {
			return 1
		}
		return 0

	case types.TypeFloat:
		left, _ := v1.AsFloat()
		right, _ := v2.AsFloat()
		if left < right {
			return -1
		} else if left > right {
			return 1
		}
		return 0

	case types.TypeDate:
		left, _ := v1.AsDate()
		right, _ := v2.AsDate()
		if left.Before(right) {
			return -1
		} else if left.After(right) {
			return 1
		}
		return 0

	case types.TypeBoolean:
		left, _ := v1.AsBoolean()
		right, _ := v2.AsBoolean()
		if !left && right {
			return -1
		} else if left && !right {
			return 1
		}
		return 0
	}

	return 0
}
