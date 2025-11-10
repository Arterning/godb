package storage

import (
	"fmt"
	"godb/types"
)

// Row 表示一行数据
type Row struct {
	Values []types.Value
}

// Serialize 序列化行
func (r *Row) Serialize() ([]byte, error) {
	buf := make([]byte, 0)

	// 列数
	colCount := uint16(len(r.Values))
	colCountBuf := make([]byte, 2)
	colCountBuf[0] = byte(colCount & 0xFF)
	colCountBuf[1] = byte((colCount >> 8) & 0xFF)
	buf = append(buf, colCountBuf...)

	// 每列的值
	for _, val := range r.Values {
		valBuf, err := val.Serialize()
		if err != nil {
			return nil, err
		}
		buf = append(buf, valBuf...)
	}

	return buf, nil
}

// DeserializeRow 反序列化行
func DeserializeRow(data []byte, numColumns int) (*Row, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("data too short for row")
	}

	colCount := int(uint16(data[0]) | (uint16(data[1]) << 8))
	if colCount != numColumns {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", numColumns, colCount)
	}

	row := &Row{
		Values: make([]types.Value, colCount),
	}

	offset := 2
	for i := 0; i < colCount; i++ {
		val, bytesRead, err := types.Deserialize(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize column %d: %w", i, err)
		}
		row.Values[i] = val
		offset += bytesRead
	}

	return row, nil
}

// TableStorage 表存储
type TableStorage struct {
	pager       *Pager
	firstPageID uint32 // 第一个数据页的 ID
	numColumns  int    // 列数
}

// NewTableStorage 创建表存储
func NewTableStorage(pager *Pager, numColumns int) (*TableStorage, error) {
	// 分配第一个数据页
	firstPage, err := pager.AllocatePage(PageTypeTable)
	if err != nil {
		return nil, err
	}

	return &TableStorage{
		pager:       pager,
		firstPageID: firstPage.ID,
		numColumns:  numColumns,
	}, nil
}

// LoadTableStorage 加载已存在的表存储
func LoadTableStorage(pager *Pager, firstPageID uint32, numColumns int) *TableStorage {
	return &TableStorage{
		pager:       pager,
		firstPageID: firstPageID,
		numColumns:  numColumns,
	}
}

// InsertRow 插入行
func (t *TableStorage) InsertRow(row *Row) error {
	if len(row.Values) != t.numColumns {
		return fmt.Errorf("column count mismatch: expected %d, got %d", t.numColumns, len(row.Values))
	}

	// 序列化行
	rowData, err := row.Serialize()
	if err != nil {
		return err
	}

	// 找到可以插入的页
	currentPageID := t.firstPageID
	for {
		page, err := t.pager.GetPage(currentPageID)
		if err != nil {
			return err
		}

		// 尝试写入
		_, err = page.WriteRow(rowData)
		if err == nil {
			// 写入成功，刷新页
			return t.pager.FlushPage(currentPageID)
		}

		// 页已满，检查是否有下一页
		if page.NextPage == 0 {
			// 分配新页
			newPage, err := t.pager.AllocatePage(PageTypeTable)
			if err != nil {
				return err
			}
			page.NextPage = newPage.ID
			if err := t.pager.FlushPage(currentPageID); err != nil {
				return err
			}
			currentPageID = newPage.ID
		} else {
			currentPageID = page.NextPage
		}
	}
}

// GetAllRows 获取所有行
func (t *TableStorage) GetAllRows() ([]*Row, error) {
	rows := make([]*Row, 0)

	currentPageID := t.firstPageID
	for {
		page, err := t.pager.GetPage(currentPageID)
		if err != nil {
			return nil, err
		}

		// 读取页中所有行
		rowsData, err := page.GetAllRows()
		if err != nil {
			return nil, err
		}

		for _, rowData := range rowsData {
			row, err := DeserializeRow(rowData, t.numColumns)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}

		// 检查是否有下一页
		if page.NextPage == 0 {
			break
		}
		currentPageID = page.NextPage
	}

	return rows, nil
}

// GetFirstPageID 获取第一页 ID
func (t *TableStorage) GetFirstPageID() uint32 {
	return t.firstPageID
}
