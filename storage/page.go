package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	PageSize     = 4096  // 页大小：4KB
	HeaderSize   = 16    // 页头大小
	MaxRowsPerPage = 100 // 每页最大行数（简化版本）
)

// PageType 页类型
type PageType uint8

const (
	PageTypeTable PageType = iota // 表数据页
	PageTypeMeta                   // 元数据页
)

// Page 数据页结构
type Page struct {
	ID       uint32   // 页 ID
	Type     PageType // 页类型
	RowCount uint16   // 当前行数
	NextPage uint32   // 下一页 ID（0 表示没有下一页）
	Data     []byte   // 实际数据（PageSize - HeaderSize）
}

// NewPage 创建新页
func NewPage(id uint32, pageType PageType) *Page {
	return &Page{
		ID:       id,
		Type:     pageType,
		RowCount: 0,
		NextPage: 0,
		Data:     make([]byte, PageSize-HeaderSize),
	}
}

// Serialize 序列化页到字节数组
func (p *Page) Serialize() []byte {
	buf := make([]byte, PageSize)

	// 页头
	binary.LittleEndian.PutUint32(buf[0:4], p.ID)
	buf[4] = byte(p.Type)
	binary.LittleEndian.PutUint16(buf[5:7], p.RowCount)
	binary.LittleEndian.PutUint32(buf[7:11], p.NextPage)

	// 页数据
	copy(buf[HeaderSize:], p.Data)

	return buf
}

// DeserializePage 从字节数组反序列化页
func DeserializePage(buf []byte) (*Page, error) {
	if len(buf) != PageSize {
		return nil, fmt.Errorf("invalid page size: %d", len(buf))
	}

	page := &Page{
		ID:       binary.LittleEndian.Uint32(buf[0:4]),
		Type:     PageType(buf[4]),
		RowCount: binary.LittleEndian.Uint16(buf[5:7]),
		NextPage: binary.LittleEndian.Uint32(buf[7:11]),
		Data:     make([]byte, PageSize-HeaderSize),
	}

	copy(page.Data, buf[HeaderSize:])

	return page, nil
}

// IsFull 检查页是否已满
func (p *Page) IsFull() bool {
	return p.RowCount >= MaxRowsPerPage
}

// WriteRow 写入行数据到页（返回写入的偏移量）
func (p *Page) WriteRow(rowData []byte) (int, error) {
	if p.IsFull() {
		return 0, fmt.Errorf("page is full")
	}

	// 计算当前偏移量（每行前 4 字节存储行数据长度）
	offset := 0
	for i := uint16(0); i < p.RowCount; i++ {
		rowLen := binary.LittleEndian.Uint32(p.Data[offset : offset+4])
		offset += 4 + int(rowLen)
	}

	// 检查是否有足够空间
	if offset+4+len(rowData) > len(p.Data) {
		return 0, fmt.Errorf("not enough space in page")
	}

	// 写入行长度
	binary.LittleEndian.PutUint32(p.Data[offset:offset+4], uint32(len(rowData)))
	offset += 4

	// 写入行数据
	copy(p.Data[offset:], rowData)

	p.RowCount++
	return offset - 4, nil
}

// ReadRow 读取指定索引的行数据
func (p *Page) ReadRow(index uint16) ([]byte, error) {
	if index >= p.RowCount {
		return nil, fmt.Errorf("row index out of range: %d", index)
	}

	offset := 0
	for i := uint16(0); i <= index; i++ {
		rowLen := binary.LittleEndian.Uint32(p.Data[offset : offset+4])
		if i == index {
			// 找到目标行
			return p.Data[offset+4 : offset+4+int(rowLen)], nil
		}
		offset += 4 + int(rowLen)
	}

	return nil, fmt.Errorf("failed to read row")
}

// GetAllRows 获取页中所有行数据
func (p *Page) GetAllRows() ([][]byte, error) {
	rows := make([][]byte, 0, p.RowCount)

	offset := 0
	for i := uint16(0); i < p.RowCount; i++ {
		if offset+4 > len(p.Data) {
			return nil, fmt.Errorf("corrupted page data")
		}

		rowLen := binary.LittleEndian.Uint32(p.Data[offset : offset+4])
		offset += 4

		if offset+int(rowLen) > len(p.Data) {
			return nil, fmt.Errorf("corrupted page data")
		}

		rowData := make([]byte, rowLen)
		copy(rowData, p.Data[offset:offset+int(rowLen)])
		rows = append(rows, rowData)

		offset += int(rowLen)
	}

	return rows, nil
}

// UpdateRow 更新指定索引的行数据（就地更新）
func (p *Page) UpdateRow(index uint16, newRowData []byte) error {
	if index >= p.RowCount {
		return fmt.Errorf("row index out of range: %d", index)
	}

	// 找到目标行的偏移量
	offset := 0
	for i := uint16(0); i < index; i++ {
		rowLen := binary.LittleEndian.Uint32(p.Data[offset : offset+4])
		offset += 4 + int(rowLen)
	}

	// 读取旧行长度
	oldRowLen := binary.LittleEndian.Uint32(p.Data[offset : offset+4])
	newRowLen := uint32(len(newRowData))

	// 检查新数据是否能放入同一位置
	// 简化实现：只有在新数据长度 <= 旧数据长度时才能就地更新
	if newRowLen > oldRowLen {
		return fmt.Errorf("new row data is larger than old row data, cannot update in place")
	}

	// 更新行长度
	binary.LittleEndian.PutUint32(p.Data[offset:offset+4], newRowLen)
	offset += 4

	// 更新行数据
	copy(p.Data[offset:offset+int(newRowLen)], newRowData)

	// 如果新数据更短，清空剩余空间（用 0 填充）
	if newRowLen < oldRowLen {
		for i := newRowLen; i < oldRowLen; i++ {
			p.Data[offset+int(i)] = 0
		}
	}

	return nil
}
