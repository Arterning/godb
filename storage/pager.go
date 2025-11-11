package storage

import (
	"fmt"
	"os"
	"sync"
)

// Pager 页管理器
type Pager struct {
	file      *os.File
	numPages  uint32
	pageCache map[uint32]*Page // 简单的页缓存
	mu        sync.RWMutex
}

// NewPager 创建页管理器
func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// 获取文件大小，计算页数
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	numPages := uint32(fileInfo.Size() / PageSize)

	return &Pager{
		file:      file,
		numPages:  numPages,
		pageCache: make(map[uint32]*Page),
	}, nil
}

// Close 关闭页管理器
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 刷新所有缓存页
	for _, page := range p.pageCache {
		if err := p.writePageToDisk(page); err != nil {
			return err
		}
	}

	return p.file.Close()
}

// GetPage 获取页（从缓存或磁盘）
func (p *Pager) GetPage(pageID uint32) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 检查缓存
	if page, ok := p.pageCache[pageID]; ok {
		return page, nil
	}

	// 从磁盘读取
	if pageID >= p.numPages {
		return nil, fmt.Errorf("page ID out of range: %d", pageID)
	}

	buf := make([]byte, PageSize)
	offset := int64(pageID) * PageSize

	n, err := p.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page: %w", err)
	}
	if n != PageSize {
		return nil, fmt.Errorf("incomplete page read")
	}

	page, err := DeserializePage(buf)
	if err != nil {
		return nil, err
	}

	// 加入缓存
	p.pageCache[pageID] = page

	return page, nil
}

// AllocatePage 分配新页
func (p *Pager) AllocatePage(pageType PageType) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pageID := p.numPages
	page := NewPage(pageID, pageType)

	// 写入磁盘
	if err := p.writePageToDisk(page); err != nil {
		return nil, err
	}

	// 加入缓存
	p.pageCache[pageID] = page
	p.numPages++

	return page, nil
}

// FlushPage 刷新页到磁盘
func (p *Pager) FlushPage(pageID uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.pageCache[pageID]
	if !ok {
		return fmt.Errorf("page not in cache: %d", pageID)
	}

	return p.writePageToDisk(page)
}

// FlushAll 刷新所有缓存页到磁盘
func (p *Pager) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, page := range p.pageCache {
		if err := p.writePageToDisk(page); err != nil {
			return err
		}
	}

	// 同步文件到磁盘
	return p.file.Sync()
}

// writePageToDisk 写入页到磁盘（内部方法，需要调用者持有锁）
func (p *Pager) writePageToDisk(page *Page) error {
	buf := page.Serialize()
	offset := int64(page.ID) * PageSize

	n, err := p.file.WriteAt(buf, offset)
	if err != nil {
		return fmt.Errorf("failed to write page: %w", err)
	}
	if n != PageSize {
		return fmt.Errorf("incomplete page write")
	}

	return nil
}

// GetNumPages 获取页数
func (p *Pager) GetNumPages() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numPages
}
