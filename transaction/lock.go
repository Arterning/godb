package transaction

import (
	"fmt"
	"sync"
	"time"
)

// LockType 锁类型
type LockType int

const (
	ReadLock  LockType = iota // 读锁（共享锁）
	WriteLock                 // 写锁（排他锁）
)

// TableLock 表锁
type TableLock struct {
	readers map[TransactionID]bool // 读锁持有者
	writer  TransactionID           // 写锁持有者（0表示无写锁）
	mu      sync.Mutex
}

// NewTableLock 创建表锁
func NewTableLock() *TableLock {
	return &TableLock{
		readers: make(map[TransactionID]bool),
		writer:  0,
	}
}

// LockManager 锁管理器
type LockManager struct {
	mu         sync.Mutex
	tableLocks map[string]*TableLock
	timeout    time.Duration // 锁超时时间
}

// NewLockManager 创建锁管理器
func NewLockManager() *LockManager {
	return &LockManager{
		tableLocks: make(map[string]*TableLock),
		timeout:    30 * time.Second, // 默认30秒超时
	}
}

// AcquireReadLock 获取读锁
func (lm *LockManager) AcquireReadLock(table string, txID TransactionID) error {
	lm.mu.Lock()
	lock, exists := lm.tableLocks[table]
	if !exists {
		lock = NewTableLock()
		lm.tableLocks[table] = lock
	}
	lm.mu.Unlock()

	// 尝试获取读锁（带超时）
	startTime := time.Now()
	for {
		lock.mu.Lock()
		// 如果没有写锁，或者写锁持有者就是当前事务，可以获取读锁
		if lock.writer == 0 || lock.writer == txID {
			lock.readers[txID] = true
			lock.mu.Unlock()
			return nil
		}
		lock.mu.Unlock()

		// 检查超时
		if time.Since(startTime) > lm.timeout {
			return fmt.Errorf("acquire read lock timeout for table %s", table)
		}

		// 等待一小段时间后重试
		time.Sleep(10 * time.Millisecond)
	}
}

// AcquireWriteLock 获取写锁
func (lm *LockManager) AcquireWriteLock(table string, txID TransactionID) error {
	lm.mu.Lock()
	lock, exists := lm.tableLocks[table]
	if !exists {
		lock = NewTableLock()
		lm.tableLocks[table] = lock
	}
	lm.mu.Unlock()

	// 尝试获取写锁（带超时）
	startTime := time.Now()
	for {
		lock.mu.Lock()
		// 如果没有写锁且没有其他读锁，或者所有锁都是当前事务持有的
		if lock.writer == 0 || lock.writer == txID {
			// 检查是否有其他事务的读锁
			hasOtherReaders := false
			for readerID := range lock.readers {
				if readerID != txID {
					hasOtherReaders = true
					break
				}
			}

			if !hasOtherReaders {
				lock.writer = txID
				lock.readers[txID] = true // 写锁也可以读
				lock.mu.Unlock()
				return nil
			}
		}
		lock.mu.Unlock()

		// 检查超时
		if time.Since(startTime) > lm.timeout {
			return fmt.Errorf("acquire write lock timeout for table %s", table)
		}

		// 等待一小段时间后重试
		time.Sleep(10 * time.Millisecond)
	}
}

// ReleaseLocks 释放事务持有的所有锁
func (lm *LockManager) ReleaseLocks(txID TransactionID) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for _, lock := range lm.tableLocks {
		lock.mu.Lock()
		// 释放读锁
		delete(lock.readers, txID)
		// 释放写锁
		if lock.writer == txID {
			lock.writer = 0
		}
		lock.mu.Unlock()
	}
}

// ReleaseTableLock 释放特定表的锁
func (lm *LockManager) ReleaseTableLock(table string, txID TransactionID) {
	lm.mu.Lock()
	lock, exists := lm.tableLocks[table]
	lm.mu.Unlock()

	if !exists {
		return
	}

	lock.mu.Lock()
	defer lock.mu.Unlock()

	delete(lock.readers, txID)
	if lock.writer == txID {
		lock.writer = 0
	}
}
