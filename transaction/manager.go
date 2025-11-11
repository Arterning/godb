package transaction

import (
	"fmt"
	"godb/catalog"
	"godb/storage"
	"sync"
	"sync/atomic"
	"time"
)

// TransactionManager 事务管理器
type TransactionManager struct {
	mu          sync.RWMutex
	nextTxID    TransactionID
	activeTxs   map[TransactionID]*Transaction
	lockManager *LockManager
	pager       *storage.Pager
	catalog     *catalog.Catalog
}

// NewTransactionManager 创建事务管理器
func NewTransactionManager(pager *storage.Pager, catalog *catalog.Catalog) *TransactionManager {
	return &TransactionManager{
		nextTxID:    1, // 事务ID从1开始，0表示自动提交
		activeTxs:   make(map[TransactionID]*Transaction),
		lockManager: NewLockManager(),
		pager:       pager,
		catalog:     catalog,
	}
}

// Begin 开始新事务
func (tm *TransactionManager) Begin() (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txID := TransactionID(atomic.AddUint64((*uint64)(&tm.nextTxID), 1) - 1)
	tx := &Transaction{
		ID:         txID,
		Status:     TxActive,
		StartTime:  time.Now(),
		Operations: make([]*Operation, 0),
	}

	tm.activeTxs[txID] = tx
	return tx, nil
}

// GetTransaction 获取事务
func (tm *TransactionManager) GetTransaction(txID TransactionID) (*Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tx, exists := tm.activeTxs[txID]
	if !exists {
		return nil, fmt.Errorf("transaction %d not found", txID)
	}
	return tx, nil
}

// Commit 提交事务
func (tm *TransactionManager) Commit(txID TransactionID) error {
	tm.mu.Lock()
	tx, exists := tm.activeTxs[txID]
	if !exists {
		tm.mu.Unlock()
		return fmt.Errorf("transaction %d not found", txID)
	}

	// 标记为已提交
	tx.Status = TxCommitted
	delete(tm.activeTxs, txID)
	tm.mu.Unlock()

	// 释放所有锁
	tm.lockManager.ReleaseLocks(txID)

	// 刷新所有脏页到磁盘（确保持久性）
	if err := tm.pager.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush pages: %w", err)
	}

	return nil
}

// Rollback 回滚事务
func (tm *TransactionManager) Rollback(txID TransactionID) error {
	tm.mu.Lock()
	tx, exists := tm.activeTxs[txID]
	if !exists {
		tm.mu.Unlock()
		return fmt.Errorf("transaction %d not found", txID)
	}

	// 标记为已中止
	tx.Status = TxAborted
	delete(tm.activeTxs, txID)
	tm.mu.Unlock()

	// 回滚所有操作（逆序执行）
	operations := tx.GetOperations()
	for i := len(operations) - 1; i >= 0; i-- {
		op := operations[i]
		if err := tm.rollbackOperation(op); err != nil {
			// 记录错误但继续回滚其他操作
			fmt.Printf("Warning: failed to rollback operation: %v\n", err)
		}
	}

	// 释放所有锁
	tm.lockManager.ReleaseLocks(txID)

	// 刷新页到磁盘（确保回滚操作持久化）
	if err := tm.pager.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush pages after rollback: %w", err)
	}

	return nil
}

// rollbackOperation 回滚单个操作
func (tm *TransactionManager) rollbackOperation(op *Operation) error {
	schema, err := tm.catalog.GetTable(op.TableName)
	if err != nil {
		return err
	}

	tableStorage, err := catalog.CreateTableStorage(tm.pager, schema)
	if err != nil {
		return err
	}

	switch op.Type {
	case OpInsert:
		// 回滚 INSERT：标记行为删除
		return tableStorage.MarkRowDeleted(op.RowID)

	case OpUpdate:
		// 回滚 UPDATE：恢复旧数据
		if op.OldData == nil {
			return fmt.Errorf("no old data for rollback")
		}
		// 标记新行为删除
		if err := tableStorage.MarkRowDeleted(op.NewData.ID); err != nil {
			return err
		}
		// 恢复旧行（取消删除标记）
		return tm.unmarkRowDeleted(tableStorage, op.OldData.ID)

	case OpDelete:
		// 回滚 DELETE：恢复删除标记
		return tm.unmarkRowDeleted(tableStorage, op.RowID)

	default:
		return fmt.Errorf("unknown operation type: %d", op.Type)
	}
}

// unmarkRowDeleted 取消行的删除标记
func (tm *TransactionManager) unmarkRowDeleted(tableStorage *storage.TableStorage, rowID storage.RowID) error {
	page, err := tm.pager.GetPage(rowID.PageID)
	if err != nil {
		return err
	}

	rowData, err := page.ReadRow(rowID.RowIndex)
	if err != nil {
		return err
	}

	row, err := storage.DeserializeRow(rowData, tableStorage.GetNumColumns())
	if err != nil {
		return err
	}

	// 取消删除标记
	row.Deleted = false
	newRowData, err := row.Serialize()
	if err != nil {
		return err
	}

	return page.UpdateRow(rowID.RowIndex, newRowData)
}

// GetLockManager 获取锁管理器
func (tm *TransactionManager) GetLockManager() *LockManager {
	return tm.lockManager
}

// IsCommitted 检查事务是否已提交
func (tm *TransactionManager) IsCommitted(txID TransactionID) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tx, exists := tm.activeTxs[txID]
	if !exists {
		// 不在活跃事务列表中，可能已提交或已中止
		// 根据 READ COMMITTED，我们假设已提交
		return true
	}
	return tx.Status == TxCommitted
}

// GetActiveTransactions 获取所有活跃事务（用于可见性判断）
func (tm *TransactionManager) GetActiveTransactions() []TransactionID {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	result := make([]TransactionID, 0, len(tm.activeTxs))
	for txID := range tm.activeTxs {
		result = append(result, txID)
	}
	return result
}
