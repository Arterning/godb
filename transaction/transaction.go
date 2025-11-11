package transaction

import (
	"godb/storage"
	"sync"
	"time"
)

// TransactionID 事务ID
type TransactionID uint64

// TransactionStatus 事务状态
type TransactionStatus int

const (
	TxActive TransactionStatus = iota // 活跃
	TxCommitted                        // 已提交
	TxAborted                          // 已中止
)

// OperationType 操作类型
type OperationType int

const (
	OpInsert OperationType = iota
	OpUpdate
	OpDelete
)

// Operation 事务操作记录（用于回滚）
type Operation struct {
	Type      OperationType
	TableName string
	RowID     storage.RowID
	OldData   *storage.Row // 用于回滚 UPDATE/DELETE
	NewData   *storage.Row // INSERT/UPDATE 的新数据
}

// Transaction 事务
type Transaction struct {
	ID         TransactionID
	Status     TransactionStatus
	StartTime  time.Time
	Operations []*Operation
	mu         sync.Mutex
}

// AddOperation 添加操作到事务日志
func (tx *Transaction) AddOperation(op *Operation) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.Operations = append(tx.Operations, op)
}

// GetOperations 获取所有操作（用于回滚）
func (tx *Transaction) GetOperations() []*Operation {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.Operations
}
