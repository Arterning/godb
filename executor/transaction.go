package executor

import (
	"fmt"
	"strings"
)

// executeBegin 开始事务
func (e *Executor) executeBegin() (string, error) {
	if e.currentTx != nil {
		return "", fmt.Errorf("transaction already in progress")
	}

	tx, err := e.txManager.Begin()
	if err != nil {
		return "", err
	}

	e.currentTx = tx
	return fmt.Sprintf("Transaction %d started", tx.ID), nil
}

// executeCommit 提交事务
func (e *Executor) executeCommit() (string, error) {
	if e.currentTx == nil {
		return "", fmt.Errorf("no active transaction")
	}

	txID := e.currentTx.ID
	if err := e.txManager.Commit(txID); err != nil {
		return "", err
	}

	e.currentTx = nil
	return fmt.Sprintf("Transaction %d committed", txID), nil
}

// executeRollback 回滚事务
func (e *Executor) executeRollback() (string, error) {
	if e.currentTx == nil {
		return "", fmt.Errorf("no active transaction")
	}

	txID := e.currentTx.ID
	if err := e.txManager.Rollback(txID); err != nil {
		return "", err
	}

	e.currentTx = nil
	return fmt.Sprintf("Transaction %d rolled back", txID), nil
}

// isTransactionCommand 检查是否是事务命令
func isTransactionCommand(sql string) bool {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))
	return sqlUpper == "BEGIN" ||
		sqlUpper == "BEGIN TRANSACTION" ||
		sqlUpper == "START TRANSACTION" ||
		sqlUpper == "COMMIT" ||
		sqlUpper == "ROLLBACK"
}

// executeTransactionCommand 执行事务命令
func (e *Executor) executeTransactionCommand(sql string) (string, error) {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	switch sqlUpper {
	case "BEGIN", "BEGIN TRANSACTION", "START TRANSACTION":
		return e.executeBegin()
	case "COMMIT":
		return e.executeCommit()
	case "ROLLBACK":
		return e.executeRollback()
	default:
		return "", fmt.Errorf("unknown transaction command: %s", sql)
	}
}

// getCurrentTxID 获取当前事务ID（如果没有活跃事务返回0表示自动提交）
func (e *Executor) getCurrentTxID() uint64 {
	if e.currentTx != nil {
		return uint64(e.currentTx.ID)
	}
	return 0 // 自动提交模式
}
