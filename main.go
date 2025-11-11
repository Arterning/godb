package main

import (
	"fmt"
	"godb/catalog"
	"godb/executor"
	"godb/index"
	"godb/repl"
	"godb/storage"
	"godb/transaction"
	"os"
)

func main() {
	// 数据库文件路径
	dbFile := "godb.db"
	metaFile := "godb_meta.json"

	// 创建或打开页管理器
	pager, err := storage.NewPager(dbFile)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer pager.Close()

	// 创建或加载元数据管理器
	catalogMgr, err := catalog.NewCatalog(metaFile)
	if err != nil {
		fmt.Printf("Failed to load catalog: %v\n", err)
		os.Exit(1)
	}

	// 创建索引管理器
	indexMgr := index.NewIndexManager()

	// 从 catalog 重建索引
	if err := rebuildIndexes(catalogMgr, indexMgr, pager); err != nil {
		fmt.Printf("Failed to rebuild indexes: %v\n", err)
		os.Exit(1)
	}

	// 创建事务管理器
	txMgr := transaction.NewTransactionManager(pager, catalogMgr)

	// 创建执行器
	exec := executor.NewExecutor(catalogMgr, pager, indexMgr, txMgr)

	// 启动 REPL
	r := repl.NewREPL(exec, os.Stdin)
	r.Start()
}

// rebuildIndexes 从 catalog 重建所有索引
func rebuildIndexes(catalogMgr *catalog.Catalog, indexMgr *index.IndexManager, pager *storage.Pager) error {
	// 获取所有索引信息
	indexNames := catalogMgr.ListIndexes()

	for _, indexName := range indexNames {
		indexInfo, err := catalogMgr.GetIndex(indexName)
		if err != nil {
			return fmt.Errorf("failed to get index %s: %w", indexName, err)
		}

		// 在索引管理器中创建索引
		if err := indexMgr.CreateIndex(indexInfo.Name, indexInfo.TableName, indexInfo.ColumnName, indexInfo.ColumnType); err != nil {
			return fmt.Errorf("failed to create index %s: %w", indexName, err)
		}

		// 获取表定义
		schema, err := catalogMgr.GetTable(indexInfo.TableName)
		if err != nil {
			return fmt.Errorf("failed to get table %s: %w", indexInfo.TableName, err)
		}

		// 获取列索引
		colIndex := schema.GetColumnIndex(indexInfo.ColumnName)
		if colIndex == -1 {
			return fmt.Errorf("column not found: %s", indexInfo.ColumnName)
		}

		// 加载表数据
		tableStorage, err := catalog.CreateTableStorage(pager, schema)
		if err != nil {
			return fmt.Errorf("failed to create table storage: %w", err)
		}

		rows, err := tableStorage.GetAllRows()
		if err != nil {
			return fmt.Errorf("failed to get rows: %w", err)
		}

		// 获取索引
		idx, err := indexMgr.GetIndex(indexInfo.Name)
		if err != nil {
			return fmt.Errorf("failed to get index: %w", err)
		}

		// 为每一行插入索引条目
		for _, row := range rows {
			if err := idx.Insert(row.Values[colIndex], row.ID); err != nil {
				return fmt.Errorf("failed to insert index entry: %w", err)
			}
		}

		fmt.Printf("Rebuilt index '%s' with %d entries\n", indexName, len(rows))
	}

	return nil
}
