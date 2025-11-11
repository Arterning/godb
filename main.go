package main

import (
	"fmt"
	"godb/catalog"
	"godb/executor"
	"godb/repl"
	"godb/storage"
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

	// 创建执行器
	exec := executor.NewExecutor(catalogMgr, pager)

	// 启动 REPL
	r := repl.NewREPL(exec, os.Stdin)
	r.Start()
}
