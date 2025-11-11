package repl

import (
	"bufio"
	"fmt"
	"godb/executor"
	"io"
	"strings"
)

// REPL Read-Eval-Print Loop
type REPL struct {
	executor *executor.Executor
	reader   *bufio.Reader
}

// NewREPL 创建 REPL
func NewREPL(exec *executor.Executor, reader io.Reader) *REPL {
	return &REPL{
		executor: exec,
		reader:   bufio.NewReader(reader),
	}
}

// Start 启动 REPL
func (r *REPL) Start() {
	fmt.Println("Welcome to godb!")
	fmt.Println("Type SQL statements followed by Enter.")
	fmt.Println("Type 'exit' or 'quit' to exit.")
	fmt.Println()

	for {
		fmt.Print("godb> ")

		// 读取输入
		input, err := r.reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("\nGoodbye!")
				return
			}
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		// 去除前后空格和换行符
		input = strings.TrimSpace(input)

		// 检查是否是退出命令
		if input == "exit" || input == "quit" {
			fmt.Println("Goodbye!")
			return
		}

		// 跳过空行
		if input == "" {
			continue
		}

		// 执行 SQL
		result, err := r.executor.Execute(input)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Println(result)
		}
		fmt.Println()
	}
}
