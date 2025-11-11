# godb - 用 Go 实现的简易数据库

一个类似 SQLite 的轻量级数据库，支持基础 SQL 语法和磁盘持久化。

## 功能特性

### 支持的数据类型
- **INT / INTEGER / BIGINT**: 整数类型（64位）
- **TEXT / VARCHAR / CHAR / STRING**: 文本类型
- **TINYINT / BOOL / BOOLEAN**: 布尔类型
- **FLOAT / DOUBLE / REAL**: 浮点数类型（64位）
- **DATE / DATETIME / TIMESTAMP**: 日期类型

### 支持的 SQL 操作
- **CREATE TABLE**: 创建表
- **DROP TABLE**: 删除表
- **INSERT**: 插入数据
- **SELECT**: 查询数据（支持列选择和 * 通配符）
- **UPDATE**: 更新数据
- **DELETE**: 删除数据
- **WHERE**: 条件过滤（支持 =, !=, <, <=, >, >= 和 AND/OR 逻辑运算）

### 存储引擎特性
- **页式存储**: 4KB 页大小，类似 SQLite 的设计
- **标记删除**: 借鉴 PostgreSQL 的 MVCC 机制
- **二进制格式**: 高效的磁盘存储
- **数据持久化**: 自动保存到磁盘
- **元数据管理**: JSON 格式的表结构信息

## 快速开始

### 编译

```bash
go build -o godb.exe
```

### 运行

```bash
./godb.exe
```

### 使用示例

```sql
-- 创建表
CREATE TABLE users (id INT, name TEXT, age INT, active TINYINT)

-- 插入数据
INSERT INTO users VALUES (1, 'Alice', 25, 'true')
INSERT INTO users VALUES (2, 'Bob', 30, 'true')
INSERT INTO users VALUES (3, 'Charlie', 35, 'false')

-- 查询所有数据
SELECT * FROM users

-- 条件查询
SELECT name, age FROM users WHERE age > 25

-- 更新数据
UPDATE users SET active = 'false' WHERE name = 'Bob'

-- 删除数据
DELETE FROM users WHERE age < 30

-- 退出
exit
```

### 从文件执行 SQL

```bash
./godb.exe < script.sql
```

## 项目架构

```
godb/
├── main.go              # 程序入口
├── parser/              # SQL 解析器封装
│   └── parser.go
├── types/               # 数据类型系统
│   └── types.go
├── storage/             # 存储引擎
│   ├── page.go         # 页管理
│   ├── pager.go        # 页缓存和磁盘 I/O
│   └── table.go        # 表存储和行管理
├── catalog/             # 元数据管理
│   └── schema.go
├── executor/            # 查询执行器
│   ├── executor.go     # 主执行器
│   ├── create.go       # CREATE/DROP TABLE
│   ├── insert.go       # INSERT
│   ├── select.go       # SELECT
│   ├── update.go       # UPDATE
│   └── delete.go       # DELETE
└── repl/                # REPL 交互界面
    └── repl.go
```

## 设计亮点

### 1. 标记删除机制
借鉴 MySQL InnoDB 和 PostgreSQL 的设计：
- UPDATE 操作 = 标记旧行删除 + 插入新行
- DELETE 操作 = 标记行为删除
- SELECT 时自动跳过已删除的行
- 为将来实现 MVCC 和事务预留了空间

### 2. 页式存储
- 4KB 固定大小的页
- 每页包含页头（页 ID、类型、行数、下一页指针）
- 支持页链表，自动分配新页
- 页缓存机制，减少磁盘 I/O

### 3. 二进制序列化
- 高效的二进制格式存储
- 每行包含删除标记和列数据
- 使用 Little Endian 字节序

### 4. 类型系统
- 支持 5 种基础数据类型
- 类型安全的序列化/反序列化
- 支持类型别名（如 INT/INTEGER/BIGINT）
- 自动类型转换（INT → FLOAT）

## 数据库文件

- **godb.db**: 二进制数据文件（页式存储）
- **godb_meta.json**: 表结构元数据（JSON 格式）

## 示例测试

### 测试基本 CRUD 操作
```bash
./godb.exe < test.sql
```

### 测试所有数据类型
```bash
./godb.exe < test_all_types.sql
```

### 测试数据持久化
```bash
./godb.exe < test_persistence.sql
```

## 技术栈

- **Go 1.23.1**: 编程语言
- **github.com/xwb1989/sqlparser**: SQL 解析器（基于 vitess）

## 未来优化方向

1. **索引支持**: 实现 B-Tree 索引，提高查询性能
2. **JOIN 操作**: 支持表连接查询
3. **事务支持**: 实现 ACID 特性
4. **垃圾回收**: VACUUM 机制清理已删除数据
5. **聚合函数**: COUNT, SUM, AVG, MIN, MAX
6. **更多 SQL 特性**:
   - GROUP BY / HAVING
   - ORDER BY / LIMIT / OFFSET
   - 子查询
   - CREATE INDEX
7. **性能优化**:
   - 更智能的页缓存策略（LRU）
   - 批量插入优化
   - 查询优化器

## 与主流数据库的对比

### UPDATE/DELETE 实现
| 数据库 | UPDATE 实现 | DELETE 实现 | 垃圾回收 |
|--------|------------|------------|----------|
| PostgreSQL | 标记删除 + 插入新行 | 设置 xmax | VACUUM |
| MySQL InnoDB | 标记删除 + 插入新行 | 设置删除标记 | Purge 线程 |
| **godb** | 标记删除 + 插入新行 | 设置删除标记 | 待实现 |

### 存储格式
| 数据库 | 页大小 | 存储格式 |
|--------|--------|---------|
| SQLite | 1KB-64KB（可配置） | B-Tree |
| PostgreSQL | 8KB | Heap |
| **godb** | 4KB | 简化的页式存储 |

## License

MIT License

## 作者

本项目为学习目的实现的简易数据库。
