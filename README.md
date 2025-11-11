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
- **CREATE INDEX**: 创建索引（支持单列 B-Tree 索引）
- **DROP INDEX**: 删除索引
- **INSERT**: 插入数据
- **SELECT**: 查询数据（支持列选择和 * 通配符，自动使用索引优化）
- **UPDATE**: 更新数据
- **DELETE**: 删除数据
- **WHERE**: 条件过滤（支持 =, !=, <, <=, >, >= 和 AND/OR 逻辑运算）
- **JOIN**: 表连接（支持 INNER JOIN, LEFT JOIN, RIGHT JOIN）
- **事务支持**: BEGIN/COMMIT/ROLLBACK（支持 ACID 特性和 READ COMMITTED 隔离级别）

### 存储引擎特性
- **页式存储**: 4KB 页大小，类似 SQLite 的设计
- **标记删除**: 借鉴 PostgreSQL 的 MVCC 机制
- **二进制格式**: 高效的磁盘存储
- **数据持久化**: 自动保存到磁盘
- **元数据管理**: JSON 格式的表结构信息

### 索引特性（NEW!）
- **B-Tree 索引**: 基于 Google B-Tree 实现的高性能索引
- **自动索引查询优化**: SELECT 语句自动使用索引
- **索引维护**: INSERT/UPDATE/DELETE 自动维护索引
- **支持查询类型**: 等值查询（=）和范围查询（<, <=, >, >=）
- **索引持久化**: 索引元数据持久化，启动时自动重建

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

-- 创建索引（NEW!）
CREATE INDEX idx_age ON users (age)

-- 使用索引的查询（自动优化）
SELECT * FROM users WHERE age = 30
SELECT * FROM users WHERE age > 25

-- 删除索引
DROP INDEX idx_age

-- 退出
exit
```

### 事务使用示例（NEW!）

```sql
-- 创建测试表
CREATE TABLE accounts (id INT, name TEXT, balance FLOAT)

-- 自动提交模式（默认）
INSERT INTO accounts VALUES (1, 'Alice', 1000.0)
INSERT INTO accounts VALUES (2, 'Bob', 500.0)

-- 事务提交示例
BEGIN
UPDATE accounts SET balance = 900.0 WHERE name = 'Alice'
UPDATE accounts SET balance = 600.0 WHERE name = 'Bob'
COMMIT

-- 事务回滚示例
BEGIN
UPDATE accounts SET balance = 0.0 WHERE name = 'Alice'
SELECT * FROM accounts  -- 事务内可以看到修改
ROLLBACK  -- 回滚，Alice 的余额恢复为 900.0

-- 复杂转账事务（原子性）
BEGIN
UPDATE accounts SET balance = 800.0 WHERE name = 'Alice'
UPDATE accounts SET balance = 700.0 WHERE name = 'Bob'
COMMIT  -- 要么全部成功，要么全部失败

-- 查看最终结果
SELECT * FROM accounts
```

**事务特性说明：**
- **原子性（Atomicity）**: 事务中的所有操作要么全部成功（COMMIT），要么全部失败（ROLLBACK）
- **一致性（Consistency）**: 事务执行前后数据库保持一致状态
- **隔离性（Isolation）**: 支持 READ COMMITTED 隔离级别，事务只能看到已提交的数据
- **持久性（Durability）**: COMMIT 后的更改永久保存到磁盘

-- 退出
exit
```

### 索引使用示例

```sql
-- 创建表
CREATE TABLE products (id INT, name TEXT, price FLOAT, stock INT)

-- 插入数据
INSERT INTO products VALUES (1, 'Laptop', 999.99, 10)
INSERT INTO products VALUES (2, 'Mouse', 29.99, 100)
INSERT INTO products VALUES (3, 'Keyboard', 79.99, 50)

-- 在 price 列创建索引
CREATE INDEX idx_price ON products (price)

-- 以下查询会自动使用索引（快速）
SELECT * FROM products WHERE price = 79.99      -- 等值查询
SELECT * FROM products WHERE price > 100         -- 范围查询
SELECT * FROM products WHERE price <= 100        -- 范围查询

-- 更新和删除会自动维护索引
UPDATE products SET price = 89.99 WHERE id = 3
DELETE FROM products WHERE id = 2

-- 删除索引
DROP INDEX idx_price
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
├── index/               # 索引系统
│   ├── index.go        # B-Tree 索引实现
│   └── manager.go      # 索引管理器
├── transaction/         # 事务系统（NEW!）
│   ├── transaction.go  # 事务结构和操作日志
│   ├── lock.go         # 锁管理器（表级锁）
│   └── manager.go      # 事务管理器
├── catalog/             # 元数据管理
│   └── schema.go       # 表和索引元数据
├── executor/            # 查询执行器
│   ├── executor.go     # 主执行器
│   ├── create.go       # CREATE/DROP TABLE
│   ├── index.go        # CREATE/DROP INDEX
│   ├── transaction.go  # BEGIN/COMMIT/ROLLBACK
│   ├── insert.go       # INSERT（维护索引+事务）
│   ├── select.go       # SELECT（索引优化+可见性过滤）
│   ├── update.go       # UPDATE（维护索引+事务）
│   ├── delete.go       # DELETE（维护索引+事务）
│   └── join.go         # JOIN 操作
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

### 5. B-Tree 索引（NEW!）
基于 Google B-Tree 实现的高性能索引系统：
- **索引结构**: 使用 B-Tree 存储键值到 RowID 的映射
- **自动优化**: SELECT 语句自动检测并使用索引
- **索引维护**: INSERT/UPDATE/DELETE 自动维护索引一致性
- **查询支持**:
  - 等值查询（WHERE col = value）使用 Search
  - 范围查询（WHERE col > value）使用 RangeSearch
- **持久化**: 索引元数据保存到 catalog，启动时自动重建
- **性能优化**: 索引查询避免全表扫描，大幅提升查询性能

**索引工作流程**:
1. CREATE INDEX 时：扫描表中所有数据，构建 B-Tree 索引
2. INSERT 时：插入数据后，自动将新行添加到相关索引
3. UPDATE 时：删除旧索引条目，插入新索引条目
4. DELETE 时：从索引中删除对应条目
5. SELECT 时：检测 WHERE 条件，如果列有索引则使用索引查询

### 6. 事务系统（NEW!）
完整的 ACID 事务支持，基于锁和操作日志实现：
- **原子性（Atomicity）**: 使用操作日志记录事务的所有修改，ROLLBACK 时逆序回滚
- **一致性（Consistency）**: 事务执行前后数据库保持一致状态
- **隔离性（Isolation）**:
  - 支持 READ COMMITTED 隔离级别
  - 使用表级锁（读锁/写锁）控制并发访问
  - 可见性规则：只能看到已提交事务的数据和当前事务的修改
- **持久性（Durability）**: COMMIT 时刷新所有脏页到磁盘
- **锁机制**:
  - 表级锁（支持并发多个读事务，写事务互斥）
  - 自动死锁超时检测（30秒）
  - 事务结束时自动释放所有锁
- **事务日志**: 记录 INSERT/UPDATE/DELETE 操作用于回滚

**事务工作流程**:
1. BEGIN: 分配事务ID，进入事务模式
2. 修改操作: 获取表锁，记录操作日志，设置行的事务ID
3. SELECT: 获取读锁，应用可见性过滤（READ COMMITTED）
4. COMMIT: 刷新脏页到磁盘，释放所有锁，标记事务为已提交
5. ROLLBACK: 逆序回滚操作日志，恢复数据，释放所有锁

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

### 测试索引功能
```bash
./godb.exe < test_index.sql
```

### 测试索引性能
```bash
./godb.exe < test_index_performance.sql
```

### 测试 JOIN 功能
```bash
./godb.exe < test_join.sql
```

### 测试事务功能（NEW!）
```bash
./godb.exe < test_transaction.sql
```

## 技术栈

- **Go 1.23.1**: 编程语言
- **github.com/xwb1989/sqlparser**: SQL 解析器（基于 vitess）
- **github.com/google/btree**: B-Tree 实现（用于索引）

## 未来优化方向

1. **复合索引**: 支持多列组合索引
2. **垃圾回收**: VACUUM 机制清理已删除数据
3. **聚合函数**: COUNT, SUM, AVG, MIN, MAX
4. **更多 SQL 特性**:
   - GROUP BY / HAVING
   - ORDER BY / LIMIT / OFFSET
   - 子查询
   - UNIQUE 约束
   - 外键约束
5. **事务增强**:
   - 支持更高隔离级别（REPEATABLE READ, SERIALIZABLE）
   - 行级锁代替表级锁
   - MVCC 多版本并发控制
   - WAL（Write-Ahead Log）
   - 崩溃恢复机制
   - SAVEPOINT 支持
6. **性能优化**:
   - 索引持久化到磁盘（当前为内存索引）
   - 更智能的页缓存策略（LRU）
   - 批量插入优化
   - 查询优化器（选择最优索引）
   - 索引统计信息
   - 并行查询执行

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
