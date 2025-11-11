-- 测试事务 ACID 功能

-- 清理旧数据
DROP TABLE accounts
DROP TABLE test_tx

-- 创建测试表
CREATE TABLE accounts (id INT, name TEXT, balance FLOAT)
CREATE TABLE test_tx (id INT, value TEXT)

-- 测试 1: 自动提交模式（默认）
INSERT INTO accounts VALUES (1, 'Alice', 1000.0)
INSERT INTO accounts VALUES (2, 'Bob', 500.0)
SELECT * FROM accounts

-- 测试 2: 事务提交（COMMIT）
BEGIN
INSERT INTO accounts VALUES (3, 'Charlie', 2000.0)
UPDATE accounts SET balance = 1500.0 WHERE name = 'Alice'
SELECT * FROM accounts
COMMIT

-- 验证提交后的数据
SELECT * FROM accounts

-- 测试 3: 事务回滚（ROLLBACK）
BEGIN
INSERT INTO accounts VALUES (4, 'David', 3000.0)
UPDATE accounts SET balance = 100.0 WHERE name = 'Bob'
SELECT * FROM accounts
ROLLBACK

-- 验证回滚后的数据（David 不应该存在，Bob 的余额应该还是 500.0）
SELECT * FROM accounts

-- 测试 4: 删除操作的回滚
BEGIN
DELETE FROM accounts WHERE name = 'Charlie'
SELECT * FROM accounts
ROLLBACK

-- 验证 Charlie 仍然存在
SELECT * FROM accounts

-- 测试 5: 复杂事务 - 转账操作（原子性）
BEGIN
UPDATE accounts SET balance = 1400.0 WHERE name = 'Alice'
UPDATE accounts SET balance = 600.0 WHERE name = 'Bob'
COMMIT

SELECT * FROM accounts

-- 测试 6: 转账失败回滚
BEGIN
UPDATE accounts SET balance = 1300.0 WHERE name = 'Alice'
UPDATE accounts SET balance = 700.0 WHERE name = 'Bob'
ROLLBACK

-- 余额应该保持 Alice=1400.0, Bob=600.0
SELECT * FROM accounts

-- 测试 7: 多个 INSERT 的回滚
BEGIN
INSERT INTO test_tx VALUES (1, 'test1')
INSERT INTO test_tx VALUES (2, 'test2')
INSERT INTO test_tx VALUES (3, 'test3')
SELECT * FROM test_tx
ROLLBACK

-- 应该没有数据
SELECT * FROM test_tx

-- 测试 8: 多个 INSERT 的提交
BEGIN
INSERT INTO test_tx VALUES (10, 'value10')
INSERT INTO test_tx VALUES (20, 'value20')
COMMIT

SELECT * FROM test_tx

-- 测试 9: UPDATE 的原子性
BEGIN
UPDATE test_tx SET value = 'updated10' WHERE id = 10
UPDATE test_tx SET value = 'updated20' WHERE id = 20
COMMIT

SELECT * FROM test_tx

-- 测试 10: DELETE 和 INSERT 混合
BEGIN
DELETE FROM test_tx WHERE id = 10
INSERT INTO test_tx VALUES (30, 'value30')
COMMIT

SELECT * FROM test_tx

exit
