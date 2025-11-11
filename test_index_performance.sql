CREATE TABLE users (id INT, name TEXT, age INT, city TEXT)
INSERT INTO users VALUES (1, 'Alice', 25, 'NYC')
INSERT INTO users VALUES (2, 'Bob', 30, 'LA')
INSERT INTO users VALUES (3, 'Charlie', 35, 'SF')
INSERT INTO users VALUES (4, 'David', 28, 'NYC')
INSERT INTO users VALUES (5, 'Eve', 32, 'LA')
INSERT INTO users VALUES (6, 'Frank', 27, 'SF')
INSERT INTO users VALUES (7, 'Grace', 29, 'NYC')
INSERT INTO users VALUES (8, 'Henry', 31, 'LA')
INSERT INTO users VALUES (9, 'Ivy', 26, 'SF')
INSERT INTO users VALUES (10, 'Jack', 33, 'NYC')
SELECT * FROM users WHERE age = 30
CREATE INDEX idx_age ON users (age)
SELECT * FROM users WHERE age = 30
SELECT * FROM users WHERE age > 30
SELECT * FROM users WHERE age <= 28
exit
