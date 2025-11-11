CREATE TABLE users (id INT, name TEXT, age INT, active TINYINT)
INSERT INTO users VALUES (1, 'Alice', 25, 'true')
INSERT INTO users VALUES (2, 'Bob', 30, 'true')
INSERT INTO users VALUES (3, 'Charlie', 35, 'false')
SELECT * FROM users
SELECT name, age FROM users WHERE age > 25
UPDATE users SET active = 'false' WHERE name = 'Bob'
SELECT * FROM users
DELETE FROM users WHERE age < 30
SELECT * FROM users
exit
