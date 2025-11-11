CREATE TABLE products (id INT, name TEXT, price FLOAT, in_stock TINYINT, created_date DATE)
INSERT INTO products VALUES (1, 'Laptop', 999.99, 'true', '2024-01-15')
INSERT INTO products VALUES (2, 'Mouse', 29.99, 'true', '2024-02-20')
INSERT INTO products VALUES (3, 'Keyboard', 79.99, 'false', '2024-03-10')
SELECT * FROM products
SELECT name, price FROM products WHERE price < 100
UPDATE products SET in_stock = 'true' WHERE name = 'Keyboard'
SELECT * FROM products
exit
