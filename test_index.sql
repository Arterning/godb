CREATE TABLE products (id INT, name TEXT, price FLOAT, stock INT)
INSERT INTO products VALUES (1, 'Laptop', 999.99, 10)
INSERT INTO products VALUES (2, 'Mouse', 29.99, 100)
INSERT INTO products VALUES (3, 'Keyboard', 79.99, 50)
INSERT INTO products VALUES (4, 'Monitor', 299.99, 20)
INSERT INTO products VALUES (5, 'Headphones', 149.99, 30)
SELECT * FROM products
CREATE INDEX idx_price ON products (price)
SELECT * FROM products WHERE price = 79.99
SELECT * FROM products WHERE price > 100
SELECT * FROM products WHERE price <= 100
CREATE INDEX idx_id ON products (id)
SELECT * FROM products WHERE id = 3
UPDATE products SET price = 89.99 WHERE id = 3
SELECT * FROM products WHERE price = 89.99
DELETE FROM products WHERE id = 5
SELECT * FROM products
DROP INDEX idx_price
SELECT * FROM products WHERE price > 100
exit
