CREATE TABLE customers (id INT, name TEXT, city TEXT)
CREATE TABLE orders (order_id INT, customer_id INT, product TEXT, amount FLOAT)
INSERT INTO customers VALUES (1, 'Alice', 'NYC')
INSERT INTO customers VALUES (2, 'Bob', 'LA')
INSERT INTO customers VALUES (3, 'Charlie', 'SF')
INSERT INTO customers VALUES (4, 'David', 'NYC')
INSERT INTO orders VALUES (101, 1, 'Laptop', 999.99)
INSERT INTO orders VALUES (102, 1, 'Mouse', 29.99)
INSERT INTO orders VALUES (103, 2, 'Keyboard', 79.99)
INSERT INTO orders VALUES (104, 3, 'Monitor', 299.99)
INSERT INTO orders VALUES (105, 999, 'Tablet', 499.99)
SELECT * FROM customers
SELECT * FROM orders
SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id
SELECT customers.name, orders.product, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id
SELECT * FROM customers LEFT JOIN orders ON customers.id = orders.customer_id
SELECT * FROM customers RIGHT JOIN orders ON customers.id = orders.customer_id
SELECT customers.name, orders.product FROM customers INNER JOIN orders ON customers.id = orders.customer_id WHERE orders.amount > 100
exit
