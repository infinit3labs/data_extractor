-- Oracle test database initialization script

-- Create test schema and tables
CREATE USER testuser IDENTIFIED BY testpass;
GRANT CREATE SESSION TO testuser;
GRANT CREATE TABLE TO testuser;
GRANT UNLIMITED TABLESPACE TO testuser;

-- Connect as test user
CONNECT testuser/testpass;

-- Create sample tables for testing
CREATE TABLE employees (
    id NUMBER(10) PRIMARY KEY,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    email VARCHAR2(100) UNIQUE,
    hire_date DATE DEFAULT SYSDATE,
    salary NUMBER(10,2),
    department_id NUMBER(10),
    last_modified DATE DEFAULT SYSDATE
);

CREATE TABLE departments (
    department_id NUMBER(10) PRIMARY KEY,
    department_name VARCHAR2(100) NOT NULL,
    manager_id NUMBER(10),
    location_id NUMBER(10),
    created_date DATE DEFAULT SYSDATE
);

CREATE TABLE orders (
    order_id NUMBER(10) PRIMARY KEY,
    customer_id NUMBER(10) NOT NULL,
    order_date DATE DEFAULT SYSDATE,
    total_amount NUMBER(12,2),
    status VARCHAR2(20) DEFAULT 'PENDING',
    last_modified DATE DEFAULT SYSDATE
);

-- Insert sample data
INSERT INTO departments (department_id, department_name, manager_id, location_id) VALUES
(1, 'Engineering', 101, 1000);

INSERT INTO departments (department_id, department_name, manager_id, location_id) VALUES
(2, 'Sales', 102, 1001);

INSERT INTO departments (department_id, department_name, manager_id, location_id) VALUES
(3, 'Marketing', 103, 1002);

INSERT INTO employees (id, first_name, last_name, email, salary, department_id) VALUES
(1, 'John', 'Doe', 'john.doe@company.com', 75000, 1);

INSERT INTO employees (id, first_name, last_name, email, salary, department_id) VALUES
(2, 'Jane', 'Smith', 'jane.smith@company.com', 80000, 1);

INSERT INTO employees (id, first_name, last_name, email, salary, department_id) VALUES
(3, 'Bob', 'Johnson', 'bob.johnson@company.com', 65000, 2);

INSERT INTO orders (order_id, customer_id, total_amount, status) VALUES
(1001, 1, 1250.00, 'COMPLETED');

INSERT INTO orders (order_id, customer_id, total_amount, status) VALUES
(1002, 2, 750.50, 'PENDING');

INSERT INTO orders (order_id, customer_id, total_amount, status) VALUES
(1003, 1, 2100.75, 'SHIPPED');

COMMIT;

-- Create indexes for better performance
CREATE INDEX idx_emp_last_modified ON employees(last_modified);
CREATE INDEX idx_order_date ON orders(order_date);
CREATE INDEX idx_order_last_modified ON orders(last_modified);