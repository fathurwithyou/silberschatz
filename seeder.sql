-- ============================================================================
-- DATABASE SEEDER
-- Tables and sample data based on test files
-- ============================================================================

-- ============================================================================
-- CREATE TABLES
-- ============================================================================

-- Departments table
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    budget FLOAT
);

-- Employees table with foreign key to departments
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    employee_code CHAR(8) NOT NULL,
    full_name VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    salary FLOAT,
    department_id INTEGER REFERENCES departments(id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Users table for testing nullable columns
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    age INTEGER
);

-- Products table
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price FLOAT,
    description VARCHAR(200)
);

-- Mixed types table for testing various data types
CREATE TABLE mixed_types (
    id INTEGER,
    name VARCHAR(50),
    code CHAR(10),
    score FLOAT
);

-- ============================================================================
-- INSERT DATA - DEPARTMENTS
-- ============================================================================

INSERT INTO departments VALUES (1, 'Engineering', 1000000.0);
INSERT INTO departments VALUES (2, 'Marketing', 500000.0);
INSERT INTO departments VALUES (3, 'Human Resources', 300000.0);
INSERT INTO departments VALUES (4, 'Finance', 750000.0);
INSERT INTO departments VALUES (5, 'Operations', 600000.0);

-- ============================================================================
-- INSERT DATA - EMPLOYEES
-- ============================================================================

INSERT INTO employees VALUES (1, 'EMP00001', 'John Doe', 'john.doe@company.com', 75000.0, 1);
INSERT INTO employees VALUES (2, 'EMP00002', 'Jane Smith', 'jane.smith@company.com', 85000.0, 1);
INSERT INTO employees VALUES (3, 'EMP00003', 'Bob Johnson', 'bob.johnson@company.com', 65000.0, 2);
INSERT INTO employees VALUES (4, 'EMP00004', 'Alice Brown', 'alice.brown@company.com', 70000.0, 2);
INSERT INTO employees VALUES (5, 'EMP00005', 'Charlie Wilson', 'charlie.wilson@company.com', 80000.0, 1);
INSERT INTO employees VALUES (6, 'EMP00006', 'David Lee', 'david.lee@company.com', 72000.0, 3);
INSERT INTO employees VALUES (7, 'EMP00007', 'Eva Garcia', 'eva.garcia@company.com', 78000.0, 4);
INSERT INTO employees VALUES (8, 'EMP00008', 'Frank Miller', 'frank.miller@company.com', 68000.0, 5);
INSERT INTO employees VALUES (9, 'EMP00009', 'Grace Davis', 'grace.davis@company.com', 82000.0, 1);
INSERT INTO employees VALUES (10, 'EMP00010', 'Henry Taylor', 'henry.taylor@company.com', 76000.0, 2);

-- ============================================================================
-- INSERT DATA - USERS
-- ============================================================================

-- Users with complete data
INSERT INTO users VALUES (1, 'John Doe', 'john@email.com', 30);
INSERT INTO users VALUES (2, 'Jane Smith', 'jane@email.com', 25);
INSERT INTO users VALUES (3, 'Bob Johnson', 'bob@email.com', 35);
INSERT INTO users VALUES (4, 'Alice Brown', 'alice@test.com', 28);
INSERT INTO users VALUES (5, 'Charlie Wilson', 'charlie@test.com', 40);

-- Users with NULL values (testing nullable columns)
INSERT INTO users VALUES (6, 'David Lee', NULL, NULL);
INSERT INTO users VALUES (7, 'Eva Garcia', NULL, 32);
INSERT INTO users VALUES (8, 'Frank Miller', 'frank@email.com', NULL);

-- Additional users
INSERT INTO users VALUES (9, 'Test User', 'test@email.com', 45);
INSERT INTO users VALUES (10, 'Sarah Johnson', 'sarah.j@email.com', 27);

-- ============================================================================
-- INSERT DATA - PRODUCTS
-- ============================================================================

INSERT INTO products VALUES (1, 'Laptop', 1200.50, 'High-performance laptop');
INSERT INTO products VALUES (2, 'Mouse', 25.99, 'Wireless optical mouse');
INSERT INTO products VALUES (3, 'Keyboard', 75.00, 'Mechanical keyboard');
INSERT INTO products VALUES (4, 'Monitor', 350.00, '27-inch 4K display');
INSERT INTO products VALUES (5, 'Headphones', 150.00, 'Noise-cancelling headphones');
INSERT INTO products VALUES (6, 'Webcam', 89.99, '1080p HD webcam');
INSERT INTO products VALUES (7, 'Desk Chair', 299.00, 'Ergonomic office chair');
INSERT INTO products VALUES (8, 'USB Hub', 35.00, '7-port USB 3.0 hub');
INSERT INTO products VALUES (9, 'External SSD', 180.00, '1TB portable SSD');
INSERT INTO products VALUES (10, 'Microphone', 120.00, 'USB condenser microphone');

-- ============================================================================
-- INSERT DATA - MIXED_TYPES
-- ============================================================================

INSERT INTO mixed_types VALUES (1, 'Test Item 1', 'CODE001', 95.5);
INSERT INTO mixed_types VALUES (2, 'Test Item 2', 'CODE002', 87.3);
INSERT INTO mixed_types VALUES (3, 'Test Item 3', 'CODE003', 92.8);
INSERT INTO mixed_types VALUES (4, 'Test Item 4', 'CODE004', 78.9);
INSERT INTO mixed_types VALUES (5, 'Test Item 5', 'CODE005', 88.6);

-- ============================================================================
-- END OF SEEDER
-- ============================================================================
