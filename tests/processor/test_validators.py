"""Test cases for SQL Syntax Validator."""

import sys
import os

# Add the project root to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.processor.validators.validator import SyntaxValidator
from pprint import pformat


def test_select_statements():
    """Test SELECT statement validation."""
    validator = SyntaxValidator()
    
    valid_queries = [
        "SELECT * FROM users",
        "SELECT name, age FROM users",
        "SELECT u.name, u.age FROM users u",
        "SELECT name AS full_name FROM users",
        "SELECT * FROM users WHERE age > 18",
        "SELECT * FROM users WHERE name = 'John'",
        "SELECT * FROM users ORDER BY age",
        "SELECT * FROM users ORDER BY age ASC",
        "SELECT * FROM users ORDER BY age DESC",
        "SELECT * FROM users LIMIT 10",
        "SELECT * FROM users WHERE age > 18 ORDER BY name LIMIT 5",
        "SELECT 1 AS result",
        "SELECT 5.5",
    ]
    
    for query in valid_queries:
        result = validator.validate(query)
        assert result.is_valid, f"Query should be valid: {query}"


def test_join_statements():
    """Test JOIN statement validation."""
    validator = SyntaxValidator()
    
    valid_queries = [
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
        "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id",
        "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id",
        "SELECT * FROM users u RIGHT JOIN orders o ON u.id = o.user_id",
        "SELECT * FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id",
    ]
    
    for query in valid_queries:
        result = validator.validate(query)
        assert result.is_valid, f"Query should be valid: {query}"


def test_insert_statements():
    """Test INSERT statement validation."""
    validator = SyntaxValidator()
    
    valid_queries = [
        "INSERT INTO users VALUES ('John', 25)",
        "INSERT INTO users (name, age) VALUES ('John', 25)",
    ]
    
    for query in valid_queries:
        result = validator.validate(query)
        assert result.is_valid, f"Query should be valid: {query}"


def test_update_statements():
    """Test UPDATE statement validation."""
    validator = SyntaxValidator()
    
    valid_queries = [
        "UPDATE users SET age = 26",
        "UPDATE users SET age = 26 WHERE name = 'John'",
        "UPDATE users SET name = 'Jane', age = 27 WHERE id = 1",
    ]
    
    for query in valid_queries:
        result = validator.validate(query)
        assert result.is_valid, f"Query should be valid: {query}"


def test_delete_statements():
    """Test DELETE statement validation."""
    validator = SyntaxValidator()
    
    valid_queries = [
        "DELETE FROM users",
        "DELETE FROM users WHERE age < 18",
    ]
    
    for query in valid_queries:
        result = validator.validate(query)
        assert result.is_valid, f"Query should be valid: {query}"


def test_ddl_statements():
    """Test DDL statement validation."""
    validator = SyntaxValidator()
    
    create_query = "CREATE TABLE users (id INT, name VARCHAR)"
    result = validator.validate(create_query)
    assert result.is_valid, f"Query should be valid: {create_query}"
    
    drop_query = "DROP TABLE users"
    result = validator.validate(drop_query)
    assert result.is_valid, f"Query should be valid: {drop_query}"

    cascade_query = "DROP TABLE users CASCADE"
    result = validator.validate(cascade_query)
    assert result.is_valid, f"Query should be valid: {cascade_query}"

    restrict_query = "DROP TABLE users RESTRICT"
    result = validator.validate(restrict_query)
    assert result.is_valid, f"Query should be valid: {restrict_query}"


def test_tcl_statements():
    """Test TCL statement validation."""
    validator = SyntaxValidator()
    
    begin_query = "BEGIN TRANSACTION"
    result = validator.validate(begin_query)
    assert result.is_valid, f"Query should be valid: {begin_query}"
    
    commit_query = "COMMIT"
    result = validator.validate(commit_query)
    assert result.is_valid, f"Query should be valid: {commit_query}"


def test_invalid_statements():
    """Test invalid statement validation."""
    validator = SyntaxValidator()
    
    invalid_queries = [
        # Basic missing parts / wrong order
        "SELECT FROM users",
        "SELECT * users",
        "SELECT * FROM",
        "SELECT * FROM users WHERE",
        "SELECT * FROM users ORDER",
        "SELECT * FROM users ORDER BY",
        "SELECT * WHERE age > 18 FROM users",  
        "UPDATE users SET",
        "UPDATE users SET name",
        "INSERT INTO users VALUES",
        "DELETE FROM",
        "CREATE TABLE",
        "DROP TABLE",
        "BEGIN",
        "",
        "   ",
        # Malformed punctuation / parentheses / commas
        "INSERT INTO users (name, age VALUES ('John', 25)",
        "INSERT INTO users (name, age,) VALUES ('John', 25)",
        "UPDATE users SET name = 'Jane', WHERE id = 1",
        "DELETE FROM users WHERE id =",
        "SELECT * FROM users WHERE name = 'John",
        "SELECT name age FROM users",
        "SELECT * FROM users (id = 1)",
        
        # Invalid clauses / keywords
        "SELECT * FROM users LIMIT ten",
        "SELECT * FROM users OFFSET -5",
        "SELECT * FROM users ORDER BY 123abc",
        "SELECT * FROM users GROUP BY",
        "SELECT COUNT(*) FROM WHERE id = 1",
        "INSERT users VALUES ('a')",
        "INSERT INTO (name) VALUES ('a')",
        
        # Bad JOINs
        "SELECT * FROM users JOIN ON users.id = orders.user_id",
        "SELECT * FROM users LEFT JOIN", 
        
        # DDL / TCL misuse
        "CREATE TABLE users id INT, name VARCHAR",
        "DROP TABLE IF EXISTS",
        "COMMIT TRANSACTION NOW",
        
        # Miscellaneous invalid identifiers and syntax
        "SELECT * FROM 123invalid",
        "SELECT * FROM users WHERE age >> 30",
        "SELECT * FROM users WHERE name LIKE %John%",
        "SELECT * FROM users WHERE (age > 18",
        "SELECT * FROM users; DROP TABLE users;",
        "SELECT DISTINCT ON (name) FROM users",
    ]
    
    for query in invalid_queries:
        result = validator.validate(query)
        # print(query)
        # print(f"ERROR: {result.error_message}")
        # if result.error_position:
        #     line, col = result.error_position
        #     print(f"LINE {line}: {query}")
        #     if query:  # Only show pointer for non-empty queries
        #         pointer = ' ' * (col + 6) + '^'
        #         print(pointer)
        # print()
        assert not result.is_valid, f"Query should be invalid: {query}"
        assert result.error_message, f"Should have error message for: {query}"

if __name__ == "__main__":
    test_select_statements()
    test_join_statements() 
    test_insert_statements()
    test_update_statements()
    test_delete_statements()
    test_ddl_statements()
    test_tcl_statements()
    test_invalid_statements()
    
    print("All tests passed!")
