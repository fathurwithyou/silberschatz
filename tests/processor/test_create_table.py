import pytest
from src.processor.validators.parser import SQLParser, ParseError
from src.processor.validators.lexer import SQLLexer


def parse_ok(query: str) -> bool:
    tokens = SQLLexer(query).tokenize()
    parser = SQLParser(tokens)
    return parser.parse()


def parse_error(query: str):
    tokens = SQLLexer(query).tokenize()
    parser = SQLParser(tokens)
    with pytest.raises(ParseError):
        parser.parse_with_error()


# -------------------------------
# VALID CREATE TABLE STATEMENTS
# -------------------------------

def test_create_table_single_column():
    assert parse_ok("CREATE TABLE users (id INT);")


def test_create_table_multiple_columns():
    assert parse_ok("CREATE TABLE users (id INT, name TEXT, age INT);")


def test_create_table_with_newlines():
    query = """
    CREATE TABLE users (
        id INT,
        username TEXT,
        email TEXT
    );
    """
    assert parse_ok(query)


# -------------------------------
# INVALID CREATE TABLE STATEMENTS
# -------------------------------

def test_create_table_missing_parenthesis():
    parse_error("CREATE TABLE users id INT);")


def test_create_table_missing_column_type():
    parse_error("CREATE TABLE users (id);")


def test_create_table_missing_column_name():
    parse_error("CREATE TABLE users (INT);")


def test_create_table_trailing_comma():
    parse_error("CREATE TABLE users (id INT, );")


def test_create_table_no_columns():
    parse_error("CREATE TABLE users ();")


def test_create_table_missing_table_name():
    parse_error("CREATE TABLE (id INT);")


def test_create_table_missing_keyword_table():
    parse_error("CREATE users (id INT);")
