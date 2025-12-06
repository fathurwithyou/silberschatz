from enum import Enum
from dataclasses import dataclass
from typing import List, Optional


class TokenType(Enum):
    # Keywords
    SELECT = "SELECT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    INSERT = "INSERT"
    FROM = "FROM"
    WHERE = "WHERE"
    JOIN = "JOIN"
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    OUTER = "OUTER"
    ON = "ON"
    ORDER = "ORDER"
    BY = "BY"
    LIMIT = "LIMIT"
    BEGIN = "BEGIN"
    TRANSACTION = "TRANSACTION"
    COMMIT = "COMMIT"
    ABORT = "ABORT"
    CREATE = "CREATE"
    DROP = "DROP"
    TABLE = "TABLE"
    CASCADE = "CASCADE"
    RESTRICT = "RESTRICT"
    AS = "AS"
    INTO = "INTO"
    VALUES = "VALUES"
    SET = "SET"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    NULL = "NULL"
    ASC = "ASC"
    DESC = "DESC"
    PRIMARY = "PRIMARY"
    KEY = "KEY"
    REFERENCES = "REFERENCES"
    INTEGER = "INTEGER"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    FLOAT = "FLOAT"
    INT = "INT"
    INDEX = "INDEX"
    USING = "USING"
    
    # Foreign key actions
    NO = "NO"
    ACTION = "ACTION"
    
    # Identifiers and literals
    IDENTIFIER = "IDENTIFIER"
    STRING_LITERAL = "STRING_LITERAL"
    NUMBER_LITERAL = "NUMBER_LITERAL"
    
    # Operators
    EQUALS = "="
    NOT_EQUALS = "!="
    LESS_THAN = "<"
    GREATER_THAN = ">"
    LESS_EQUAL = "<="
    GREATER_EQUAL = ">="
    
    # Punctuation
    COMMA = ","
    SEMICOLON = ";"
    LEFT_PAREN = "("
    RIGHT_PAREN = ")"
    ASTERISK = "*"
    DOT = "."
    
    # Special
    WHITESPACE = "WHITESPACE"
    EOF = "EOF"
    INVALID = "INVALID"


@dataclass
class Token:
    type: TokenType
    value: str
    position: int
    line: int
    column: int


class SQLLexer:
    KEYWORDS = {
        'SELECT': TokenType.SELECT,
        'UPDATE': TokenType.UPDATE,
        'DELETE': TokenType.DELETE,
        'INSERT': TokenType.INSERT,
        'FROM': TokenType.FROM,
        'WHERE': TokenType.WHERE,
        'JOIN': TokenType.JOIN,
        'INNER': TokenType.INNER,
        'LEFT': TokenType.LEFT,
        'RIGHT': TokenType.RIGHT,
        'FULL': TokenType.FULL,
        'OUTER': TokenType.OUTER,
        'ON': TokenType.ON,
        'ORDER': TokenType.ORDER,
        'BY': TokenType.BY,
        'LIMIT': TokenType.LIMIT,
        'BEGIN': TokenType.BEGIN,
        'TRANSACTION': TokenType.TRANSACTION,
        'COMMIT': TokenType.COMMIT,
        'CREATE': TokenType.CREATE,
        'ABORT': TokenType.ABORT,
        'DROP': TokenType.DROP,
        'TABLE': TokenType.TABLE,
        'CASCADE': TokenType.CASCADE,
        'RESTRICT': TokenType.RESTRICT,
        'AS': TokenType.AS,
        'INTO': TokenType.INTO,
        'VALUES': TokenType.VALUES,
        'SET': TokenType.SET,
        'AND': TokenType.AND,
        'OR': TokenType.OR,
        'NOT': TokenType.NOT,
        'NULL': TokenType.NULL,
        'ASC': TokenType.ASC,
        'DESC': TokenType.DESC,
        'PRIMARY': TokenType.PRIMARY,
        'KEY': TokenType.KEY,
        'REFERENCES': TokenType.REFERENCES,
        'INTEGER': TokenType.INTEGER,
        'VARCHAR': TokenType.VARCHAR,
        'CHAR': TokenType.CHAR,
        'FLOAT': TokenType.FLOAT,
        'INT': TokenType.INT,
        'INDEX': TokenType.INDEX,
        'USING': TokenType.USING,
        'NO': TokenType.NO,
        'ACTION': TokenType.ACTION,
    }
    
    # Operator patterns
    OPERATORS = {
        '=': TokenType.EQUALS,
        '!=': TokenType.NOT_EQUALS,
        '<>': TokenType.NOT_EQUALS,
        '<': TokenType.LESS_THAN,
        '>': TokenType.GREATER_THAN,
        '<=': TokenType.LESS_EQUAL,
        '>=': TokenType.GREATER_EQUAL,
    }
    
    # Single character tokens
    SINGLE_CHARS = {
        ',': TokenType.COMMA,
        ';': TokenType.SEMICOLON,
        '(': TokenType.LEFT_PAREN,
        ')': TokenType.RIGHT_PAREN,
        '*': TokenType.ASTERISK,
        '.': TokenType.DOT,
    }
    
    def __init__(self, text: str):
        self.text = text
        self.position = 0
        self.line = 1
        self.column = 1
        self.tokens: List[Token] = []
    
    def tokenize(self) -> List[Token]:
        """Tokenize the input text using DFA approach."""
        self.tokens = []
        self.position = 0
        self.line = 1
        self.column = 1
        
        while self.position < len(self.text):
            self._skip_whitespace()
            
            if self.position >= len(self.text):
                break
            
            token = self._next_token()
            if token and token.type != TokenType.WHITESPACE:
                self.tokens.append(token)
        
        # Add EOF token
        self.tokens.append(Token(
            type=TokenType.EOF,
            value="",
            position=self.position,
            line=self.line,
            column=self.column
        ))
        
        return self.tokens
    
    def _next_token(self) -> Optional[Token]:
        """Get the next token using DFA states."""
        if self.position >= len(self.text):
            return None
        
        start_pos = self.position
        start_line = self.line
        start_col = self.column
        current_char = self.text[self.position]
        
        # String literals (single or double quotes)
        if current_char in ('"', "'"):
            return self._read_string_literal(start_pos, start_line, start_col)
        
        # Numbers
        if current_char.isdigit():
            return self._read_number(start_pos, start_line, start_col)
        
        if (current_char == '.' and 
            self.position + 1 < len(self.text) and 
            self.text[self.position + 1].isdigit()):
            return self._read_decimal_number(start_pos, start_line, start_col)
        
        # Identifiers and keywords
        if current_char.isalpha() or current_char == '_':
            return self._read_identifier(start_pos, start_line, start_col)
        
        # Two-character operators
        if self.position + 1 < len(self.text):
            two_char = self.text[self.position:self.position + 2]
            if two_char in self.OPERATORS:
                self._advance(2)
                return Token(
                    type=self.OPERATORS[two_char],
                    value=two_char,
                    position=start_pos,
                    line=start_line,
                    column=start_col
                )
        
        # Single character tokens
        if current_char in self.SINGLE_CHARS:
            self._advance()
            return Token(
                type=self.SINGLE_CHARS[current_char],
                value=current_char,
                position=start_pos,
                line=start_line,
                column=start_col
            )
        
        # Single character operators
        if current_char in self.OPERATORS:
            self._advance()
            return Token(
                type=self.OPERATORS[current_char],
                value=current_char,
                position=start_pos,
                line=start_line,
                column=start_col
            )
        
        # Invalid character
        self._advance()
        return Token(
            type=TokenType.INVALID,
            value=current_char,
            position=start_pos,
            line=start_line,
            column=start_col
        )
    
    def _read_string_literal(self, start_pos: int, start_line: int, start_col: int) -> Token:
        """Read a string literal (DFA for string state)."""
        quote_char = self.text[self.position]
        value = quote_char
        self._advance()
        
        found_closing_quote = False
        while self.position < len(self.text):
            current_char = self.text[self.position]
            
            if current_char == quote_char:
                value += current_char
                self._advance()
                found_closing_quote = True
                break
            elif current_char == '\\' and self.position + 1 < len(self.text):
                # Handle escape sequences
                value += current_char
                self._advance()
                if self.position < len(self.text):
                    escape_char = self.text[self.position]
                    value += escape_char
                    self._advance()
            else:
                value += current_char
                self._advance()
        
        # If we reached end of text without finding closing quote, it's invalid
        if not found_closing_quote:
            return Token(
                type=TokenType.INVALID,
                value=value,
                position=start_pos,
                line=start_line,
                column=start_col
            )
        
        return Token(
            type=TokenType.STRING_LITERAL,
            value=value,
            position=start_pos,
            line=start_line,
            column=start_col
        )
    
    def _read_number(self, start_pos: int, start_line: int, start_col: int) -> Token:
        """Read a numeric literal (DFA for number state)."""
        value = ""
        has_decimal = False
        
        while self.position < len(self.text):
            current_char = self.text[self.position]
            
            if current_char.isdigit():
                value += current_char
                self._advance()
            elif current_char == '.' and not has_decimal:
                if (self.position + 1 < len(self.text) and 
                    self.text[self.position + 1].isdigit()):
                    has_decimal = True
                    value += current_char
                    self._advance()
                elif (self.position + 1 >= len(self.text) or
                      not self.text[self.position + 1].isalpha()):
                    has_decimal = True
                    value += current_char
                    self._advance()
                else:
                    break
            else:
                break
        
        if self.position < len(self.text):
            next_char = self.text[self.position]
            
            # invalid: immediately followed by letter (123abc)
            if next_char.isalpha():
                # Consume the invalid part
                invalid_value = value
                while (self.position < len(self.text) and 
                       (self.text[self.position].isalnum() or self.text[self.position] == '_')):
                    invalid_value += self.text[self.position]
                    self._advance()
                
                return Token(
                    type=TokenType.INVALID,
                    value=invalid_value,
                    position=start_pos,
                    line=start_line,
                    column=start_col
                )
        
        return Token(
            type=TokenType.NUMBER_LITERAL,
            value=value,
            position=start_pos,
            line=start_line,
            column=start_col
        )
    
    def _read_decimal_number(self, start_pos: int, start_line: int, start_col: int) -> Token:
        value = ""
        
        # Read the dot and following digits
        while self.position < len(self.text):
            current_char = self.text[self.position]
            
            if current_char == '.' or current_char.isdigit():
                value += current_char
                self._advance()
            else:
                break
        
        # invalid: immediately followed by letter
        if (self.position < len(self.text) and 
            self.text[self.position].isalpha()):
            # Consume the invalid part
            invalid_value = value
            while (self.position < len(self.text) and 
                   (self.text[self.position].isalnum() or self.text[self.position] == '_')):
                invalid_value += self.text[self.position]
                self._advance()
            
            return Token(
                type=TokenType.INVALID,
                value=invalid_value,
                position=start_pos,
                line=start_line,
                column=start_col
            )
        
        return Token(
            type=TokenType.NUMBER_LITERAL,
            value=value,
            position=start_pos,
            line=start_line,
            column=start_col
        )
    
    def _read_identifier(self, start_pos: int, start_line: int, start_col: int) -> Token:
        """Read an identifier or keyword (DFA for identifier state)."""
        value = ""
        
        while self.position < len(self.text):
            current_char = self.text[self.position]
            
            if current_char.isalnum() or current_char == '_':
                value += current_char
                self._advance()
            else:
                break
        
        # Check if it's a keyword (case-insensitive)
        upper_value = value.upper()
        token_type = self.KEYWORDS.get(upper_value, TokenType.IDENTIFIER)
        
        return Token(
            type=token_type,
            value=value,
            position=start_pos,
            line=start_line,
            column=start_col
        )
    
    def _skip_whitespace(self) -> None:
        """Skip whitespace characters."""
        while (self.position < len(self.text) and 
               self.text[self.position].isspace()):
            self._advance()
    
    def _advance(self, count: int = 1) -> None:
        """Advance position and update line/column counters."""
        for _ in range(count):
            if self.position < len(self.text):
                if self.text[self.position] == '\n':
                    self.line += 1
                    self.column = 1
                else:
                    self.column += 1
                self.position += 1
