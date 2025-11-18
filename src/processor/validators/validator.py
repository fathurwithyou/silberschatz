from typing import List, Tuple, Optional, Any
from .lexer import SQLLexer, Token, TokenType
from .parser import SQLParser, ParseError


class ValidationResult:
    """Result of SQL syntax validation."""
    
    def __init__(self, is_valid: bool, error_message: str = "", 
                 error_position: Optional[Tuple[int, int]] = None,
                 tokens: Optional[List[Token]] = None):
        self.is_valid = is_valid
        self.error_message = error_message
        self.error_position = error_position
        self.tokens = tokens or []
    
    def __str__(self) -> str:
        if self.is_valid:
            return "Valid SQL syntax"
        else:
            if self.error_position:
                line, col = self.error_position
                return f"Syntax error at line {line}, column {col}: {self.error_message}"
            return f"Syntax error: {self.error_message}"
    
    def __bool__(self) -> bool:
        return self.is_valid


class SyntaxValidator:
    """Main SQL syntax validator combining lexer and parser."""
    
    def __init__(self):
        self.supported_commands = {
            'SELECT', 'UPDATE', 'INSERT', 'DELETE',
            'CREATE TABLE', 'DROP TABLE',
            'BEGIN TRANSACTION', 'COMMIT'
        }
    
    def validate(self, query: str) -> ValidationResult:
        """
        Validate SQL query syntax.
        """
        if not query or not query.strip():
            return ValidationResult(
                is_valid=False,
                error_message="empty query",
                error_position=(1, 1)
            )
        
        try:
            # Lexical analysis (DFA tokenization)
            lexer = SQLLexer(query)
            tokens = lexer.tokenize()
            
            # Check for invalid tokens
            invalid_tokens = [t for t in tokens if t.type == TokenType.INVALID]
            if invalid_tokens:
                first_invalid = invalid_tokens[0]
                return ValidationResult(
                    is_valid=False,
                    error_message=f"syntax error at or near \"{first_invalid.value}\"",
                    error_position=(first_invalid.line, first_invalid.column),
                    tokens=tokens
                )
            
            # Syntax analysis (Grammar parsing)
            parser = SQLParser(tokens)
            parser.parse_with_error()
            
            return ValidationResult(
                is_valid=True,
                error_message="",
                tokens=tokens
            )
            
        except ParseError as e:
            error_line = 1
            error_col = 1
            
            if e.token:
                error_line = e.token.line
                error_col = e.token.column
            
            return ValidationResult(
                is_valid=False,
                error_message=str(e),
                error_position=(error_line, error_col),
                tokens=getattr(lexer, 'tokens', [])
            )
        
        except Exception as e:
            return ValidationResult(
                is_valid=False,
                error_message=f"Unexpected error: {str(e)}",
                error_position=(1, 1)
            )