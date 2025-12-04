from typing import List, Optional
from .lexer import Token, TokenType


class ParseError(Exception):
    def __init__(self, message: str, token: Optional[Token] = None):
        self.message = message
        self.token = token
        super().__init__(self._format_message())
    
    def _format_message(self) -> str:
        if self.token and self.token.value:
            return f"syntax error at or near \"{self.token.value}\""
        else:
            return f"syntax error"


class SQLParser:
    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.current = 0
        self.current_token = self.tokens[0] if tokens else None
    
    def parse(self) -> bool:
        """Parse the token stream and validate syntax."""
        try:
            self._parse_statement()
            self._expect(TokenType.EOF)
            return True
        except ParseError:
            return False
    
    def parse_with_error(self) -> None:
        """Parse and raise detailed error if syntax is invalid."""
        self._parse_statement()
        self._expect(TokenType.EOF)
    
    def _parse_statement(self) -> None:
        """
        <statement> ::= <select_statement> | <insert_statement> | <update_statement> | 
                       <delete_statement> | <create_statement> | <drop_statement> | 
                       <begin_statement> | <commit_statement> | <abort_statement>
        """
        if not self.current_token:
            raise ParseError("empty query")
        
        token_type = self.current_token.type
        
        if token_type == TokenType.SELECT:
            self._parse_select_statement()
        elif token_type == TokenType.INSERT:
            self._parse_insert_statement()
        elif token_type == TokenType.UPDATE:
            self._parse_update_statement()
        elif token_type == TokenType.DELETE:
            self._parse_delete_statement()
        elif token_type == TokenType.CREATE:
            self._parse_create_statement()
        elif token_type == TokenType.DROP:
            self._parse_drop_statement()
        elif token_type == TokenType.BEGIN:
            self._parse_begin_statement()
        elif token_type == TokenType.COMMIT:
            self._parse_commit_statement()
        elif token_type == TokenType.ABORT:
            self._parse_abort_statement()
        else:
            raise ParseError(f"syntax error", self.current_token)
    
    def _parse_select_statement(self) -> None:
        """
        <select_statement> ::= SELECT <select_list> FROM <from_clause> 
                              [ <where_clause> ] [ <order_by_clause> ] [ <limit_clause> ]
        """
        self._expect(TokenType.SELECT)
        
        # SELECT clause
        self._parse_select_list()
        
        # FROM clause (optional for some DBs, but we'll require it)
        if self._check(TokenType.FROM):
            self._expect(TokenType.FROM)
            self._parse_from_clause()
        
        # Optional clauses
        if self._check(TokenType.WHERE):
            self._parse_where_clause()
        
        if self._check(TokenType.ORDER):
            self._parse_order_by_clause()
        
        if self._check(TokenType.LIMIT):
            self._parse_limit_clause()
    
    def _parse_select_list(self) -> None:
        """
        <select_list> ::= '*' | <expression> [ AS IDENTIFIER ] { ',' <expression> [ AS IDENTIFIER ] }
        """
        if self._check(TokenType.ASTERISK):
            self._advance()  # SELECT *
        else:
            # Parse column list
            self._parse_expression()
            
            if self._check(TokenType.AS):
                self._advance()
                self._expect(TokenType.IDENTIFIER)
            
            # Additional columns
            while self._check(TokenType.COMMA):
                self._advance()
                self._parse_expression()
                
                if self._check(TokenType.AS):
                    self._advance()
                    self._expect(TokenType.IDENTIFIER)
    
    def _parse_from_clause(self) -> None:
        """
        <from_clause> ::= <join_expression> { ',' <join_expression> }
        """
        self._parse_join_expression()
        
        while self._check(TokenType.COMMA):
            self._advance()
            self._parse_join_expression()

    def _parse_join_expression(self) -> None:
        """
        <join_expression> ::= <table_reference> { <join_clause> }
        """
        self._parse_table_reference()
        
        while self._check_join():
            self._parse_join_clause()
    
    def _parse_table_reference(self) -> None:
        """
        <table_reference> ::= IDENTIFIER [ [ AS ] IDENTIFIER ]
        """
        self._expect(TokenType.IDENTIFIER)
        
        # Optional alias
        if self._check(TokenType.AS):
            self._advance()
            self._expect(TokenType.IDENTIFIER)
        elif self._check(TokenType.IDENTIFIER):
            # Implicit alias
            self._advance()
    
    def _parse_join_clause(self) -> None:
        """
        <join_clause> ::= [ INNER | LEFT [ OUTER ] | RIGHT [ OUTER ] | FULL OUTER ] 
                         JOIN <table_reference> [ ON <expression> ]
        """
        # Optional join type (INNER, LEFT, RIGHT, FULL OUTER)
        if self._check(TokenType.INNER):
            self._advance()
        elif self._check(TokenType.LEFT):
            self._advance()
            if self._check(TokenType.OUTER):
                self._advance()
        elif self._check(TokenType.RIGHT):
            self._advance()
            if self._check(TokenType.OUTER):
                self._advance()
        elif self._check(TokenType.FULL):
            self._advance()
            self._expect(TokenType.OUTER)
        
        self._expect(TokenType.JOIN)
        self._parse_table_reference()
        
        # JOIN condition
        if self._check(TokenType.ON):
            self._advance()
            self._parse_expression()
    
    def _parse_where_clause(self) -> None:
        """
        <where_clause> ::= WHERE <expression>
        """
        self._expect(TokenType.WHERE)
        self._parse_expression()
    
    def _parse_order_by_clause(self) -> None:
        """
        <order_by_clause> ::= ORDER BY <expression> [ ASC | DESC ] { ',' <expression> [ ASC | DESC ] }
        """
        self._expect(TokenType.ORDER)
        self._expect(TokenType.BY)
        
        # Parse order expressions
        self._parse_expression()
        
        # Optional ASC/DESC
        if self._check(TokenType.ASC) or self._check(TokenType.DESC):
            self._advance()
        
        # Additional order expressions
        while self._check(TokenType.COMMA):
            self._advance()
            self._parse_expression()
            
            if self._check(TokenType.ASC) or self._check(TokenType.DESC):
                self._advance()
    
    def _parse_limit_clause(self) -> None:
        """
        <limit_clause> ::= LIMIT NUMBER_LITERAL
        """
        self._expect(TokenType.LIMIT)
        self._expect(TokenType.NUMBER_LITERAL)
    
    def _parse_insert_statement(self) -> None:
        """
        <insert_statement> ::= INSERT INTO IDENTIFIER [ '(' <column_list> ')' ] 
                              VALUES '(' <value_list> ')'
        <column_list> ::= IDENTIFIER { ',' IDENTIFIER }
        <value_list> ::= <factor> { ',' <factor> }
        """
        self._expect(TokenType.INSERT)
        self._expect(TokenType.INTO)
        self._expect(TokenType.IDENTIFIER)
        
        # Optional column list
        if self._check(TokenType.LEFT_PAREN):
            self._advance()
            self._expect(TokenType.IDENTIFIER)
            
            while self._check(TokenType.COMMA):
                self._advance()
                self._expect(TokenType.IDENTIFIER)
            
            self._expect(TokenType.RIGHT_PAREN)
        
        # VALUES clause
        self._expect(TokenType.VALUES)
        self._expect(TokenType.LEFT_PAREN)
        self._parse_factor()
        
        while self._check(TokenType.COMMA):
            self._advance()
            self._parse_factor()
        
        self._expect(TokenType.RIGHT_PAREN)
    
    def _parse_update_statement(self) -> None:
        """
        <update_statement> ::= UPDATE IDENTIFIER SET <assignment_list> [ <where_clause> ]
        <assignment_list> ::= <assignment> { ',' <assignment> }
        <assignment> ::= IDENTIFIER '=' <expression>
        """
        self._expect(TokenType.UPDATE)
        self._expect(TokenType.IDENTIFIER)
        self._expect(TokenType.SET)
        
        # SET clause
        self._expect(TokenType.IDENTIFIER)
        self._expect(TokenType.EQUALS)
        self._parse_expression()
        
        while self._check(TokenType.COMMA):
            self._advance()
            self._expect(TokenType.IDENTIFIER)
            self._expect(TokenType.EQUALS)
            self._parse_expression()
        
        # Optional WHERE clause
        if self._check(TokenType.WHERE):
            self._parse_where_clause()
    
    def _parse_delete_statement(self) -> None:
        """
        <delete_statement> ::= DELETE FROM IDENTIFIER [ <where_clause> ]
        """
        self._expect(TokenType.DELETE)
        self._expect(TokenType.FROM)
        self._expect(TokenType.IDENTIFIER)
        
        # Optional WHERE clause
        if self._check(TokenType.WHERE):
            self._parse_where_clause()
    
    def _parse_create_statement(self) -> None:
        """
        <create_statement> ::= CREATE TABLE IDENTIFIER '(' <column_definition_list> ')'
        <column_definition_list> ::= <column_definition> { ',' <column_definition> }
        """
        self._expect(TokenType.CREATE)
        self._expect(TokenType.TABLE)
        self._expect(TokenType.IDENTIFIER)
        
        self._expect(TokenType.LEFT_PAREN)
        
        self._parse_column_definition()
        
        while self._check(TokenType.COMMA):
            self._advance()
            self._parse_column_definition()
        
        self._expect(TokenType.RIGHT_PAREN)

        if self._check(TokenType.SEMICOLON):
            self._advance()
    
    def _parse_column_definition(self) -> None:
        """
        <column_definition> ::= IDENTIFIER <data_type> [<column_constraints>]
        """
        self._expect(TokenType.IDENTIFIER)
        self._parse_data_type()
        self._parse_column_constraints()
    
    def _parse_data_type(self) -> None:
        """
        <data_type> ::= INTEGER | VARCHAR '(' NUMBER ')' | CHAR '(' NUMBER ')' | FLOAT | INT
        """
        if (self._check(TokenType.INTEGER) or self._check(TokenType.INT) or 
            self._check(TokenType.FLOAT)):
            self._advance()
        elif (self._check(TokenType.VARCHAR) or self._check(TokenType.CHAR)):
            self._advance()
            self._expect(TokenType.LEFT_PAREN)
            self._expect(TokenType.NUMBER_LITERAL)
            self._expect(TokenType.RIGHT_PAREN)
        
    
    def _parse_column_constraints(self) -> None:
        """
        <column_constraints> ::= {<constraint>}
        <constraint> ::= PRIMARY KEY | NOT NULL | NULL | REFERENCES <table_ref> [<fk_actions>]
        <table_ref> ::= IDENTIFIER '(' IDENTIFIER ')'
        """
        while (self._check(TokenType.PRIMARY) or self._check(TokenType.NOT) or 
               self._check(TokenType.NULL) or self._check(TokenType.REFERENCES)):
            
            if self._check(TokenType.PRIMARY):
                # PRIMARY KEY constraint
                self._advance()
                self._expect(TokenType.KEY)
            elif self._check(TokenType.NOT):
                # NOT NULL constraint
                self._advance()
                self._expect(TokenType.NULL)
            elif self._check(TokenType.NULL):
                # NULL constraint
                self._advance()
            elif self._check(TokenType.REFERENCES):
                # REFERENCES constraint (foreign key)
                self._advance()
                self._expect(TokenType.IDENTIFIER)
                self._expect(TokenType.LEFT_PAREN)
                self._expect(TokenType.IDENTIFIER)
                self._expect(TokenType.RIGHT_PAREN)
                
                # Parse optional foreign key actions
                self._parse_foreign_key_actions()

    def _parse_foreign_key_actions(self) -> None:
        """
        <fk_actions> ::= [ON DELETE <action>] [ON UPDATE <action>]
        <action> ::= CASCADE | RESTRICT | SET NULL | NO ACTION
        """
        # ON DELETE action
        if self._check(TokenType.ON):
            self._advance()
            if self._check(TokenType.DELETE):
                self._advance()
                self._parse_fk_action()
            elif self._check(TokenType.UPDATE):
                # Need to backtrack and let the UPDATE clause handle this
                self.current -= 1  # Go back one token
                self.current_token = self.tokens[self.current] if self.current < len(self.tokens) else None
        
        # ON UPDATE action
        if self._check(TokenType.ON):
            self._advance()
            if self._check(TokenType.UPDATE):
                self._advance()
                self._parse_fk_action()

    def _parse_fk_action(self) -> None:
        """
        <action> ::= CASCADE | RESTRICT | SET NULL | NO ACTION
        """
        if self._check(TokenType.CASCADE):
            self._advance()
        elif self._check(TokenType.RESTRICT):
            self._advance()
        elif self._check(TokenType.SET):
            self._advance()
            self._expect(TokenType.NULL)
        elif self._check(TokenType.NO):
            self._advance()
            self._expect(TokenType.ACTION)
        else:
            raise ParseError(f"Expected foreign key action (CASCADE, RESTRICT, SET NULL, NO ACTION)", self.current_token)

    
    def _parse_drop_statement(self) -> None:
        """
        <drop_statement> ::= DROP TABLE IDENTIFIER
        """
        self._expect(TokenType.DROP)
        self._expect(TokenType.TABLE)
        self._expect(TokenType.IDENTIFIER)

        if self._check(TokenType.CASCADE) or self._check(TokenType.RESTRICT):
            self._advance()
    
    def _parse_begin_statement(self) -> None:
        """
        <begin_statement> ::= BEGIN TRANSACTION
        """
        self._expect(TokenType.BEGIN)
        self._expect(TokenType.TRANSACTION)
    
    def _parse_commit_statement(self) -> None:
        """
        <commit_statement> ::= COMMIT
        """
        self._expect(TokenType.COMMIT)
    
    def _parse_abort_statement(self) -> None:
        """
        <abort_statement> ::= ABORT
        """
        self._expect(TokenType.ABORT)
        
    def _parse_expression(self) -> None:
        """
        <expression> ::= <term> { ( AND | OR ) <term> }
        """
        self._parse_term()
        
        # Handle AND/OR operators
        while self._check(TokenType.AND) or self._check(TokenType.OR):
            self._advance()
            self._parse_term()
    
    def _parse_term(self) -> None:
        """
        <term> ::= [ NOT ] <factor> [ <comparison_operator> <factor> | LIKE <factor> ]
        <comparison_operator> ::= '=' | '!=' | '<' | '>' | '<=' | '>='
        """
        # Handle NOT
        if self._check(TokenType.NOT):
            self._advance()
        
        self._parse_factor()
        
        # Handle comparison operators
        if self._check_comparison_operator():
            self._advance()
            self._parse_factor()
        elif self._check(TokenType.LIKE):
            self._advance()
            self._parse_factor()
    
    def _parse_factor(self) -> None:
        """
        <factor> ::= IDENTIFIER [ '.' IDENTIFIER ] | STRING_LITERAL | NUMBER_LITERAL | 
                    NULL | '(' <expression> ')' | '*'
        """
        if self._check(TokenType.IDENTIFIER):
            self._advance()
            # Handle table.column
            if self._check(TokenType.DOT):
                self._advance()
                self._expect(TokenType.IDENTIFIER)
        elif self._check(TokenType.STRING_LITERAL) or self._check(TokenType.NUMBER_LITERAL):
            self._advance()
        elif self._check(TokenType.NULL):
            self._advance()
        elif self._check(TokenType.LEFT_PAREN):
            self._advance()
            self._parse_expression()
            self._expect(TokenType.RIGHT_PAREN)
        elif self._check(TokenType.ASTERISK):
            self._advance()
        else:
            raise ParseError("syntax error", self.current_token)
    
    def _check_join(self) -> bool:
        """Check if current token starts a JOIN clause."""
        return (self._check(TokenType.JOIN) or 
                self._check(TokenType.INNER) or 
                self._check(TokenType.LEFT) or 
                self._check(TokenType.RIGHT) or 
                self._check(TokenType.FULL))
    
    def _check_comparison_operator(self) -> bool:
        """Check if current token is a comparison operator."""
        return (self._check(TokenType.EQUALS) or 
                self._check(TokenType.NOT_EQUALS) or 
                self._check(TokenType.LESS_THAN) or 
                self._check(TokenType.GREATER_THAN) or 
                self._check(TokenType.LESS_EQUAL) or 
                self._check(TokenType.GREATER_EQUAL))
    
    def _check(self, token_type: TokenType) -> bool:
        """Check if current token is of given type."""
        return bool(self.current_token and self.current_token.type == token_type)
    
    def _advance(self) -> Optional[Token]:
        """Consume current token and move to next."""
        if self.current_token and self.current_token.type != TokenType.EOF:
            previous = self.current_token
            self.current += 1
            if self.current < len(self.tokens):
                self.current_token = self.tokens[self.current]
            else:
                self.current_token = None
            return previous
        return self.current_token
    
    def _expect(self, token_type: TokenType) -> Token:
        """Expect a specific token type and consume it."""
        if not self._check(token_type):
            if self.current_token:
                raise ParseError("syntax error", self.current_token)
            else:
                raise ParseError("unexpected end of input")
        
        token = self._advance()
        if token is None:
            raise ParseError("unexpected end of input")
        return token
