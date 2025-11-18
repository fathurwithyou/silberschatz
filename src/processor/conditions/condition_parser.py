from typing import List
import re
from .condition import ConditionNode, SimpleCondition, ComplexCondition
from core.models import ComparisonOperator, TableSchema

class ConditionParser:
    _instance = None

    def __init__(self, schemas: List[TableSchema]):
        self.tokens = []
        self.current = 0
        self.schemas = schemas
        
    @staticmethod
    def get_instance(schemas: List[TableSchema]) -> "ConditionParser":
        if ConditionParser._instance is None:
            ConditionParser._instance = ConditionParser(schemas)
        return ConditionParser._instance
        
    def parse(self, condition_str: str) -> ConditionNode:
        if not condition_str:
            raise ValueError("Empty condition string")
        
        # Tokenize
        self.tokens = self._tokenize(condition_str)
        self.current = 0
        
        # Parsing
        result = self._parse_expression()
        
        if self.current < len(self.tokens):
            raise SyntaxError(f"Unexpected token at end: {self.tokens[self.current]}")
            
        return result

    def _tokenize(self, text: str) -> List[str]:
        token_pattern = re.compile(
            r"('[^']*')|(\"[^\"]*\")|(\(|\))|(>=|<=|<>|=|>|<)|(\bAND\b|\bOR\b)|([a-zA-Z0-9_.]+)"
        )
        tokens = []
        for match in token_pattern.finditer(text):
            token = match.group(0)
            tokens.append(token)
        return tokens

    def _current_token(self):
        if self.current < len(self.tokens):
            return self.tokens[self.current].upper()
        return None
    
    def _current_token_raw(self):
        if self.current < len(self.tokens):
            return self.tokens[self.current]
        return None

    def _advance(self):
        self.current += 1

    def _check(self, expected_token: str) -> bool:
        current = self._current_token()
        return current is not None and current == expected_token.upper()
    
    def _expect(self, expected_token: str) -> str:
        if not self._check(expected_token):
            current = self._current_token_raw()
            if current:
                raise SyntaxError(f"Expected '{expected_token}', found '{current}'")
            else:
                raise SyntaxError(f"Expected '{expected_token}', found end of input")
        
        token = self._current_token_raw()
        if token is None:
            raise SyntaxError(f"Expected '{expected_token}', found end of input")
        self._advance()
        return token

    # --- Recursive Descent ---

    # <expression> ::= <term> { OR <term> }
    def _parse_expression(self) -> ConditionNode:
        node = self._parse_term()
        nodes = [node]
        
        while self._check('OR'):
            self._expect('OR')
            node = self._parse_term()
            nodes.append(node)
            # left_node = ComplexCondition('OR', [left_node, right_node])
        
        if len(nodes) == 1:
            return nodes[0]
        
        return ComplexCondition('OR', nodes)

    # <term> ::= <factor> { AND <factor> }
    def _parse_term(self) -> ConditionNode:
        node = self._parse_factor()
        nodes = [node]

        while self._check('AND'):
            self._expect('AND')
            node = self._parse_factor()
            nodes.append(node)
        
        if len(nodes) == 1:
            return nodes[0]
        
        return ComplexCondition('AND', nodes)

    # <factor> ::= ( <expression> ) | <simple_condition>
    def _parse_factor(self) -> ConditionNode:
        if self._check('('):
            self._expect('(')
            node = self._parse_expression()
            self._expect(')')
            return node
        else:
            return self._parse_simple_condition()

    # <simple_condition> ::= identifier operator value
    def _parse_simple_condition(self) -> ConditionNode:
        left = self._current_token_raw()
        if left is None:
            raise SyntaxError("Expected identifier")
        self._advance()
        
        op_str = self._current_token_raw()
        if op_str not in ('=', '>', '<', '>=', '<=', '<>'):
            raise SyntaxError(f"Expected operator, found {op_str}")
        self._advance()
        
        right = self._current_token_raw()
        if right is None:
            raise SyntaxError("Expected value")
        self._advance()
        
        operator = self._convert_operator(op_str)
        
        return SimpleCondition(left, operator, right, self.schemas)
    
    def _convert_operator(self, op_str: str) -> ComparisonOperator:
        operator_map = {
            '=': ComparisonOperator.EQ,
            '>': ComparisonOperator.GT,
            '<': ComparisonOperator.LT,
            '>=': ComparisonOperator.GE,
            '<=': ComparisonOperator.LE,
            '<>': ComparisonOperator.NE
        }
        
        if op_str not in operator_map:
            raise SyntaxError(f"Unsupported operator: {op_str}")
            
        return operator_map[op_str]