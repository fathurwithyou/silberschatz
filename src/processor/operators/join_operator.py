from core.models.result import Rows
from core.models import Condition, ComparisonOperator
from typing import List
import re

class JoinOperator:
    def __init__(self):
        pass
    
    def execute(self, outer_relation: Rows, inner_relation: Rows, conditions: str) -> Rows:
        raise NotImplementedError
        