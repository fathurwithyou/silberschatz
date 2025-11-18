from src.core.models.result import Rows
from ..conditions import ConditionEvaluator

class SelectionOperator:
    def execute(self, rows: Rows, conditions: str) -> Rows:
        evaluator = ConditionEvaluator(rows.schema)
        filtered_data = []
        for row in rows.data:
            if evaluator.evaluate(conditions, row):
                filtered_data.append(row)
                
        return Rows(schema=rows.schema, 
                    rows_count=len(filtered_data), 
                    data=filtered_data)