from typing import Optional, Dict
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic, ComparisonOperator
from src.core import IStorageManager


class SelectivityEstimator:

    def __init__(self, storage_manager: Optional[IStorageManager] = None):
        self._storage_manager = storage_manager

    def estimate_selection_selectivity(self, selection_node: QueryTree) -> float:
        if not selection_node.value or not self._storage_manager:
            return 0.33

        condition = selection_node.value.lower()
        total_selectivity = 1.0

        conditions = condition.split(' and ')
        for cond in conditions:
            cond = cond.strip()
            operator = self._parse_operator_from_condition(cond)

            if operator:
                col_selectivity = self._estimate_operator_selectivity(cond, operator)
                total_selectivity *= col_selectivity
            elif 'like' in cond:
                total_selectivity *= 0.20 if '%' in cond else 0.10
            elif ' in ' in cond:
                if '(' in cond and ')' in cond:
                    in_clause = cond[cond.index('('):cond.index(')')+1]
                    num_items = in_clause.count(',') + 1
                    total_selectivity *= min(num_items * 0.1, 0.5)
                else:
                    total_selectivity *= 0.20
            elif 'is null' in cond:
                total_selectivity *= 0.05
            elif 'is not null' in cond:
                total_selectivity *= 0.95
            else:
                total_selectivity *= 0.33

        return min(total_selectivity, 1.0)

    def _parse_operator_from_condition(self, condition: str) -> Optional[ComparisonOperator]:
        condition = condition.strip()
        if '>=' in condition:
            return ComparisonOperator.GE
        elif '<=' in condition:
            return ComparisonOperator.LE
        elif '!=' in condition:
            return ComparisonOperator.NE
        elif '=' in condition:
            return ComparisonOperator.EQ
        elif '>' in condition:
            return ComparisonOperator.GT
        elif '<' in condition:
            return ComparisonOperator.LT
        return None

    def _estimate_operator_selectivity(self, condition: str, operator: ComparisonOperator) -> float:
        if not self._storage_manager:
            if operator == ComparisonOperator.EQ:
                return 0.10
            elif operator == ComparisonOperator.NE:
                return 0.90
            elif operator in [ComparisonOperator.GT, ComparisonOperator.LT,
                            ComparisonOperator.GE, ComparisonOperator.LE]:
                return 0.33
            return 0.33

        if operator == ComparisonOperator.EQ:
            return self._estimate_equality_selectivity(condition, operator)
        elif operator == ComparisonOperator.NE:
            eq_selectivity = self._estimate_equality_selectivity(condition, ComparisonOperator.EQ)
            return 1.0 - eq_selectivity
        elif operator in [ComparisonOperator.GT, ComparisonOperator.LT,
                         ComparisonOperator.GE, ComparisonOperator.LE]:
            return self._estimate_range_selectivity(condition, operator)
        return 0.33

    def _estimate_range_selectivity(self, condition: str, operator: ComparisonOperator) -> float:
        if not self._storage_manager:
            return 0.33

        operator_str = operator.value
        parts = condition.split(operator_str)
        if len(parts) != 2:
            return 0.33

        left_part = parts[0].strip()
        right_part = parts[1].strip()

        try:
            comparison_value = float(right_part.strip("'\""))
        except (ValueError, AttributeError):
            comparison_value = right_part.strip("'\"")

        if '.' in left_part:
            table_col = left_part.split('.')
            if len(table_col) == 2:
                table_name = table_col[0].strip()
                column_name = table_col[1].strip()

                try:
                    stats = self._storage_manager.get_stats(table_name)

                    if (stats.min_values and column_name in stats.min_values and
                        stats.max_values and column_name in stats.max_values):

                        min_val = stats.min_values[column_name]
                        max_val = stats.max_values[column_name]

                        try:
                            if isinstance(comparison_value, (int, float)):
                                min_val = float(min_val)
                                max_val = float(max_val)

                            if max_val == min_val:
                                if operator in [ComparisonOperator.GT, ComparisonOperator.LT]:
                                    return 0.0 if comparison_value == min_val else (1.0 if comparison_value < min_val else 0.0)
                                else:
                                    return 1.0 if comparison_value == min_val else (1.0 if comparison_value < min_val else 0.0)

                            if operator == ComparisonOperator.GT:
                                if comparison_value >= max_val:
                                    return 0.0
                                elif comparison_value <= min_val:
                                    return 1.0
                                else:
                                    return (max_val - comparison_value) / (max_val - min_val)

                            elif operator == ComparisonOperator.GE:
                                if comparison_value > max_val:
                                    return 0.0
                                elif comparison_value <= min_val:
                                    return 1.0
                                else:
                                    return (max_val - comparison_value) / (max_val - min_val)

                            elif operator == ComparisonOperator.LT:
                                if comparison_value <= min_val:
                                    return 0.0
                                elif comparison_value >= max_val:
                                    return 1.0
                                else:
                                    return (comparison_value - min_val) / (max_val - min_val)

                            elif operator == ComparisonOperator.LE:
                                if comparison_value < min_val:
                                    return 0.0
                                elif comparison_value >= max_val:
                                    return 1.0
                                else:
                                    return (comparison_value - min_val) / (max_val - min_val)

                        except (TypeError, ValueError):
                            pass
                except Exception:
                    pass

        return 0.33

    def _estimate_equality_selectivity(self, condition: str, operator: ComparisonOperator) -> float:
        if not self._storage_manager:
            return 0.10

        operator_str = operator.value
        parts = condition.split(operator_str)
        if len(parts) != 2:
            return 0.10

        left_part = parts[0].strip()

        if '.' in left_part:
            table_col = left_part.split('.')
            if len(table_col) == 2:
                table_name = table_col[0].strip()
                column_name = table_col[1].strip()

                try:
                    stats = self._storage_manager.get_stats(table_name)
                    if column_name in stats.V:
                        distinct_values = stats.V[column_name]
                        return max(1.0 / distinct_values, 0.001)
                except Exception:
                    pass

        return 0.10
