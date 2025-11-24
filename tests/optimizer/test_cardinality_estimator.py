import pytest
from unittest.mock import Mock, MagicMock
from src.optimizer.cost.cardinality_estimator import CardinalityEstimator
from src.core.models.storage import Statistic, Condition, ComparisonOperator


class TestCardinalityEstimator:
    
    @pytest.fixture
    def mock_storage_manager(self):
        mock = Mock()
        mock.get_stats.return_value = None
        mock.get_table_schema.return_value = None
        return mock
    
    @pytest.fixture
    def sample_statistics(self):
        return {
            "users": Statistic(
                table_name="users",
                n_r=1000,
                b_r=50,
                l_r=200,
                f_r=20,
                V={"id": 1000, "age": 50, "name": 800},
                min_values={"age": 18, "salary": 30000},
                max_values={"age": 65, "salary": 100000}
            ),
            "orders": Statistic(
                table_name="orders",
                n_r=5000,
                b_r=100,
                l_r=150,
                f_r=50,
                V={"order_id": 5000, "user_id": 1000, "status": 5},
                min_values={},
                max_values={}
            )
        }
    
    def test_estimate_selection_cardinality_no_conditions(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        result = estimator.estimate_selection_cardinality("users", [])
        
        assert result == 1000.0
    
    def test_estimate_selection_cardinality_with_equality_condition(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        condition = Condition("age", ComparisonOperator.EQ, 25)
        result = estimator.estimate_selection_cardinality("users", [condition])
        
        expected = 1000.0 * (1.0 / 50.0)
        assert result == expected
    
    def test_estimate_selection_cardinality_table_not_found(self, mock_storage_manager):
        statistics = {}
        estimator = CardinalityEstimator(statistics, mock_storage_manager)
        condition = Condition("age", ComparisonOperator.EQ, 25)
        result = estimator.estimate_selection_cardinality("unknown_table", [condition])
        
        assert result == 100.0
    
    def test_estimate_condition_selectivity_equality(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        stats = sample_statistics["users"]
        condition = Condition("age", ComparisonOperator.EQ, 25)
        
        selectivity = estimator.estimate_condition_selectivity(stats, condition)
        expected = 1.0 / 50.0
        assert selectivity == expected
    
    def test_estimate_condition_selectivity_inequality(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        stats = sample_statistics["users"]
        condition = Condition("age", ComparisonOperator.NE, 25)
        
        selectivity = estimator.estimate_condition_selectivity(stats, condition)
        eq_selectivity = 1.0 / 50.0
        expected = 1.0 - eq_selectivity
        assert selectivity == expected
    
    def test_estimate_condition_selectivity_range_gt(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        stats = sample_statistics["users"]
        condition = Condition("age", ComparisonOperator.GT, 30)
        
        selectivity = estimator.estimate_condition_selectivity(stats, condition)
        assert 0.01 <= selectivity <= 0.99
    
    def test_estimate_condition_selectivity_unknown_column(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        stats = sample_statistics["users"]
        condition = Condition("unknown_col", ComparisonOperator.EQ, "value")
        
        selectivity = estimator.estimate_condition_selectivity(stats, condition)
        assert selectivity == 0.05
    
    def test_estimate_join_cardinality_equijoin(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        left_stats = sample_statistics["users"]
        right_stats = sample_statistics["orders"]
        join_condition = "users.id = orders.user_id"
        
        result = estimator.estimate_join_cardinality(left_stats, right_stats, join_condition)
        
        expected = (1000.0 * 5000.0) / max(1000, 1000)
        assert result == expected
    
    def test_estimate_join_cardinality_non_equijoin(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        left_stats = sample_statistics["users"]
        right_stats = sample_statistics["orders"]
        join_condition = "users.age > orders.user_id"
        
        result = estimator.estimate_join_cardinality(left_stats, right_stats, join_condition)
        
        expected = 1000.0 * 5000.0 * 0.1
        assert result == expected
    
    def test_is_equijoin_detection(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        
        assert estimator.is_equijoin("table1.col1 = table2.col2") == True
        assert estimator.is_equijoin("col1 = col2") == True
        assert estimator.is_equijoin("table1.col1 > table2.col2") == False
        assert estimator.is_equijoin("table1.col1 != table2.col2") == False
        assert estimator.is_equijoin("") == False
    
    def test_extract_join_columns(self, sample_statistics, mock_storage_manager):
        estimator = CardinalityEstimator(sample_statistics, mock_storage_manager)
        
        result = estimator.extract_join_columns("users.id = orders.user_id")
        assert result == ("users.id", "orders.user_id")
        
        result = estimator.extract_join_columns("id = user_id")
        assert result == ("id", "user_id")
        
        result = estimator.extract_join_columns("invalid condition")
        assert result is None
    
    def test_conservative_fallback_estimation(self, mock_storage_manager):
        statistics = {}
        estimator = CardinalityEstimator(statistics, mock_storage_manager)
        
        conditions = [Condition("age", ComparisonOperator.EQ, 25)]
        result = estimator.conservative_fallback_estimation("unknown", conditions)
        assert result == 100.0
        
        result = estimator.conservative_fallback_estimation("unknown", [])
        assert result == 1000.0