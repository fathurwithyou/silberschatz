import pytest
from unittest.mock import Mock
from src.optimizer.cost.join_ordering_cost import JoinOrderingOptimizer
from src.core.models.query import QueryTree
from src.optimizer.cost.cost_model import CostModel


class TestJoinOrderingOptimizer:
    
    @pytest.fixture
    def mock_cost_model(self):
        mock = Mock(spec=CostModel)
        mock.estimate_input_cardinality.return_value = 1000.0
        mock.get_cost.return_value = 500.0
        return mock
    
    @pytest.fixture
    def sample_table_trees(self):
        return [
            QueryTree(type="table", value="users", children=[]),
            QueryTree(type="table", value="orders", children=[]),
            QueryTree(type="table", value="products", children=[])
        ]
    
    def test_find_optimal_join_order_single_table(self, mock_cost_model):
        optimizer = JoinOrderingOptimizer(mock_cost_model)
        single_table = [QueryTree(type="table", value="users", children=[])]
        
        result = optimizer.find_optimal_join_order(single_table)
        
        assert result == single_table[0]
    
    def test_find_optimal_join_order_empty_list(self, mock_cost_model):
        optimizer = JoinOrderingOptimizer(mock_cost_model)
        
        result = optimizer.find_optimal_join_order([])
        
        assert result is None
    
    def test_exhaustive_search_small_join(self, mock_cost_model):
        optimizer = JoinOrderingOptimizer(mock_cost_model, max_join_size=4)
        
        tables = [
            QueryTree(type="table", value="users", children=[]),
            QueryTree(type="table", value="orders", children=[])
        ]
        
        result = optimizer.exhaustive_search(tables)
        
        assert result is not None
        assert result.type == "join"
        assert len(result.children) == 2
        mock_cost_model.get_cost.assert_called()
    
    def test_greedy_ordering_large_join(self, mock_cost_model):
        optimizer = JoinOrderingOptimizer(mock_cost_model, max_join_size=2)
        
        tables = [
            QueryTree(type="table", value="users", children=[]),
            QueryTree(type="table", value="orders", children=[]),
            QueryTree(type="table", value="products", children=[]),
            QueryTree(type="table", value="categories", children=[])
        ]
        
        result = optimizer.greedy_ordering(tables)
        
        assert result is not None
        assert result.type == "join"
        mock_cost_model.get_cost.assert_called()
    
    def test_build_left_deep_tree(self, mock_cost_model):
        optimizer = JoinOrderingOptimizer(mock_cost_model)
        
        tables = [
            QueryTree(type="table", value="A", children=[]),
            QueryTree(type="table", value="B", children=[]),
            QueryTree(type="table", value="C", children=[])
        ]
        
        result = optimizer.build_left_deep_tree(tables)
        
        assert result.type == "join"
        assert result.children[0].type == "join"
        assert result.children[0].children[0].type == "table"
        assert result.children[0].children[1].type == "table"
        assert result.children[1].type == "table"
    
    def test_greedy_ordering_with_size_sorting(self, mock_cost_model):
        optimizer = JoinOrderingOptimizer(mock_cost_model)
        
        def side_effect(node):
            if node.value == "small_table":
                return 100.0
            elif node.value == "medium_table":
                return 1000.0
            else:
                return 10000.0
        
        mock_cost_model.estimate_input_cardinality.side_effect = side_effect
        
        tables = [
            QueryTree(type="table", value="large_table", children=[]),
            QueryTree(type="table", value="small_table", children=[]),
            QueryTree(type="table", value="medium_table", children=[])
        ]
        
        result = optimizer.greedy_ordering(tables)
        
        assert result is not None
        mock_cost_model.estimate_input_cardinality.assert_called()
    
    def test_greedy_ordering_single_table(self, mock_cost_model):
        optimizer = JoinOrderingOptimizer(mock_cost_model)
        
        single_table = [QueryTree(type="table", value="users", children=[])]
        
        result = optimizer.greedy_ordering(single_table)
        
        assert result == single_table[0]
    
    def test_greedy_ordering_no_tables(self, mock_cost_model):
        optimizer = JoinOrderingOptimizer(mock_cost_model)
        
        result = optimizer.greedy_ordering([])
        
        assert result is None
    
    def test_algorithm_selection_based_on_size(self, mock_cost_model):
        optimizer_small = JoinOrderingOptimizer(mock_cost_model, max_join_size=4)
        small_tables = [QueryTree(type="table", value=f"table_{i}", children=[]) for i in range(3)]
        
        mock_cost_model.get_cost.reset_mock()
        result_small = optimizer_small.find_optimal_join_order(small_tables)
        
        assert mock_cost_model.get_cost.call_count == 6
        
        optimizer_large = JoinOrderingOptimizer(mock_cost_model, max_join_size=3)
        large_tables = [QueryTree(type="table", value=f"table_{i}", children=[]) for i in range(5)]
        
        mock_cost_model.get_cost.reset_mock()
        result_large = optimizer_large.find_optimal_join_order(large_tables)
        
        assert mock_cost_model.get_cost.call_count < 120
        
        assert result_small is not None
        assert result_large is not None