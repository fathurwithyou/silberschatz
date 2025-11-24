import pytest
from unittest.mock import Mock, MagicMock
from src.optimizer.cost.cost_model import CostModel
from src.core.models.query import QueryTree
from src.core.models.storage import Statistic


class TestCostModel:
    
    @pytest.fixture
    def mock_storage_manager(self):
        mock = Mock()
        mock.get_stats.return_value = None
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
                V={}, 
                min_values={}, 
                max_values={}
            ),
            "orders": Statistic(
                table_name="orders",
                n_r=5000, 
                b_r=100,
                l_r=150,
                f_r=50, 
                V={}, 
                min_values={}, 
                max_values={}
            ),
            "products": Statistic(
                table_name="products",
                n_r=100, 
                b_r=5,
                l_r=100,
                f_r=20, 
                V={}, 
                min_values={}, 
                max_values={}
            )
        }
    
    @pytest.fixture
    def cost_model(self, sample_statistics, mock_storage_manager):
        return CostModel(sample_statistics, mock_storage_manager)
    
    def test_compute_table_scan_cost(self, cost_model):
        table_node = QueryTree(type="table", value="users", children=[])
        cost = cost_model.compute_table_scan_cost(table_node)
        
        assert cost == 50.0
    
    def test_compute_table_scan_cost_unknown_table(self, cost_model):
        table_node = QueryTree(type="table", value="unknown_table", children=[])
        cost = cost_model.compute_table_scan_cost(table_node)
        
        assert cost == 1000.0
    
    def test_compute_selection_cost(self, cost_model):
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="age > 25", children=[table_node])
        
        cost = cost_model.compute_selection_cost(selection_node)
        
        expected = 50.0 + (1000.0 * 0.0001)
        assert cost == pytest.approx(expected)
    
    def test_compute_projection_cost(self, cost_model):
        table_node = QueryTree(type="table", value="users", children=[])
        projection_node = QueryTree(type="projection", value="id, name", children=[table_node])
        
        cost = cost_model.compute_projection_cost(projection_node)
        
        expected = 50.0 + (1000.0 * 0.0001 * 0.5)
        assert cost == pytest.approx(expected)
    
    def test_compute_join_cost(self, cost_model):
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="users.id = orders.user_id", children=[left_table, right_table])
        
        cost = cost_model.compute_join_cost(join_node)
        
        assert cost > 0
        assert cost != float('inf')
    
    def test_compute_join_cost_invalid(self, cost_model):
        join_node = QueryTree(type="join", value="condition", children=[])
        cost = cost_model.compute_join_cost(join_node)
        
        assert cost == float('inf')
    
    def test_estimate_join_algorithm_cost(self, cost_model):
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        
        cost = cost_model.estimate_join_algorithm_cost(
            left_table, right_table, 1000.0, 5000.0, "users.id = orders.user_id"
        )
        
        assert cost > 0
        assert cost != float('inf')
    
    def test_estimate_blocks_with_statistics(self, cost_model):
        table_node = QueryTree(type="table", value="users", children=[])
        blocks = cost_model.estimate_blocks(1000.0, table_node)
        
        assert blocks == 50.0
    
    def test_estimate_blocks_without_statistics(self, cost_model):
        other_node = QueryTree(type="selection", value="condition", children=[])
        blocks = cost_model.estimate_blocks(1000.0, other_node)
        
        assert blocks > 0
    
    def test_nested_loop_join_cost(self, cost_model):
        cost = cost_model.nested_loop_join_cost(50.0, 100.0)
        
        assert cost == 5000.0
    
    def test_hash_join_cost(self, cost_model):
        cost = cost_model.hash_join_cost(50.0, 100.0, 1000.0, 5000.0)
        
        expected = (50.0 + 100.0) + ((1000.0 + 5000.0) * 0.001)
        assert cost == pytest.approx(expected)
    
    def test_merge_join_cost_equijoin(self, cost_model):
        cost = cost_model.merge_join_cost(50.0, 100.0, 1000.0, 5000.0, "col1 = col2")
        
        assert cost != float('inf')
        assert cost > 0
    
    def test_merge_join_cost_non_equijoin(self, cost_model):
        cost = cost_model.merge_join_cost(50.0, 100.0, 1000.0, 5000.0, "col1 > col2")
        
        assert cost == float('inf')
    
    def test_external_sort_cost_internal(self, cost_model):
        cost = cost_model.external_sort_cost(100.0)
        
        assert cost > 0
    
    def test_external_sort_cost_external(self, cost_model):
        cost = cost_model.external_sort_cost(1000000.0)
        
        assert cost > 0
    
    def test_estimate_input_cardinality_table(self, cost_model):
        table_node = QueryTree(type="table", value="users", children=[])
        cardinality = cost_model.estimate_input_cardinality(table_node)
        
        assert cardinality == 1000.0
    
    def test_estimate_input_cardinality_selection(self, cost_model):
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="condition", children=[table_node])
        
        cardinality = cost_model.estimate_input_cardinality(selection_node)
        
        assert cardinality == 500.0
    
    def test_estimate_input_cardinality_join(self, cost_model):
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="condition", children=[left_table, right_table])
        
        cardinality = cost_model.estimate_input_cardinality(join_node)
        
        assert cardinality == 2000.0
    
    def test_get_cost_exception_handling(self, cost_model):
        invalid_node = QueryTree(type="invalid", value="", children=[])
        
        cost = cost_model.get_cost(invalid_node)
        
        assert cost == float('inf')