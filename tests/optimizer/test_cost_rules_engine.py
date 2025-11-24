import pytest
from unittest.mock import Mock, MagicMock
from src.optimizer.cost.cost_rules_engine import (
    CostBasedRuleEngine, 
    CostBasedProjectionPushdown, 
    CostBasedJoinReordering
)
from src.core.models.query import QueryTree
from src.optimizer.cost.cost_model import CostModel


class TestCostBasedRuleEngine:
    
    @pytest.fixture
    def mock_cost_model(self):
        mock = Mock(spec=CostModel)
        mock.get_cost.return_value = 100.0
        mock.storage_manager = Mock()
        return mock
    
    @pytest.fixture
    def mock_rule(self):
        mock = Mock()
        mock.is_applicable.return_value = True
        mock.apply.return_value = QueryTree(type="table", value="optimized", children=[])
        mock.name = "test_rule"
        return mock
    
    def test_apply_cost_based_rules_no_improvement(self, mock_cost_model, mock_rule):
        engine = CostBasedRuleEngine(mock_cost_model)
        initial_tree = QueryTree(type="table", value="initial", children=[])
        
        mock_cost_model.get_cost.return_value = 100.0
        
        result = engine.apply_cost_based_rules(initial_tree, [mock_rule])
        
        assert result == initial_tree
        mock_rule.is_applicable.assert_called()
        mock_rule.apply.assert_called()
    
    def test_apply_cost_based_rules_with_improvement(self, mock_cost_model, mock_rule):
        engine = CostBasedRuleEngine(mock_cost_model)
        initial_tree = QueryTree(type="table", value="initial", children=[])
        
        optimized_tree = QueryTree(type="optimized", value="optimized", children=[])
        mock_rule.apply.return_value = optimized_tree
        
        mock_cost_model.get_cost.side_effect = [100.0, 80.0]
        
        result = engine.apply_cost_based_rules(initial_tree, [mock_rule])
        
        assert result.type == "optimized"
        assert mock_rule.apply.called
    
    def test_apply_cost_based_rules_max_iterations(self, mock_cost_model, mock_rule):
        engine = CostBasedRuleEngine(mock_cost_model, max_iterations=1)
        initial_tree = QueryTree(type="table", value="initial", children=[])
        
        mock_cost_model.get_cost.return_value = 50.0
        
        result = engine.apply_cost_based_rules(initial_tree, [mock_rule])
        
        assert mock_rule.apply.call_count == 1


class TestCostBasedProjectionPushdown:
    
    @pytest.fixture
    def mock_cost_model(self):
        mock = Mock(spec=CostModel)
        mock.storage_manager = Mock()
        mock.extract_table_name.return_value = "users"
        return mock
    
    def test_is_applicable_projection_over_selection(self, mock_cost_model):
        rule = CostBasedProjectionPushdown(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="age > 25", children=[table_node])
        projection_node = QueryTree(type="projection", value="id, name", children=[selection_node])
        
        assert rule.is_applicable(projection_node) == True
    
    def test_is_applicable_projection_over_join(self, mock_cost_model):
        rule = CostBasedProjectionPushdown(mock_cost_model)
        
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="condition", children=[left_table, right_table])
        projection_node = QueryTree(type="projection", value="*", children=[join_node])
        
        assert rule.is_applicable(projection_node) == True
    
    def test_is_applicable_not_applicable(self, mock_cost_model):
        rule = CostBasedProjectionPushdown(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        assert rule.is_applicable(table_node) == False
        
        multi_child_proj = QueryTree(type="projection", value="*", children=[table_node, table_node])
        assert rule.is_applicable(multi_child_proj) == False
    
    def test_apply_projection_pushdown_cost_improvement(self, mock_cost_model):
        rule = CostBasedProjectionPushdown(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="age > 25", children=[table_node])
        projection_node = QueryTree(type="projection", value="id, name", children=[selection_node])
        
        mock_schema = Mock()
        mock_schema.columns = [Mock(name="id"), Mock(name="name"), Mock(name="age")]
        mock_schema.table_name = "users"
        mock_cost_model.storage_manager.get_table_schema.return_value = mock_schema
        
        mock_cost_model.get_cost.side_effect = [100.0, 80.0]
        
        result = rule.apply(projection_node)
        
        assert result is not None
        assert result.type == "selection"
        assert result.children[0].type == "projection"
        assert result.children[0].value == "id, name"
    
    def test_apply_projection_pushdown_no_improvement(self, mock_cost_model):
        rule = CostBasedProjectionPushdown(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="age > 25", children=[table_node])
        projection_node = QueryTree(type="projection", value="id, name", children=[selection_node])
        
        mock_cost_model.get_cost.side_effect = [100.0, 120.0]
        
        result = rule.apply(projection_node)
        
        assert result is None


class TestCostBasedJoinReordering:
    
    @pytest.fixture
    def mock_cost_model(self):
        mock = Mock(spec=CostModel)
        return mock
    
    def test_is_applicable_join_with_table_children(self, mock_cost_model):
        rule = CostBasedJoinReordering(mock_cost_model)
        
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="condition", children=[left_table, right_table])
        
        assert rule.is_applicable(join_node) == True
    
    def test_apply_join_reordering_cost_improvement(self, mock_cost_model):
        rule = CostBasedJoinReordering(mock_cost_model)
        
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="condition", children=[left_table, right_table])
        
        mock_cost_model.get_cost.side_effect = [100.0, 80.0]
        
        result = rule.apply(join_node)
        
        assert result is not None
        assert result.type == "join"
        assert result.children[0] == right_table
        assert result.children[1] == left_table
    
    def test_apply_join_reordering_no_improvement(self, mock_cost_model):
        rule = CostBasedJoinReordering(mock_cost_model)
        
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="condition", children=[left_table, right_table])
        
        mock_cost_model.get_cost.side_effect = [100.0, 120.0]
        
        result = rule.apply(join_node)
        
        assert result is None