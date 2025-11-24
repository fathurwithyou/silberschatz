import pytest
from unittest.mock import Mock
from src.optimizer.cost.early_projection import EarlyProjectionRule
from src.core.models.query import QueryTree
from src.optimizer.cost.cost_model import CostModel


class TestEarlyProjectionRule:
    
    @pytest.fixture
    def mock_cost_model(self):
        mock = Mock(spec=CostModel)
        mock.storage_manager = Mock()
        mock.extract_table_name.return_value = "users"
        return mock
    
    def test_is_applicable_projection_over_selection(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="age > 25", children=[table_node])
        projection_node = QueryTree(type="projection", value="id, name", children=[selection_node])
        
        assert rule.is_applicable(projection_node) == True
    
    def test_is_applicable_projection_over_join(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)
        
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="condition", children=[left_table, right_table])
        projection_node = QueryTree(type="projection", value="*", children=[join_node])
        
        assert rule.is_applicable(projection_node) == True
    
    def test_is_applicable_not_applicable_cases(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)

        table_node = QueryTree(type="table", value="users", children=[])
        assert rule.is_applicable(table_node) == False

        empty_proj = QueryTree(type="projection", value="*", children=[])
        assert rule.is_applicable(empty_proj) == False

        table_node = QueryTree(type="table", value="users", children=[])
        multi_child_proj = QueryTree(type="projection", value="*", children=[table_node, table_node])
        assert rule.is_applicable(multi_child_proj) == False

        projection_over_table = QueryTree(type="projection", value="*", children=[table_node])
        assert rule.is_applicable(projection_over_table) == False
    
    def test_push_through_selection(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="age > 25", children=[table_node])
        projection_node = QueryTree(type="projection", value="id, name", children=[selection_node])
        
        result = rule.push_through_selection(projection_node, selection_node)
        
        assert result is not None
        assert result.type == "selection"
        assert result.children[0].type == "projection"
        assert result.children[0].value == "id, name"
    
    def test_push_through_join_with_star_projection(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)
        
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="users.id = orders.user_id", children=[left_table, right_table])
        projection_node = QueryTree(type="projection", value="*", children=[join_node])
        
        mock_schema = Mock()
        mock_schema.columns = [Mock(name="id"), Mock(name="name")]
        mock_schema.table_name = "users"
        
        def get_schema_side_effect(table_name):
            schema = Mock()
            schema.columns = [Mock(name="id"), Mock(name="name")]
            schema.table_name = table_name
            return schema
        
        mock_cost_model.storage_manager.get_table_schema.side_effect = get_schema_side_effect
        
        result = rule.push_through_join(projection_node, join_node)
        
        assert result is not None
        assert result.type == "join"
        assert result.children[0].type == "projection"
        assert result.children[1].type == "projection"
    
    def test_push_through_join_with_specific_columns(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)
        
        left_table = QueryTree(type="table", value="users", children=[])
        right_table = QueryTree(type="table", value="orders", children=[])
        join_node = QueryTree(type="join", value="condition", children=[left_table, right_table])
        projection_node = QueryTree(type="projection", value="id, name, order_date", children=[join_node])
        
        def get_schema_side_effect(table_name):
            schema = Mock()
            if table_name == "users":
                schema.columns = [Mock(name="id"), Mock(name="name")]
                schema.table_name = "users"
            elif table_name == "orders":
                schema.columns = [Mock(name="id"), Mock(name="order_date")]
                schema.table_name = "orders"
            else:
                schema.columns = []
                schema.table_name = table_name
            return schema
        
        mock_cost_model.storage_manager.get_table_schema.side_effect = get_schema_side_effect
        
        result = rule.push_through_join(projection_node, join_node)
        
        assert result is not None
        assert result.type == "join"
        assert result.children[0].type == "projection"
        assert result.children[1].type == "projection"
        
        left_projection = result.children[0]
        right_projection = result.children[1]
        
        assert "id" in left_projection.value
        assert "name" in left_projection.value
        
        assert "order_date" in right_projection.value
    
    def test_apply_with_cost_improvement(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="age > 25", children=[table_node])
        projection_node = QueryTree(type="projection", value="id, name", children=[selection_node])
        
        mock_cost_model.get_cost.side_effect = [100.0, 80.0]
        
        result = rule.apply(projection_node)
        
        assert result is not None
        assert result.type == "selection"
    
    def test_apply_without_cost_improvement(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        selection_node = QueryTree(type="selection", value="age > 25", children=[table_node])
        projection_node = QueryTree(type="projection", value="id, name", children=[selection_node])
        
        mock_cost_model.get_cost.side_effect = [100.0, 120.0]
        
        result = rule.apply(projection_node)
        
        assert result is None
    
    def test_apply_invalid_node(self, mock_cost_model):
        rule = EarlyProjectionRule(mock_cost_model)
        
        table_node = QueryTree(type="table", value="users", children=[])
        
        result = rule.apply(table_node)
        
        assert result is None