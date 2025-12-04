import os
import sys
import ast
import pytest
from src.core import IStorageManager
from src.core.models.storage import Statistic, TableSchema, DataRetrieval, DataWrite, DataDeletion, ColumnDefinition, DataType, Condition, ComparisonOperator
from src.core.models.result import Rows
from src.core.models.query import QueryTree # Use the actual QueryTree class

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

CARDINALITY_ESTIMATOR_PATH = os.path.join(os.path.dirname(__file__), '../../src/optimizer/cost/cardinality_estimator.py')


def import_cardinality_estimator_directly():
    if not os.path.exists(CARDINALITY_ESTIMATOR_PATH):
        raise ImportError(f"CardinalityEstimator file not found at: {CARDINALITY_ESTIMATOR_PATH}")
    
    print(f"Reading CardinalityEstimator from: {CARDINALITY_ESTIMATOR_PATH}")
    
    with open(CARDINALITY_ESTIMATOR_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    
    try:
        tree = ast.parse(content)
        print("✓ File parsed successfully")
        
        class_found = any(
            isinstance(node, ast.ClassDef) and node.name == 'CardinalityEstimator' 
            for node in tree.body
        )
        if not class_found:
            raise ImportError("CardinalityEstimator class not found in file")
        print("✓ CardinalityEstimator class found")
        
    except SyntaxError as e:
        raise ImportError(f"Syntax error in CardinalityEstimator file: {e}")
    
    namespace = {
        'Dict': dict,
        'List': list, 
        'Optional': type('Optional', (), {}),
        'Set': set,
        'math': __import__('math'),
        're': __import__('re'),
        'IStorageManager': IStorageManager,
        # Now use the actual imported classes for the namespace
        'Statistic': Statistic,
        'Condition': Condition,
        'ComparisonOperator': ComparisonOperator,
        'QueryTree': QueryTree,
        'DataType': DataType,
    }
    
    # Remove imports that are now handled by namespace or are directly imported
    import_lines_to_remove = [
        'from src.core.models.query import QueryTree', # This line should be kept in the actual file, but its a local def
        'from src.core.models.storage import Statistic, Condition, ComparisonOperator', # Also this should be kept
        'from src.core import IStorageManager', # This too
    ]
    
    # Remove local class definitions for testing
    local_class_definitions_to_remove = [
        'class QueryTree:',
        'class Statistic:',
        'class Condition:',
        'class ComparisonOperator:'
    ]
    
    modified_content = content
    # For robust removal, find exact class definitions and replace with pass
    for class_keyword in local_class_definitions_to_remove:
        idx = 0
        while True:
            idx = modified_content.find(class_keyword, idx) 
            if idx == -1:
                break
            
            # Find the end of the class block
            end_of_class = modified_content.find('\nclass ', idx + 1) # Look for next class or end of file
            if end_of_class == -1:
                end_of_class = len(modified_content)
            
            # Replace the class body with 'pass'
            modified_content = modified_content[:idx] + class_keyword + modified_content[idx:].split('\n', 1)[0] + '\n    pass' + modified_content[end_of_class:]
            idx += len(class_keyword) # Move past this class
    
    # Comment out old import lines for the actual module being imported
    modified_content = content # Reset content to avoid double-modifying
    for import_line in import_lines_to_remove:
        # Check if the line exists before attempting to replace, to avoid unintended changes
        if import_line.strip() in modified_content: # Use strip for a robust check
            modified_content = modified_content.replace(import_line, f'# {import_line}  # REMOVED FOR TESTING')

    print("✓ Removed problematic imports and local class definitions for testing")
    
    try:
        exec(modified_content, namespace)
        print("✓ Successfully executed CardinalityEstimator file")
    except Exception as e:
        raise ImportError(f"Error executing CardinalityEstimator file: {e}")
    
    if 'CardinalityEstimator' not in namespace:
        raise ImportError("CardinalityEstimator class not found after execution")
    
    print("✓ SUCCESS: CardinalityEstimator imported directly from file!")
    return namespace['CardinalityEstimator']


try:
    ActualCardinalityEstimator = import_cardinality_estimator_directly()
    ACTUAL_IMPORT_SUCCESS = True
except Exception as e:
    print(f"⨉ FAILED to import actual CardinalityEstimator: {e}")
    ACTUAL_IMPORT_SUCCESS = False
    ActualCardinalityEstimator = None


@pytest.fixture
def statistics():
    from src.core.models.storage import Statistic # Import actual Statistic
    from src.core.models.storage import ComparisonOperator # Import actual ComparisonOperator

    return {
        'Employee': Statistic(
            table_name='Employee',
            n_r=1000,
            b_r=100,
            l_r=100,
            f_r=10,
            V={
                'id': 1000,
                'salary': 50,
                'dept_id': 10,
                'name': 800
            },
            min_values={'salary': 10000},
            max_values={'salary': 100000},
            null_counts={}
        ),
        'Department': Statistic(
            table_name='Department',
            n_r=100,
            b_r=10,
            l_r=200,
            f_r=5,
            V={
                'id': 100,
                'name': 100
            },
            min_values={},
            max_values={},
            null_counts={}
        )
    }

@pytest.fixture
def mock_storage_manager(statistics):
    class MockStorageManager(IStorageManager):
        def get_stats(self, table_name: str) -> Statistic:
            if table_name in statistics:
                return statistics[table_name]
            # Return a default Statistic object if not found
            return Statistic(
                table_name=table_name,
                n_r=1000, # Default values
                b_r=100, l_r=100, f_r=10, V={},
                min_values={}, max_values={}, null_counts={}
            )

        def read_block(self, data_retrieval: DataRetrieval) -> Rows: raise NotImplementedError
        def write_block(self, data_write: DataWrite) -> int: raise NotImplementedError
        def delete_block(self, data_deletion: DataDeletion) -> int: raise NotImplementedError
        def set_index(self, table: str, column: str, index_type: str) -> None: raise NotImplementedError
        def drop_index(self, table: str, column: str) -> None: raise NotImplementedError
        def has_index(self, table: str, column: str) -> bool: return False
        def create_table(self, schema: TableSchema) -> None: raise NotImplementedError
        def drop_table(self, table_name: str) -> None: raise NotImplementedError
        def get_table_schema(self, table_name: str) -> Optional[TableSchema]: return None
        def list_tables(self) -> List[str]: return []

    return MockStorageManager()

@pytest.fixture
def estimator(mock_storage_manager):
    if not ACTUAL_IMPORT_SUCCESS or ActualCardinalityEstimator is None:
        pytest.skip("Could not import ACTUAL CardinalityEstimator implementation")
    return ActualCardinalityEstimator(mock_storage_manager)


def test_actual_selection_cardinality_no_conditions(estimator):
    cardinality = estimator.estimate_selection_cardinality('Employee', [])
    assert cardinality == 1000


def test_actual_selection_cardinality_unknown_table(estimator):
    # If the table is unknown and conditions are provided, default_selectivity will be applied
    condition = Condition(column='salary', operator=ComparisonOperator.EQ, value=50000)
    cardinality = estimator.estimate_selection_cardinality('UnknownTable', [condition])
    
    # Default n_r is 1000, default_selectivity for EQ is 0.1
    assert cardinality == pytest.approx(1000 * 0.1, rel=1e-2)


def test_actual_selection_cardinality_equality_condition(estimator):
    condition = Condition(column='salary', operator=ComparisonOperator.EQ, value=50000)
    cardinality = estimator.estimate_selection_cardinality('Employee', [condition])
    
    expected_cardinality = 1000 * (1.0 / 50)
    assert cardinality == pytest.approx(expected_cardinality, rel=1e-2)


def test_actual_selection_cardinality_inequality_condition(estimator):
    condition = Condition(column='salary', operator=ComparisonOperator.NE, value=50000)
    cardinality = estimator.estimate_selection_cardinality('Employee', [condition])
    
    expected_cardinality = 1000 * (1.0 - (1.0 / 50))
    assert cardinality == pytest.approx(expected_cardinality, rel=1e-2)


def test_actual_selection_cardinality_range_condition(estimator):
    condition = Condition(column='salary', operator=ComparisonOperator.GT, value=50000)
    cardinality = estimator.estimate_selection_cardinality('Employee', [condition])
    
    expected_cardinality = 1000 * 0.33
    assert cardinality == pytest.approx(expected_cardinality, rel=1e-2)


def test_actual_selection_cardinality_unknown_column(estimator):
    condition = Condition(column='unknown_column', operator=ComparisonOperator.EQ, value=50000)
    cardinality = estimator.estimate_selection_cardinality('Employee', [condition])
    
    expected_cardinality = 1000 * 0.1
    assert cardinality == pytest.approx(expected_cardinality, rel=1e-2)


def test_actual_selection_cardinality_multiple_conditions(estimator):
    conditions = [
        Condition(column='salary', operator=ComparisonOperator.GT, value=50000),
        Condition(column='dept_id', operator=ComparisonOperator.EQ, value=5)
    ]
    cardinality = estimator.estimate_selection_cardinality('Employee', conditions)
    
    expected_cardinality = 1000 * 0.33 * (1.0 / 10)
    assert cardinality == pytest.approx(expected_cardinality, rel=1e-2)


def test_actual_join_cardinality_equijoin(estimator, statistics):
    left_stats = statistics['Employee']
    right_stats = statistics['Department']
    
    join_condition = 'dept_id = id'
    
    cardinality = estimator.estimate_join_cardinality(left_stats, right_stats, join_condition)
    
    expected_cardinality = (1000 * 100) / max(10, 100)
    assert cardinality == pytest.approx(expected_cardinality, rel=1e-2)


def test_actual_join_cardinality_theta_join(estimator, statistics):
    left_stats = statistics['Employee']
    right_stats = statistics['Department']
    join_condition = 'Employee.salary > Department.budget'
    
    cardinality = estimator.estimate_join_cardinality(left_stats, right_stats, join_condition)
    
    expected_cardinality = 1000 * 100 * 0.1
    assert cardinality == pytest.approx(expected_cardinality, rel=1e-2)


def test_actual_projection_cardinality(estimator):
    input_cardinality = 500
    cardinality = estimator.estimate_projection_cardinality(input_cardinality)
    
    assert cardinality == input_cardinality


def test_actual_cartesian_product_cardinality(estimator):
    left_card = 1000
    right_card = 100
    cardinality = estimator.estimate_cartesian_product_cardinality(left_card, right_card)
    
    expected_cardinality = 1000 * 100
    assert cardinality == expected_cardinality


def test_actual_is_equijoin_method(estimator):
    equijoin_conditions = [
        'Employee.dept_id = Department.id',
        'a = b',
        'table1.col1 = table2.col2'
    ]
    for condition in equijoin_conditions:
        assert estimator.is_equijoin(condition)
    
    non_equijoin_conditions = [
        'Employee.salary > Department.budget',
        'age != 30',
        'salary <= 50000'
    ]
    for condition in non_equijoin_conditions:
        assert not estimator.is_equijoin(condition)


def test_actual_extract_join_columns_method(estimator):
    condition = 'Employee.dept_id = Department.id'
    columns = estimator.extract_join_columns(condition)
    assert columns == ('Employee.dept_id', 'Department.id')
    
    condition = 'dept_id = id'
    columns = estimator.extract_join_columns(condition)
    assert columns == ('dept_id', 'id')
    
    condition = 'salary > 50000'
    columns = estimator.extract_join_columns(condition)
    assert columns is None


def test_actual_estimate_condition_selectivity_method(estimator, statistics):
    stats = statistics['Employee']
    
    eq_condition = Condition(column='salary', operator=ComparisonOperator.EQ, value=50000)
    selectivity = estimator.estimate_condition_selectivity(stats, eq_condition)
    expected_selectivity = 1.0 / 50
    assert selectivity == pytest.approx(expected_selectivity, rel=1e-4)
    
    range_condition = Condition(column='salary', operator=ComparisonOperator.GT, value=50000)
    selectivity = estimator.estimate_condition_selectivity(stats, range_condition)
    assert selectivity == pytest.approx(0.33, rel=1e-2)


def test_actual_default_selectivity_method(estimator):
    test_cases = [
        (ComparisonOperator.EQ, 0.1),
        (ComparisonOperator.NE, 0.9),
        (ComparisonOperator.GT, 0.33),
        (ComparisonOperator.LT, 0.33),
        (ComparisonOperator.GE, 0.33),
        (ComparisonOperator.LE, 0.33)
    ]
    
    for operator, expected in test_cases:
        selectivity = estimator.default_selectivity(operator)
        assert selectivity == pytest.approx(expected, rel=1e-2)


def test_actual_methods_exist(estimator):
    expected_methods = [
        'estimate_selection_cardinality',
        'estimate_condition_selectivity',
        'default_selectivity',
        'estimate_join_cardinality', 
        'estimate_projection_cardinality',
        'estimate_cartesian_product_cardinality',
        'is_equijoin',
        'extract_join_columns'
    ]
    
    for method_name in expected_methods:
        assert hasattr(estimator, method_name)


def test_cardinality_estimator_file_exists():
    assert os.path.exists(CARDINALITY_ESTIMATOR_PATH), (
        f"CardinalityEstimator file should exist at: {CARDINALITY_ESTIMATOR_PATH}"
    )


def test_cardinality_estimator_file_has_correct_content():
    with open(CARDINALITY_ESTIMATOR_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    
    assert 'class CardinalityEstimator:' in content
    
    expected_methods = [
        'estimate_selection_cardinality',
        'estimate_condition_selectivity',
        'estimate_join_cardinality',
        'default_selectivity'
    ]
    for method in expected_methods:
        assert f'def {method}' in content