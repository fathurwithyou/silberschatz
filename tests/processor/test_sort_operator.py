import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from src.processor.operators.sort_operator import SortOperator
from src.core.models import Rows, TableSchema, ColumnDefinition, DataType


def create_test_schema():
    """Create a test table schema."""
    columns = [
        ColumnDefinition(name="id", data_type=DataType.INTEGER, primary_key=True),
        ColumnDefinition(name="name", data_type=DataType.VARCHAR, max_length=50),
        ColumnDefinition(name="age", data_type=DataType.INTEGER),
        ColumnDefinition(name="score", data_type=DataType.FLOAT),
        ColumnDefinition(name="joined", data_type=DataType.VARCHAR, max_length=20)
    ]
    return [TableSchema(table_name="test", columns=columns)]


def create_test_data():
    """Create test data for sorting."""
    return [
        {"id": 1, "name": "Zara", "age": 25, "score": 80.5, "joined": "2023-01-01"},
        {"id": 2, "name": "Ali",  "age": 30, "score": 90.0, "joined": "2023-02-01"},
        {"id": 3, "name": "Ali",  "age": 20, "score": 85.0, "joined": "2023-03-01"},  # Ali muda
        {"id": 4, "name": "Budi", "age": None, "score": 88.0, "joined": None},
        {"id": 5, "name": "caca", "age": 28, "score": 70.0, "joined": "2023-01-15"}  # Lowercase name
    ]


def test_sort_no_order_by():
    """Test jika order_by kosong, urutan tidak berubah"""
    operator = SortOperator()
    raw_data = create_test_data()
    schema = create_test_schema()
    input_rows = Rows(data=raw_data, rows_count=len(raw_data), schema=schema)
    
    result = operator.execute(input_rows, "")
    assert result.data == raw_data
    
    result_none = operator.execute(input_rows, "")
    assert result_none.data == raw_data


def test_sort_asc_integer():
    """Test sorting ASC pada kolom integer"""
    operator = SortOperator()
    raw_data = create_test_data()
    schema = create_test_schema()
    input_rows = Rows(data=raw_data, rows_count=len(raw_data), schema=schema)
    
    result = operator.execute(input_rows, "id ASC")
    ids = [row['id'] for row in result.data]
    assert ids == [1, 2, 3, 4, 5]


def test_sort_desc_integer():
    """Test sorting DESC pada kolom integer"""
    operator = SortOperator()
    raw_data = create_test_data()
    schema = create_test_schema()
    input_rows = Rows(data=raw_data, rows_count=len(raw_data), schema=schema)
    
    result = operator.execute(input_rows, "id DESC")
    ids = [row['id'] for row in result.data]
    assert ids == [5, 4, 3, 2, 1]


def test_sort_string_case_insensitive():
    """Test sorting string (memastikan 'Ali' < 'Budi' < 'caca')"""
    operator = SortOperator()
    raw_data = create_test_data()
    schema = create_test_schema()
    input_rows = Rows(data=raw_data, rows_count=len(raw_data), schema=schema)
    
    # 'caca' (lowercase) harusnya setelah 'Budi' jika case-insensitive
    # Karena 'c' > 'B' secara case-insensitive ('b' < 'c')
    result = operator.execute(input_rows, "name ASC")
    names = [row['name'] for row in result.data]
    
    # Ekspektasi: Ali, Ali, Budi, caca, Zara
    expected = ["Ali", "Ali", "Budi", "caca", "Zara"]
    assert names == expected


def test_sort_desc_string():
    """Test sorting DESC pada string (menguji logika pembalik bit char)"""
    operator = SortOperator()
    raw_data = create_test_data()
    schema = create_test_schema()
    input_rows = Rows(data=raw_data, rows_count=len(raw_data), schema=schema)
    
    result = operator.execute(input_rows, "name DESC")
    names = [row['name'] for row in result.data]
    
    # Ekspektasi: Zara, caca, Budi, Ali, Ali
    expected = ["Zara", "caca", "Budi", "Ali", "Ali"]
    assert names == expected


def test_sort_multi_column():
    """Test sorting multi kolom: Nama ASC, Umur ASC"""
    operator = SortOperator()
    raw_data = create_test_data()
    schema = create_test_schema()
    input_rows = Rows(data=raw_data, rows_count=len(raw_data), schema=schema)
    
    # Kasus: Ali (30) vs Ali (20). Ali(20) harus duluan.
    result = operator.execute(input_rows, "name ASC, age ASC")
    
    # Ambil tuple (name, age) untuk verifikasi
    pairs = [(row['name'], row['age']) for row in result.data]
    
    # Cek spesifik urutan Ali
    ali_subset = [p for p in pairs if p[0] == "Ali"]
    assert ali_subset == [("Ali", 20), ("Ali", 30)]


def test_sort_multi_column_mixed_direction():
    """Test sorting multi kolom: Nama ASC, Umur DESC"""
    operator = SortOperator()
    raw_data = create_test_data()
    schema = create_test_schema()
    input_rows = Rows(data=raw_data, rows_count=len(raw_data), schema=schema)
    
    # Kasus: Ali (30) vs Ali (20). Ali(30) harus duluan karena Age DESC.
    result = operator.execute(input_rows, "name ASC, age DESC")
    
    pairs = [(row['name'], row['age']) for row in result.data]
    
    ali_subset = [p for p in pairs if p[0] == "Ali"]
    assert ali_subset == [("Ali", 30), ("Ali", 20)]


def test_sort_null_handling():
    """Test handling NULL values (Harus selalu di awal/terkecil)"""
    operator = SortOperator()
    raw_data = create_test_data()
    schema = create_test_schema()
    input_rows = Rows(data=raw_data, rows_count=len(raw_data), schema=schema)
    
    # Ada row dengan age=None (ID 4)
    result = operator.execute(input_rows, "age ASC")
    
    first_row = result.data[0]
    assert first_row['age'] is None
    assert first_row['id'] == 4


def test_parse_order_by_method():
    """Test method private _parse_order_by"""
    operator = SortOperator()
    query = "name, age DESC,  score   ASC"
    parsed = operator._parse_order_by(query)
    
    expected = [
        ("name", "ASC"),   # Default ASC
        ("age", "DESC"),
        ("score", "ASC")
    ]
    assert parsed == expected


def test_normalize_value_method():
    """Test method private _normalize_value untuk type safety"""
    operator = SortOperator()
    
    # Test None
    val_none = operator._normalize_value(None)
    assert val_none == (0, None)
    
    # Test Int
    val_int = operator._normalize_value(100)
    assert val_int == (1, 100)
    
    # Test String
    val_str = operator._normalize_value("Test")
    assert val_str == (4, "test")  # Expect lowercase


def test_apply_direction_method():
    """Test method private _apply_direction untuk logika DESC"""
    operator = SortOperator()
    
    # Test Int DESC
    norm_int = (1, 50)
    res_int = operator._apply_direction(norm_int, "DESC")
    assert res_int == (1, -50)
    
    # Test String DESC
    norm_str = (4, "abc")
    res_str = operator._apply_direction(norm_str, "DESC")
    inverted = "".join(chr(255 - ord(c)) for c in "abc")
    assert res_str == (4, inverted)