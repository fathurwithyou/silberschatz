import struct
from typing import Dict, Any, List
from src.core.models import DataType, ColumnDefinition, TableSchema, Rows

class Serializer:
    def __init__(self):
        pass
    
    def serialize_row(self, data: Dict[str, Any], schema: TableSchema) -> bytes:
        parts = []
        
        # 1. Null bitmap
        parts.append(self._create_null_bitmap(data, schema))
        
        # 2. Serialize tiap kolom sesuai urutan schema
        for column in schema.columns:
            value = data.get(column.name)
            
            # Handle NULL
            if value is None:
                if column.data_type == DataType.VARCHAR:
                    parts.append(struct.pack('H', 0))  # Length = 0
                elif column.data_type == DataType.CHAR:
                    parts.append(b'\x00' * (column.max_length or 1))
                continue
            
            # Serialize berdasarkan tipe
            if column.data_type == DataType.INTEGER:
                parts.append(struct.pack('i', int(value)))
            
            elif column.data_type == DataType.FLOAT:
                parts.append(struct.pack('d', float(value)))
            
            elif column.data_type == DataType.CHAR:
                # Fixed-length, padded dengan \x00
                max_len = column.max_length or 255
                value_bytes = str(value).encode('utf-8')[:max_len]
                parts.append(value_bytes.ljust(max_len, b'\x00'))
            
            elif column.data_type == DataType.VARCHAR:
                # Variable-length dengan 2B prefix
                max_len = column.max_length or 65535
                value_bytes = str(value).encode('utf-8')[:max_len]
                parts.append(struct.pack('H', len(value_bytes)))
                parts.append(value_bytes)
        
        return b''.join(parts)
    
    def deserialize_row(self, data: bytes, schema: TableSchema) -> Dict[str, Any]:
        result = {}
        offset = 0
        
        # 1. Read null bitmap
        num_null_bytes = (len(schema.columns) + 7) // 8
        null_bitmap = data[offset:offset + num_null_bytes]
        offset += num_null_bytes
        
        # 2. Deserialize tiap kolom
        for i, column in enumerate(schema.columns):
            is_null = self._is_null(null_bitmap, i)
            
            if column.data_type == DataType.INTEGER:
                if is_null:
                    result[column.name] = None
                else:
                    result[column.name] = struct.unpack('i', data[offset:offset + 4])[0]
                    offset += 4
            
            elif column.data_type == DataType.FLOAT:
                if is_null:
                    result[column.name] = None
                else:
                    result[column.name] = struct.unpack('d', data[offset:offset + 8])[0]
                    offset += 8
            
            elif column.data_type == DataType.CHAR:
                max_len = column.max_length or 255
                value_bytes = data[offset:offset + max_len]
                offset += max_len
                result[column.name] = None if is_null else value_bytes.rstrip(b'\x00').decode('utf-8')
            
            elif column.data_type == DataType.VARCHAR:
                str_len = struct.unpack('H', data[offset:offset + 2])[0]
                offset += 2
                if is_null or str_len == 0:
                    result[column.name] = None
                else:
                    result[column.name] = data[offset:offset + str_len].decode('utf-8')
                    offset += str_len
        
        return result
    
    def serialize_rows(self, rows: Rows[Dict[str, Any]], schema: TableSchema) -> bytes:
        parts = [struct.pack('I', rows.rows_count)]
        
        for row_data in rows.data:
            serialized_row = self.serialize_row(row_data, schema)
            parts.append(struct.pack('I', len(serialized_row)))
            parts.append(serialized_row)
        
        return b''.join(parts)
    
    def deserialize_rows(self, data: bytes, schema: TableSchema) -> Rows[Dict[str, Any]]:
        if len(data) < 4:
            return Rows(data=[], rows_count=0)
        
        rows_list = []
        offset = 0
        
        # Read count
        row_count = struct.unpack('I', data[offset:offset + 4])[0]
        offset += 4
        
        # Read tiap row
        for _ in range(row_count):
            if offset + 4 > len(data):
                break
            
            row_len = struct.unpack('I', data[offset:offset + 4])[0]
            offset += 4
            
            if offset + row_len > len(data):
                break
            
            row = self.deserialize_row(data[offset:offset + row_len], schema)
            rows_list.append(row)
            offset += row_len
        
        return Rows(data=rows_list, rows_count=len(rows_list))

    def calculate_row_size(self, schema: TableSchema) -> int:
        size = (len(schema.columns) + 7) // 8  # Null bitmap
        
        for column in schema.columns:
            if column.data_type == DataType.INTEGER:
                size += 4
            elif column.data_type == DataType.FLOAT:
                size += 8
            elif column.data_type == DataType.CHAR:
                size += column.max_length or 255
            elif column.data_type == DataType.VARCHAR:
                size += 2 + (column.max_length or 255)
        
        return size
    
    def _create_null_bitmap(self, data: Dict[str, Any], schema: TableSchema) -> bytes:
        num_bytes = (len(schema.columns) + 7) // 8
        bitmap = bytearray(num_bytes)
        
        for i, column in enumerate(schema.columns):
            if data.get(column.name) is None:
                bitmap[i // 8] |= (1 << (i % 8))
        
        return bytes(bitmap)
    
    def _is_null(self, null_bitmap: bytes, column_index: int) -> bool:
        byte_idx = column_index // 8
        bit_idx = column_index % 8
        return byte_idx < len(null_bitmap) and (null_bitmap[byte_idx] & (1 << bit_idx)) != 0