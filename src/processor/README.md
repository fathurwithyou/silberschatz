# Query Processor

## 📁 Struktur Folder

```
processor/
├── __init__.py
├── processor.py           # Kelas utama QueryProcessor
├── handlers/              # Handler untuk berbagai jenis query
│   ├── __init__.py
│   ├── dml_handler.py     # Data Manipulation Language (SELECT, UPDATE)
│   └── tcl_handler.py     # Transaction Control Language (BEGIN, COMMIT, ABORT)
└── operators/             # Operator fisik untuk eksekusi query
    ├── __init__.py
    ├── join_operator.py       # Operasi JOIN
    ├── projection_operator.py # Operasi PROJECT (SELECT kolom)
    ├── scan_operator.py       # Operasi SCAN tabel
    ├── selection_operator.py  # Operasi SELECT (WHERE clause)
    ├── sort_operator.py       # Operasi ORDER BY
    └── update_operator.py     # Operasi UPDATE/INSERT/DELETE
```

## 🏗️ Arsitektur

### 1. QueryProcessor (`processor.py`)
Kelas utama yang mengkoordinasikan seluruh proses eksekusi query:

- **Input**: Query string dari user
- **Proses**: 
  1. Routing query ke handler yang sesuai
  2. Eksekusi physical operators
- **Output**: ExecutionResult

```python
# Contoh penggunaan
processor = QueryProcessor(optimizer)
result = processor.execute_query("SELECT * FROM mahasiswa WHERE npm = '123'")
```

### 2. Handlers (`handlers/`)
Handler menangani logika bisnis untuk berbagai jenis query:

#### DMLHandler (`dml_handler.py`)
- Menangani query manipulasi data: `SELECT`, `UPDATE`
- Mengintegrasikan dengan Query Optimizer untuk mendapatkan execution plan
- Mengelola transaksi implicit (auto-commit)

#### TCLHandler (`tcl_handler.py`)
- Menangani kontrol transaksi: `BEGIN TRANSACTION`, `COMMIT`, `ABORT`
- Validasi sintaks perintah TCL
- Koordinasi dengan Concurrency Control Manager

### 3. Operators (`operators/`)
Physical operators yang mengeksekusi operasi database secara konkret:

#### ScanOperator (`scan_operator.py`)
- Membaca data dari tabel (table scan)
- Berinteraksi dengan Storage Manager
- Mengelola lock melalui Concurrency Control Manager

#### SelectionOperator (`selection_operator.py`) 
- Implementasi operasi WHERE clause
- Filtering rows berdasarkan kondisi

#### ProjectionOperator (`projection_operator.py`)
- Implementasi operasi SELECT kolom tertentu
- Membatasi kolom yang dikembalikan

#### JoinOperator (`join_operator.py`)
- Implementasi algoritma JOIN

#### SortOperator (`sort_operator.py`)
- Implementasi operasi ORDER BY

#### UpdateOperator (`update_operator.py`)
- Implementasi operasi UPDATE
- Koordinasi dengan Storage Manager

## 🔄 Alur Eksekusi Query

### 1. DML Query (contoh: SELECT)
```
User Input: "SELECT name FROM students WHERE age > 20"
    ↓
QueryProcessor._route_query() → identifikasi sebagai DML
    ↓
DMLHandler.handle()
    ↓
Optimizer.parse_query() → ParsedQuery
    ↓
Optimizer.optimize_query() → Execution Plan
    ↓
QueryProcessor.execute() → eksekusi recursive
    ↓
Physical Operators (Scan → Selection → Projection)
    ↓
ExecutionResult
```

### 2. TCL Query (contoh: BEGIN)
```
User Input: "BEGIN TRANSACTION"
    ↓
QueryProcessor._route_query() → identifikasi sebagai TCL
    ↓
TCLHandler.handle_begin()
    ↓
Validasi sintaks
    ↓
Concurrency Control Manager (mulai transaksi)
    ↓
ExecutionResult
```

### 3. Pengerjaan
harusnya nanti kerjaannya sisa implemen operator-operator yang ada di folder `operators/`