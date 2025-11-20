import sys
import os
import argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.models import ExecutionResult
from src.processor.processor import QueryProcessor
from src.concurrency.concurrency_manager import ConcurrencyControlManager
from src.storage.storage_manager import StorageManager
from src.optimizer.optimizer import QueryOptimizer
from src.failure.failure_recovery_manager import FailureRecoveryManager
from src.utils.network import recv_string, send_string
import socket
import threading
import time


class DatabaseServer:
    def __init__(self, host: str = '127.0.0.1', port: int = 12345):
        self.host = host
        self.port = port
        
    def initialize_components(self, data_dir="data_test"):
        """Initialize all database components."""
        query_optimizer = QueryOptimizer()
        storage_manager = StorageManager(data_dir)
        concurrency_manager = ConcurrencyControlManager()
        failure_recovery_manager = FailureRecoveryManager()
        
        self.query_processor = QueryProcessor(
            query_optimizer,
            concurrency_manager,
            failure_recovery_manager,
            storage_manager
        )
    
    def format_execution_result(self, result: ExecutionResult, execution_time: float) -> str:
        """Format execution result into a table string."""
        output = []
        
        if not result.data:
            return "No Data returned.\n"
        
        rows = result.data.data or []
        headers = self._resolve_ordered_headers(rows, result.data.schema)
        shown_headers = [self._format_header_name(h) for h in headers]
        col_widths = [len(header) for header in shown_headers]
        
        for i, header in enumerate(headers):
            value_width = max(len(str(row.get(header, 'NULL'))) for row in rows) if rows else 0
            col_widths[i] = max(col_widths[i], value_width)
        
        # Format header row
        header_row = "| " + " | ".join(header.ljust(col_widths[i]) for i, header in enumerate(shown_headers)) + " |"
        separator = "+" + "+".join("-" * (col_widths[i] + 2) for i in range(len(headers))) + "+"
        
        # Build the table
        output.append(separator)
        output.append(header_row)
        output.append(separator)
        
        # Format data rows
        for row in rows:
            values = [str(row.get(h, 'NULL')).ljust(col_widths[i]) for i, h in enumerate(headers)]
            data_row = "| " + " | ".join(values) + " |"
            output.append(data_row)
        
        if result.data.rows_count > 0:
            output.append(separator)
        
        output.append(f"\n({result.data.rows_count} rows)")
        output.append(f"Time: {execution_time * 1000:.4f} ms")
            
        return "\n".join(output)

    def _resolve_ordered_headers(self, rows, schemas):
        if rows:
            ordered = []
            for row in rows:
                for key in row.keys():
                    if key not in ordered:
                        ordered.append(key)
            return ordered

        header_keys = []
        for schema in schemas:
            for column in schema.columns:
                header_keys.append(f"{schema.table_name}.{column.name}")
        return header_keys

    def _format_header_name(self, header: str) -> str:
        if "." in header:
            return header.split(".", 1)[1]
        return header

    def handle_client(self, conn, addr):
        """Handle a single client connection in a separate thread."""
        client_name = f"{addr[0]}:{addr[1]}"
        print(f"[{client_name}] Client connected")
        
        try:
            while True:
                query_str = recv_string(conn)
                if not query_str: 
                    print(f"[{client_name}] Client disconnected")
                    break
                
                print(f"[{client_name}] Received query: {query_str[:50]}...")
                
                # Eksekusi
                try:
                    start_time = time.time()
                    result_obj = self.query_processor.execute_query(query_str)
                    end_time = time.time()
                    execution_time = end_time - start_time
                except Exception as e:
                    response_text = f"ERROR: {e}"
                else:
                    response_text = self.format_execution_result(result_obj, execution_time)
                
                # Kirim String
                send_string(conn, response_text)
                print(f"[{client_name}] Response sent")
                
        except Exception as e:
            print(f"[{client_name}] Error: {e}")
        finally:
            conn.close()
            print(f"[{client_name}] Connection closed")
            
    def start(self):
        """Start the server and handle multiple concurrent client connections."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            self.server_socket = server_sock
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            server_sock.bind((self.host, self.port))
            server_sock.listen(5)
            print(f"Server listening on {self.host}:{self.port}")
            print("Waiting for client connections...")
            
            try:
                while True:
                    conn, addr = server_sock.accept()
                    
                    # Create a new thread for each client
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(conn, addr),
                        daemon=True
                    )
                    client_thread.start()
                    
            except KeyboardInterrupt:
                print("\nServer shutting down...")
            except Exception as e:
                print(f"Server error: {e}")
            finally:
                print("Server stopped")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Database Server', add_help=False)
    
    parser.add_argument('-h', '--host', 
                       default='127.0.0.1',
                       help='Server bind address (default: 127.0.0.1)')
    
    parser.add_argument('-p', '--port', 
                       type=int, 
                       default=12345,
                       help='Server port (default: 12345)')
    
    parser.add_argument('-d', '--data-dir',
                       default='data_test',
                       help='Data directory (default: data_test)')
    
    parser.add_argument('-H', '--help', 
                       action='help',
                       help='Show this help message and exit')
    
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    
    server = DatabaseServer(host=args.host, port=args.port)
    server.initialize_components(data_dir=args.data_dir)
    server.start()
