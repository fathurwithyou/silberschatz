"""
Script to run SQL seeder file by sending commands to the database server.
Server must be running (uv run src/server.py)

Usage: python scripts/run_seeder.py seeder.sql [--host 127.0.0.1] [--port 12345]
"""

import argparse
import os
import re
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils.network import send_string, recv_string


def parse_sql_file(file_path: str) -> list[str]:
    """
    Parse SQL file and return list of SQL statements.
    Handles comments and multi-line statements.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Remove single-line comments (-- ...)
    content = re.sub(r'--[^\n]*', '', content)

    # Remove multi-line comments (/* ... */)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)

    # Split by semicolon
    statements = content.split(';')

    # Clean up statements
    cleaned_statements = []
    for stmt in statements:
        stmt = stmt.strip()
        if stmt:
            # Remove extra whitespace and newlines, replace with single space
            stmt = re.sub(r'\s+', ' ', stmt)
            if stmt.endswith(';'):
                stmt = stmt[:-1].strip()
            cleaned_statements.append(stmt)

    return cleaned_statements


class SeederClient:
    def __init__(self, host: str = '127.0.0.1', port: int = 12345):
        self.host = host
        self.port = port
        self.socket = None

    def connect(self) -> bool:
        """Connect to the database server."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False

    def disconnect(self):
        """Disconnect from the server."""
        if self.socket:
            try:
                send_string(self.socket, "ABORT")
                self.socket.close()
            except:
                pass

    def send_query(self, query: str) -> str:
        """Send a single query and return the response."""
        if not self.socket:
            raise ConnectionError("Not connected to server")

        try:
            send_string(self.socket, query.strip())
            response = recv_string(self.socket)
            return response
        except Exception as e:
            raise ConnectionError(f"Communication error: {e}")

    def run_sql_file(self, sql_file_path: str, verbose: bool = True):
        """
        Run SQL file by sending each statement to the server.
        """
        print("SQL SEEDER")
        print(f"File: {sql_file_path}")
        print(f"Server: {self.host}:{self.port}")
        print("-" * 60)

        try:
            statements = parse_sql_file(sql_file_path)
            print(f"Found {len(statements)} SQL statements")
        except Exception as e:
            print(f"Error reading SQL file: {e}")
            return False

        if not self.connect():
            print("Could not connect to server")
            print("Make sure the server is running: uv run src/server.py")
            return False

        print("Connected to server")
        print("-" * 60)

        success_count = 0
        error_count = 0

        for idx, statement in enumerate(statements, 1):
            stmt_type = statement.split()[0].upper() if statement else "UNKNOWN"
            table_name = ""

            if stmt_type in ["CREATE", "INSERT"]:
                match = re.search(r'(?:CREATE TABLE|INSERT INTO)\s+(\w+)', statement, re.IGNORECASE)
                if match:
                    table_name = match.group(1)

            display_info = f"[{idx}/{len(statements)}] {stmt_type}"
            if table_name:
                display_info += f" {table_name}"

            try:
                response = self.send_query(statement)

                # Check if response contains error
                if "ERROR" in response or "error" in response.lower():
                    error_count += 1
                    if verbose:
                        print(f"{display_info}: {response}")
                    else:
                        print(f"{display_info}: ERROR")
                else:
                    success_count += 1
                    if verbose:
                        print(f"{display_info}: {response}")
                    else:
                        print(f"{display_info}: OK")

            except Exception as e:
                error_count += 1
                print(f"{display_info}: ERROR - {str(e)}")

        print("-" * 60)
        print(f"Total: {len(statements)} | Success: {success_count} | Failed: {error_count}")

        if error_count == 0:
            print("All statements executed successfully")
        else:
            print(f"{error_count} statement(s) failed")

        return error_count == 0


def main():
    parser = argparse.ArgumentParser(
        description="Run SQL seeder file by sending commands to the database server"
    )

    parser.add_argument("sql_file", help="Path to SQL file to execute")
    parser.add_argument("--host", default="127.0.0.1", help="Server hostname (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=12345, help="Server port (default: 12345)")
    parser.add_argument("--quiet", action="store_true", help="Reduce output verbosity")

    args = parser.parse_args()

    client = SeederClient(host=args.host, port=args.port)

    try:
        success = client.run_sql_file(args.sql_file, verbose=not args.quiet)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
