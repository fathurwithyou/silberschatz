import socket
import argparse
from utils.network import send_string, recv_string


class DatabaseClient:
    def __init__(self, host: str = '127.0.0.1', port: int = 12345):
        self.host = host
        self.port = port
        self.socket = None
        self.saved_queries = ""
        
    def connect(self) -> bool:
        """Connect to the database server."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            return True
        except Exception:
            return False
    
    def disconnect(self):
        """Disconnect from the server."""
        if self.socket:
            try:
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
            return response if response else "No response from server."
        except Exception as e:
            raise ConnectionError(f"Communication error: {e}")
    
    def process_input(self, query: str) -> list:
        """Process user input and return list of queries to execute."""
        if ';' not in query:
            self.saved_queries += query + "\n"
            return []
        
        queries = [q.strip() for q in query.split(';') if q.strip()]
        if len(queries) == 0:
            queries = [self.saved_queries]
        else:
            queries[0] = self.saved_queries + queries[0]
        
        self.saved_queries = queries[-1] if not query.endswith(';') and queries else ""
        return queries
    
    def interactive_session(self):
        """Run an interactive client session."""
        if not self.connect():
            print(f"Error: Could not connect to server at {self.host}:{self.port}")
            print("Make sure the server is running.")
            return
            
        try:
            print(f"Connected to server at {self.host}:{self.port}")
            print("Type 'quit' or 'exit' to disconnect")
            print("-" * 50)
            
            while True:
                query = input("SQL> ").strip()
                
                if query.lower() in ['quit', 'exit', '']:
                    print("Disconnecting...")
                    break
                
                queries_to_execute = self.process_input(query)
                
                if not queries_to_execute:
                    continue
                
                stop = False
                for part in queries_to_execute:
                    try:
                        response = self.send_query(part)
                        print(response)
                        print()
                    except ConnectionError as e:
                        print(f"Communication error: {e}")
                        stop = True
                        break
                    
                if stop:
                    break
                    
        except ConnectionRefusedError:
            print(f"Error: Could not connect to server at {self.host}:{self.port}")
            print("Make sure the server is running.")
        except Exception as e:
            print(f"Client error: {e}")
        finally:
            self.disconnect()
            print("Client disconnected.")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Database Client', add_help=False)
    
    parser.add_argument('-h', '--host', 
                       default='127.0.0.1',
                       help='Server hostname (default: 127.0.0.1)')
    
    parser.add_argument('-p', '--port', 
                       type=int, 
                       default=12345,
                       help='Server port (default: 12345)')
    
    parser.add_argument('-H', '--help', 
                       action='help',
                       help='Show this help message and exit')
    
    return parser.parse_args()


def client():
    """Legacy function for backwards compatibility."""
    db_client = DatabaseClient()
    db_client.interactive_session()


if __name__ == '__main__':
    args = parse_arguments()
    
    db_client = DatabaseClient(host=args.host, port=args.port)
    db_client.interactive_session()