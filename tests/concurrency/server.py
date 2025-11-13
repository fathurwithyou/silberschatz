import threading
from dataclasses import dataclass
import socket
from typing import Optional

@dataclass
class Operation:
    rid: Optional[int] = None
    action: str = ""

@dataclass
class Transaction:
    operations: dict[int, list[Operation]]

def read_transactions(input: str) -> Transaction: 
    transaction_dict: Transaction = Transaction(operations={})
    
    for row in input:
        line = row.strip()
        if line:
            parts = line.split(',')
            
            if len(parts) == 3:
                rid, tid, action = parts
                if int(tid) not in transaction_dict.operations:
                    transaction_dict.operations[int(tid)] = []
                else:
                    transaction_dict.operations[int(tid)].append(Operation(rid=int(rid), action=action))
            else:
                tid, action = parts
                if int(tid) not in transaction_dict.operations:
                    transaction_dict.operations[int(tid)] = []
                else:
                    transaction_dict.operations[int(tid)].append(Operation(action=action))

    return transaction_dict

def serve():
    print("Server is running...")
    host: str = '127.0.0.1'
    port: int = 5000

    server_socket = socket.socket()
    server_socket.bind((host, port))

    print("Server is listening at port", port)

    while True:
        mode: int = int(input("Enter mode (1: Auto Concurrency, 2: Fixed Order): "))

        if mode == 1:
            print("Auto Concurrency Mode Selected")
        elif mode == 2:
            print("Fixed Order Mode Selected")

        server_socket.listen(1)
        print("Waiting for a connection...")
        
        conn, address = server_socket.accept()
        print(f"Connection from: {address}")

        data = conn.recv(4096).decode()
        print(f"Received data: \n{data}")

        transaction = read_transactions(data.splitlines())
        print(f"Parsed Transactions: \n{transaction}")
        

if __name__ == '__main__':
    serve()