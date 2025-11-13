import threading
from dataclasses import dataclass
import socket
from typing import Optional

@dataclass
class Operation:
    data: Optional[int] = None
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
                action, tid, data = parts
                if int(tid) not in transaction_dict.operations:
                    transaction_dict.operations[int(tid)] = []

                transaction_dict.operations[int(tid)].append(Operation(data=int(data), action=action))
            else:
                action, tid = parts
                if int(tid) not in transaction_dict.operations:
                    transaction_dict.operations[int(tid)] = []

                transaction_dict.operations[int(tid)].append(Operation(action=action))

    return transaction_dict

def print_transactions(transaction: Transaction):
    for tid, ops in transaction.operations.items():
        print(f"Transaction ID: {tid}")
        for op in ops:
            if op.data is not None:
                print(f"  Operation: {op.action} on data {op.data}")
            else:
                print(f"  Operation: {op.action}")

def serve():
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
        print("Transactions received:")
        print_transactions(transaction)
        

if __name__ == '__main__':
    serve()