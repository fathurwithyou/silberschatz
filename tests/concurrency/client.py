import socket
import json

def client():
    # Server IP and port
    host = '127.0.0.1'
    port = 5000
    
    client_socket = socket.socket() 
    client_socket.connect((host, port))

    while(True):
        input_file = input("Enter the transaction file name (e.g., '1.txt'): ")
        with open(input_file, 'r') as file:
            data = file.read()
        client_socket.send(data.encode())
        print(f"Sent data from {input_file} to server.")

    client_socket.close()

if __name__ == '__main__':
    client()
