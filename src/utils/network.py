import socket
import struct

def send_string(sock: socket.socket, text: str):
    data = text.encode('utf-8')
    header = struct.pack('!I', len(data))
    sock.sendall(header + data)

def recv_string(sock: socket.socket) -> str:
    header_data = _recvall(sock, 4)
    if not header_data:
        return ""
        
    data_len = struct.unpack('!I', header_data)[0]
    
    data_bytes = _recvall(sock, data_len)
    if not data_bytes:
        return ""
        
    return data_bytes.decode('utf-8')

def _recvall(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return bytes(data)