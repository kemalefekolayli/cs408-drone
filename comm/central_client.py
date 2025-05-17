# comm/central_client.py
import socket
import json

# For local testing:
HOST, PORT = 'localhost', 9100

def send_to_central(payload: dict):
    data = json.dumps(payload) + '\n'
    with socket.create_connection((HOST, PORT)) as sock:
        sock.sendall(data.encode('utf-8'))
