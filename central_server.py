# central_server.py
import socket

HOST, PORT = '0.0.0.0', 9100

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen()
    print(f"📡 Central server listening on {HOST}:{PORT}")

    while True:
        conn, addr = srv.accept()
        print("📥 Connection from", addr)
        with conn:
            buffer = ""
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                buffer += data.decode('utf-8', errors='replace')
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if line.strip():
                        print("✅ Received summary:", line)
        print("🔒 Connection closed, waiting for next…")
