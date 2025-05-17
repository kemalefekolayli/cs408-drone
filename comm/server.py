# comm/server.py
import socket
import threading
import json
from queue import Queue
from anomaly.consumer import start_consumer

# Thread-safe queue for incoming sensor readings
sensor_queue = Queue()

HOST = '0.0.0.0'  # listen on all interfaces
PORT = 9000       # sensor feed port


def handle_client(conn, addr):
    """
    Handle an individual sensor connection:
    - Read newline-delimited JSON frames
    - Parse and enqueue each reading
    """
    print(f"Connection established from {addr}")
    buffer = ''
    with conn:
        while True:
            try:
                data = conn.recv(1024)
                if not data:
                    break
                buffer += data.decode('utf-8', errors='replace')
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if not line.strip():
                        continue
                    try:
                        reading = json.loads(line)
                        sensor_queue.put(reading)
                        print('Enqueued reading:', reading)
                    except json.JSONDecodeError as e:
                        print('JSON decode error:', e, 'line:', line)
            except (ConnectionResetError, OSError):
                break
    print(f"Connection closed from {addr}")


def serve():
    """
    Start the sensor TCP server and the anomaly consumer.
    Each sensor connection is handled in its own daemon thread.
    """
    # Start anomaly detection and aggregation threads
    start_consumer(sensor_queue)
    print("Anomaly and aggregator threads started…")

    # Create, bind, and listen on the server socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((HOST, PORT))
        sock.listen()
        print(f"Listening on {HOST}:{PORT}…")

        # Accept and dispatch connections indefinitely
        while True:
            conn, addr = sock.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()


if __name__ == '__main__':
    serve()
