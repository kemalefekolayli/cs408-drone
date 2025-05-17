# comm/test_client.py
import socket
import time
import json
from datetime import datetime

HOST, PORT = 'localhost', 9000
MAX_BACKOFF = 16  # seconds


def send_reading(payload):
    """
    Attempt to send a JSON payload to the server with retry on failure.
    Uses exponential backoff up to MAX_BACKOFF.
    """
    backoff = 1
    while True:
        try:
            with socket.create_connection((HOST, PORT), timeout=5) as sock:
                sock.sendall((json.dumps(payload) + '\n').encode('utf-8'))
                print(f"Sent payload: {payload}")
                return
        except ConnectionRefusedError:
            print(f"Connection refused, retrying in {backoff} seconds...")
        except socket.timeout:
            print(f"Connection timed out, retrying in {backoff} seconds...")
        except Exception as e:
            print(f"Error: {e}, retrying in {backoff} seconds...")
        time.sleep(backoff)
        backoff = min(MAX_BACKOFF, backoff * 2)


if __name__ == '__main__':
    # Construct a test payload
    payload = {
        "sensor_id": "drone1_s1",
        "temperature": 20.0,
        "pressure": 1013.0,
        "altitude": 100.0,
        "motor_energies": [10, 20, 30, 40],
        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    send_reading(payload)
