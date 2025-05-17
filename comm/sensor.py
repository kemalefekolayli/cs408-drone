# comm/sensor.py
import socket
import time
import json
import argparse
import random
from datetime import datetime

# Exponential backoff parameters
MAX_BACKOFF = 16  # seconds
INITIAL_BACKOFF = 1
SEND_INTERVAL = 2  # seconds between sends


def generate_reading(sensor_id: str) -> dict:
    """
    Generate a fake sensor reading with realistic ranges.
    """
    return {
        "sensor_id": sensor_id,
        "temperature": round(random.uniform(-10.0, 60.0), 2),
        "humidity": round(random.uniform(10.0, 90.0), 2),
        "pressure": round(random.uniform(300.0, 1100.0), 2),
        "altitude": round(random.uniform(0.0, 500.0), 2),
        "motor_energies": [random.randint(0, 100) for _ in range(4)],
        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    }


def main():
    parser = argparse.ArgumentParser(description="Fake sensor node sending data to the drone.")
    parser.add_argument('--host', required=True, help='Drone server IP or hostname')
    parser.add_argument('--port', type=int, required=True, help='Drone server port')
    parser.add_argument('--sensor-id', default='sensor1', help='Unique sensor identifier')
    args = parser.parse_args()

    host, port = args.host, args.port
    sensor_id = args.sensor_id

    print(f"Sensor {sensor_id} started. Target = {host}:{port}")

    backoff = INITIAL_BACKOFF
    sock = None

    while True:
        # Ensure connection exists
        if sock is None:
            try:
                sock = socket.create_connection((host, port), timeout=5)
                print(f"Connected to drone at {host}:{port}")
                backoff = INITIAL_BACKOFF
            except (ConnectionRefusedError, socket.timeout):
                print(f"Couldn't connect, retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff = min(MAX_BACKOFF, backoff * 2)
                continue
            except Exception as e:
                print(f"Connection error: {e}, retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff = min(MAX_BACKOFF, backoff * 2)
                continue

        # Generate and send a reading
        reading = generate_reading(sensor_id)
        try:
            sock.sendall((json.dumps(reading) + '\n').encode('utf-8'))
            print(f"Sent data: {reading}")
        except (BrokenPipeError, ConnectionResetError, OSError):
            print("Connection lost, retrying...")
            try:
                sock.close()
            except Exception:
                pass
            sock = None
            continue

        # Wait before next reading
        time.sleep(SEND_INTERVAL)


if __name__ == '__main__':
    main()
