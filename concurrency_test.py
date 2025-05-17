# concurrency_test.py
import threading
import socket
import json
from datetime import datetime

HOST, PORT = 'localhost', 9000

def send_reading(sensor_id):
    payload = {
        "sensor_id":    sensor_id,
        "temperature":  20.0,
        "pressure":     1013.0,
        "altitude":     100.0,
        "motor_energies":[10,20,30,40],
        "timestamp":    datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    }
    try:
        with socket.create_connection((HOST, PORT)) as sock:
            sock.sendall((json.dumps(payload) + '\n').encode('utf-8'))
    except Exception as e:
        print(f"{sensor_id} failed:", e)

threads = []
# spawn 5 concurrent “sensors”
for i in range(5):
    sid = f"drone1_s{i+1}"
    t = threading.Thread(target=send_reading, args=(sid,), daemon=True)
    threads.append(t)
    t.start()

# wait for all to finish
for t in threads:
    t.join()

print("All clients sent their readings.")
