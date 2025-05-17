# anomaly/consumer.py
import threading
import time
import json
from collections import defaultdict, deque
from datetime import datetime
from comm.central_client import send_to_central
from comm.battery_manager import (
    update_time_drain,
    drain_on_read,
    drain_on_send,
    get_level,
    check_return_to_base,
    should_enqueue
)

# Buffer window size for discrepancy checks (seconds)
WINDOW = 2.0
# How often to send summaries to central server (seconds)
BATCH_INTERVAL = 2.0

# Per-drone sliding window of recent readings for discrepancy
buffers = defaultdict(lambda: deque())
# Per-drone list of readings to summarize
summary_buffers = defaultdict(list)


def parse_timestamp(ts_str):
    """
    Parse an ISO 8601 timestamp string to a UNIX timestamp.
    """
    try:
        return datetime.fromisoformat(ts_str.replace('Z', '+00:00')).timestamp()
    except Exception:
        return time.time()


def detect_threshold_anomalies(r):
    """
    Check for sensor values outside their defined safe ranges.
    """
    anomalies = []
    # Temperature threshold
    t = r.get('temperature')
    if t is not None and (t < -10 or t > 60):
        anomalies.append({'type': 'temperature', 'value': t})

    # Pressure threshold
    p = r.get('pressure')
    if p is not None and (p < 300 or p > 1100):
        anomalies.append({'type': 'pressure', 'value': p})

    # Altitude threshold
    alt = r.get('altitude')
    if alt is not None and (alt < 0 or alt > 500):
        anomalies.append({'type': 'altitude', 'value': alt})

    # Motor energy thresholds
    motors = r.get('motor_energies')
    if motors:
        for idx, m in enumerate(motors):
            if m < 0 or m > 100:
                anomalies.append({'type': f'motor_{idx}', 'value': m})

    return anomalies


def detect_discrepancy_anomalies(drone_id, ts):
    """
    Check across a sliding window of readings for discrepancies between sensors.
    """
    buf = buffers[drone_id]
    # Purge old readings
    while buf and buf[0][0] < ts - WINDOW:
        buf.popleft()

    anomalies = []
    if len(buf) >= 4:
        temps = [r['temperature'] for _, r in buf if 'temperature' in r]
        alts  = [r['altitude']    for _, r in buf if 'altitude'    in r]
        if temps and (max(temps) - min(temps) > 5):
            anomalies.append({'type': 'temperature_discrepancy', 'range': max(temps) - min(temps)})
        if alts and (max(alts) - min(alts) > 1):
            anomalies.append({'type': 'altitude_discrepancy', 'range': max(alts) - min(alts)})
    return anomalies


def handle_reading(r: dict):
    """
    Process and log anomalies for a single sensor reading,
    then append the reading for later summary, with battery management.
    """
    ts = parse_timestamp(r.get('timestamp', ''))
    drone_id = r.get('drone_id') or r.get('sensor_id', '').split('_')[0]

    # Drain based on elapsed time
    update_time_drain(drone_id, ts)

    # Optionally drop readings if battery critical (<10%)
    if not should_enqueue(drone_id):
        print(f"Battery critical ({get_level(drone_id):.1f}%), dropping reading")
        return

    # Drain per read and shutdown motors if too low
    level_after_read = drain_on_read(drone_id)
    if level_after_read < 10:
        r['motor_energies'] = [0] * len(r.get('motor_energies', []))

    # Buffer for anomaly processing and summary
    buffers[drone_id].append((ts, r))
    summary_buffers[drone_id].append(r)

    # Detect anomalies
    threshold_anoms   = detect_threshold_anomalies(r)
    discrepancy_anoms = detect_discrepancy_anomalies(drone_id, ts)
    all_anoms = threshold_anoms + discrepancy_anoms

    if all_anoms:
        print(f"Anomalies detected for {drone_id}: {json.dumps(all_anoms)}")
    else:
        print(f"No anomalies for {drone_id} at {r.get('timestamp')}")


def start_aggregator():
    """
    Periodically summarize readings and send to the central server,
    with battery-based gating and drain.
    """
    def agg_loop():
        while True:
            time.sleep(BATCH_INTERVAL)
            now = time.time()
            for drone_id, readings in list(summary_buffers.items()):
                if not readings:
                    continue

                # Compute averages
                avg_temperature = sum(r['temperature'] for r in readings) / len(readings)
                avg_pressure    = sum(r['pressure']    for r in readings) / len(readings)
                avg_altitude    = sum(r['altitude']    for r in readings) / len(readings)
                avg_motors      = [
                    sum(r['motor_energies'][i] for r in readings) / len(readings)
                    for i in range(len(readings[0]['motor_energies']))
                ]

                # Battery check for return-to-base
                return_evt, lvl = check_return_to_base(drone_id)
                if return_evt:
                    print(f"*** Return-to-base triggered for {drone_id} at {lvl:.1f}% ***")

                # Gate sending based on battery level (<20%)
                if lvl < 20:
                    print(f"Battery low ({lvl:.1f}%), skipping summary")
                else:
                    # Drain for send + motor usage
                    new_lvl = drain_on_send(drone_id, sum(avg_motors) / len(avg_motors))
                    payload = {
                        "drone_id": drone_id,
                        "avg_temperature": avg_temperature,
                        "avg_pressure": avg_pressure,
                        "avg_altitude": avg_altitude,
                        "avg_motor_energies": avg_motors,
                        "timestamp": datetime.utcfromtimestamp(now).strftime('%Y-%m-%dT%H:%M:%SZ')
                    }
                    try:
                        send_to_central(payload)
                        print(f"Sent summary to central for {drone_id}: {payload}; battery: {new_lvl:.1f}%")
                    except Exception as e:
                        print(f"Error sending to central: {e}")

                # Clear buffer for next batch
                summary_buffers[drone_id].clear()

    t = threading.Thread(target=agg_loop, daemon=True)
    t.start()


def start_consumer(queue):
    """
    Start both the consumer and aggregator threads.
    """
    # Start aggregator loop
    start_aggregator()
    print("Aggregator thread started…")

    # Start consumer loop
    def worker():
        while True:
            reading = queue.get()
            handle_reading(reading)
            queue.task_done()

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    print("Consumer thread started…")
