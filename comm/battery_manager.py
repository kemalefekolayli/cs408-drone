# comm/battery_manager.py

import threading
import time
from collections import defaultdict

# Battery state per drone: starts at 100%
battery_levels = defaultdict(lambda: 100.0)
returned_to_base = set()

# Drain parameters
DRAIN_PER_SEC   = 0.1    # % per second
DRAIN_PER_READ  = 0.05   # % per sensor frame
DRAIN_PER_SEND  = 0.2    # % per summary sent
DRAIN_MOTOR_FAC = 0.001  # % per 1% avg motor power

# Track last timestamp for time-based drain
last_timestamp = {}

lock = threading.Lock()

def update_time_drain(drone_id, now_ts):
    with lock:
        last = last_timestamp.get(drone_id, now_ts)
        delta = now_ts - last
        battery_levels[drone_id] = max(0.0,
            battery_levels[drone_id] - delta * DRAIN_PER_SEC
        )
        last_timestamp[drone_id] = now_ts

def drain_on_read(drone_id):
    with lock:
        battery_levels[drone_id] = max(0.0,
            battery_levels[drone_id] - DRAIN_PER_READ
        )
        return battery_levels[drone_id]

def drain_on_send(drone_id, avg_motor_power):
    with lock:
        drain = DRAIN_PER_SEND + (avg_motor_power * DRAIN_MOTOR_FAC)
        battery_levels[drone_id] = max(0.0, battery_levels[drone_id] - drain)
        return battery_levels[drone_id]

def get_level(drone_id):
    return battery_levels[drone_id]

def check_return_to_base(drone_id):
    lvl = battery_levels[drone_id]
    if lvl < 20 and drone_id not in returned_to_base:
        returned_to_base.add(drone_id)
        return True, lvl
    return False, lvl

def should_enqueue(drone_id):
    # below 10% we stop processing new frames
    return battery_levels[drone_id] >= 10
