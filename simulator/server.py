import time, math, random
from pymodbus.server import StartTcpServer
from pymodbus.datastore import (
    ModbusSlaveContext, ModbusServerContext, ModbusSequentialDataBlock
)

NUM_METERS = 8

def clamp(val, lo=0, hi=65535):
    return max(lo, min(hi, int(val)))

def build_context():
    slaves = {}
    for i in range(1, NUM_METERS + 1):
        block = ModbusSequentialDataBlock(0, [0] * 50)
        slaves[i] = ModbusSlaveContext(ir=block)
    return ModbusServerContext(slaves=slaves, single=False)

def update_registers(context):
    t = time.time()
    for i in range(1, NUM_METERS + 1):
        # --- frequency_Hz (register 0) ---
        # Simulate slight grid drift around 50 Hz, scaled x100
        frequency = clamp((50.0 + 0.3 * math.sin(t / 20 + i)) * 100)

        # --- voltage_V (register 1) ---
        # Nominal 230V with small fluctuation, scaled x10
        voltage = clamp((230.0 + random.uniform(-5, 5)) * 10)

        # --- current_A (register 2) ---
        # Load varies per meter, scaled x10
        base_load = 15 + i * 3
        current = clamp((base_load + 5 * math.sin(t / 10 + i) + random.uniform(-0.5, 0.5)) * 10)

        # --- power_factor (register 3) ---
        # Realistic PF between 0.75 and 0.99, scaled x100
        pf = clamp((0.85 + 0.08 * math.sin(t / 15 + i)) * 100)

        # --- thd_voltage_pct (register 4) ---
        # Typically 2-8%, scaled x10
        thd_v = clamp((4.0 + 2 * math.sin(t / 12 + i) + random.uniform(-0.5, 0.5)) * 10)

        # --- thd_current_pct (register 5) ---
        # Typically 5-20%, scaled x10
        thd_i = clamp((12.0 + 5 * math.sin(t / 9 + i) + random.uniform(-1, 1)) * 10)

        # --- active_energy_kWh (register 6) ---
        # Monotonically increasing energy counter (resets at 65535)
        # Base accumulation rate differs per meter
        kwh = clamp(math.fmod(t * (0.01 + i * 0.002), 65535))

        context[i].setValues(4, 0, [
            frequency,   # reg 0 → frequency_Hz ×100
            voltage,     # reg 1 → voltage_V ×10
            current,     # reg 2 → current_A ×10
            pf,          # reg 3 → power_factor ×100
            thd_v,       # reg 4 → thd_voltage_pct ×10
            thd_i,       # reg 5 → thd_current_pct ×10
            kwh,         # reg 6 → active_energy_kWh (raw)
        ])

context = build_context()

import threading
def loop():
    while True:
        update_registers(context)
        time.sleep(5)

threading.Thread(target=loop, daemon=True).start()
StartTcpServer(context=context, address=("0.0.0.0", 1502))
def update_registers(context):
    t = time.time()
    for i in range(1, NUM_METERS + 1):
        base = 200 + i * 10
        kw      = clamp((base + 20 * math.sin(t / 10 + i)) * 10)
        kvar    = clamp((base * 0.3 + 5 * math.sin(t / 8 + i)) * 10)
        voltage = clamp((220 + random.uniform(-2, 2)) * 10)
        current = clamp((kw / max(voltage, 1) + random.uniform(-0.1, 0.1)) * 10)
        # With this — reset every hour to stay in range:
        kwh = clamp((math.fmod(t, 3600) / 3600 * base) * 10)
        context[i].setValues(4, 0, [kw, kvar, voltage, current, kwh])

context = build_context()

import threading
def loop():
    while True:
        update_registers(context)
        time.sleep(5)

threading.Thread(target=loop, daemon=True).start()
StartTcpServer(context=context, address=("0.0.0.0", 1502))
