import time, math, random
from pymodbus.server import StartTcpServer
from pymodbus.datastore import (
    ModbusSlaveContext, ModbusServerContext, ModbusSequentialDataBlock
)

NUM_METERS = 8

def build_context():
    slaves = {}
    for i in range(1, NUM_METERS + 1):
        block = ModbusSequentialDataBlock(0, [0] * 50)
        slaves[i] = ModbusSlaveContext(ir=block)
    return ModbusServerContext(slaves=slaves, single=False)

def update_registers(context):
    t = time.time()
    for i in range(1, NUM_METERS + 1):
        base = 200 + i * 10
        kw      = int((base + 20 * math.sin(t / 10 + i)) * 10)
        kvar    = int((base * 0.3 + 5 * math.sin(t / 8 + i)) * 10)
        voltage = int((220 + random.uniform(-2, 2)) * 10)
        current = int((kw / voltage + random.uniform(-0.1, 0.1)) * 10)
        kwh     = int((t / 3600 * base) * 10)
        context[i].setValues(4, 0, [kw, kvar, voltage, current, kwh])

context = build_context()

import threading
def loop():
    while True:
        update_registers(context)
        time.sleep(2)

threading.Thread(target=loop, daemon=True).start()
StartTcpServer(context=context, address=("0.0.0.0", 1502))
