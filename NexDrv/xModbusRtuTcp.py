from __future__ import annotations

from dataclasses import dataclass
from typing import List

from pymodbus.client.sync import ModbusSerialClient


@dataclass(frozen=True)
class RtuParams:
    baudrate: int
    bytesize: int
    parity: str
    stopbits: int
    timeout: float


class ModbusRtuTcpDriver:
    """
    只負責：透過 RTU-over-TCP 讀 raw registers
    不做 mapping、不做 envelope、不做 Rabbit
    """

    def __init__(self, host: str, port: int, rtu: RtuParams):
        self.host = host
        self.port = port
        self.rtu = rtu

    def read_registers(self, *, fc: int, address: int, count: int, unit: int) -> List[int] | None:
        cli = ModbusSerialClient(
            method="rtu",
            port=f"socket://{self.host}:{self.port}",
            baudrate=self.rtu.baudrate,
            bytesize=self.rtu.bytesize,
            parity=self.rtu.parity,
            stopbits=self.rtu.stopbits,
            timeout=self.rtu.timeout,
        )

        if not cli.connect():
            return None

        try:
            if fc == 3:
                rr = cli.read_holding_registers(address, count, unit=unit)
            elif fc == 4:
                rr = cli.read_input_registers(address, count, unit=unit)
            else:
                raise ValueError(f"Unsupported function code: {fc}")

            if rr.isError():
                return None

            return rr.registers
        finally:
            try:
                cli.close()
            except Exception:
                pass
