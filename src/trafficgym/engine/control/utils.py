from __future__ import annotations

from typing import Any, TypedDict

from trafficgym.engine.ports.simulation import SimulationPort
from trafficgym.engine.control.registry import block, BlockParam


class RampMeterCycleActuatorInputs(TypedDict):
    meter_rate_veh_per_h: float


class RampMeterCycleActuatorOutputs(TypedDict, total=False):
    state: str


@block("Cycle Actuator")
class RampMeterCycleActuator:
    """Converts a continuous metering rate (veh/h) into a two-phase TLS state string
    ("G" / "r"). Only emits on phase transitions. Red duration is recalculated at
    each green→red boundary from the current rate, so changes take effect next cycle."""

    def __init__(
        self,
        green_time_s: float = 1.0,
        r_min: float = 180.0,
        r_max: float = 1800.0,
    ):
        self.green_time_s = green_time_s
        self.r_min = r_min
        self.r_max = r_max
        self._phase_elapsed_s: float = 0.0
        self._in_green: bool = False
        self._current_red_s: float = max(1.0, 3600.0 / r_max - green_time_s)

    def step(self, adapter: SimulationPort, inputs: RampMeterCycleActuatorInputs) -> RampMeterCycleActuatorOutputs:
        rate = max(self.r_min, min(self.r_max, inputs["meter_rate_veh_per_h"]))
        self._phase_elapsed_s += adapter.seconds_per_step

        if self._in_green:
            if self._phase_elapsed_s >= self.green_time_s:
                self._in_green = False
                self._phase_elapsed_s = 0.0
                self._current_red_s = max(1.0, 3600.0 / rate - self.green_time_s)
                return {"state": "r"}
        else:
            if self._phase_elapsed_s >= self._current_red_s:
                self._in_green = True
                self._phase_elapsed_s = 0.0
                return {"state": "G"}

        return {}


@block(
    "Constant",
    extra_params=[BlockParam("value_type", "select", "Type", "float", ["float", "int", "str"])],
)
class Constant:
    """Emits a fixed constant value every step under a configurable key.
    Useful for injecting a static setpoint, threshold, or identifier into a pipeline."""

    def __init__(self, value: str = "0", output_key: str = "value", value_type: str = "float") -> None:
        self._raw = value
        self._output_key = output_key
        self._value_type = value_type

    def step(self, adapter: SimulationPort, _inputs: dict[str, Any]) -> dict[str, Any]:
        if self._value_type == "int":
            v: int | float | str = int(float(self._raw))
        elif self._value_type == "float":
            v = float(self._raw)
        else:
            v = str(self._raw)
        return {self._output_key: v}


@block("Rename")
class Renamer:
    """Passes the first input value through unchanged under a new key.
    Use this to connect a block whose output key does not match a downstream
    block's expected input key when the types are compatible."""

    def __init__(self, output_key: str = "value") -> None:
        self._output_key = output_key

    def step(self, adapter: SimulationPort, inputs: dict[str, Any]) -> dict[str, Any]:
        val = next(iter(inputs.values()), None)
        return {self._output_key: val} if val is not None else {}
