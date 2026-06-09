from __future__ import annotations

from typing import Any, TypedDict

from trafficgym.engine.ports.simulation import SimulationPort
from trafficgym.engine.control.registry import block, BlockParam


class RatioActuatorInputs(TypedDict):
    duty_setpoint: float

class RatioActuatorOutputs(TypedDict, total=False):
    phase: int

@block("Green Time Ratio Actuator", extra_params=[BlockParam("duty_phase", "select", "Duty Phase", "0", ["0", "2"])])
class RatioActuator:
    """Actuates a four legged intersection traffic light such that phase i is active `duty`% of the time.
    This actuator expects four phases. Phases 0 and 2 are traffic phases, whilst phases 1 and 3 are clearing phases (yellow aspects)."""

    def __init__(
        self,
        duty_phase: str = "0",
        max_duty: float = 0.8,
        min_duty: float = 0.2,
        cycle_duration_s: float = 30.0,
    ):
        self.duty_phase = int(duty_phase)
        self.complement_phase = (self.duty_phase + 2) % 4
        self.max_duty = max_duty
        self.min_duty = min_duty
        self.cycle_duration_s = cycle_duration_s

        self._current_phase: int = self.duty_phase
        self._phase_elapsed_s: float = 0.0
        self._duty_elapsed_s: float = 0.0
        self._cycle_elapsed_s: float = 0.0

    def step(self, adapter: SimulationPort, inputs: RatioActuatorInputs) -> RatioActuatorOutputs:
        dt = adapter.seconds_per_step
        duty = max(self.min_duty, min(self.max_duty, inputs["duty_setpoint"]))

        self._phase_elapsed_s += dt
        self._cycle_elapsed_s += dt

        # Reset cycle tracking
        if self._cycle_elapsed_s >= self.cycle_duration_s:
            self._cycle_elapsed_s = 0.0
            self._duty_elapsed_s = 0.0

        # Track time spent in duty phase this cycle
        if self._current_phase == self.duty_phase:
            self._duty_elapsed_s += dt

        next_phase = self._current_phase

        if self._current_phase == self.duty_phase:
            # Switch to clearing phase if duty quota is met
            duty_quota = duty * self.cycle_duration_s
            if self._duty_elapsed_s >= duty_quota:
                next_phase = (self._current_phase + 1) % 4
                self._phase_elapsed_s = 0.0

        elif self._current_phase == self.complement_phase:
            # Switch to complement clearing phase when remaining time is right
            remaining = self.cycle_duration_s - self._cycle_elapsed_s
            complement_quota = (1.0 - duty) * self.cycle_duration_s
            if self._phase_elapsed_s >= complement_quota:
                next_phase = (self._current_phase + 1) % 4
                self._phase_elapsed_s = 0.0

        elif self._current_phase in (1, 3):
            # Yellow clearing phase — fixed 2s
            if self._phase_elapsed_s >= 2.0:
                next_phase = (self._current_phase + 1) % 4
                self._phase_elapsed_s = 0.0

        if next_phase != self._current_phase:
            self._current_phase = next_phase
            return {"phase": self._current_phase}

        return {}

class MeterRateToDutyInputs(TypedDict):
    meter_rate_veh_per_h: float

class MeterRateToDutyOutputs(TypedDict):
    duty_setpoint: float

@block("Meter Rate to Duty Converter")
class MeterRateToDuty:
    """Converts a metering rate (veh/h) to a green time duty fraction
    for the RatioActuator, given a saturation flow and cycle duration."""

    def __init__(
        self,
        saturation_flow_veh_per_h: float = 1800.0,
        cycle_duration_s: float = 30.0,
    ):
        self.saturation_flow = saturation_flow_veh_per_h
        self.cycle_duration_h = cycle_duration_s / 3600.0

    def step(self, adapter: SimulationPort, inputs: MeterRateToDutyInputs) -> MeterRateToDutyOutputs:
        duty = inputs["meter_rate_veh_per_h"] / (self.saturation_flow * (1 / self.cycle_duration_h))
        duty = max(0.0, min(1.0, duty))
        return {"duty_setpoint": duty}

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
