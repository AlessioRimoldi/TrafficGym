from enum import Enum, auto
from typing import TypedDict
from trafficgym.engine.ports.simulation import SimulationPort
from trafficgym.engine.control.registry import block, BlockParam


class RampMeterInputs(TypedDict):
    occupancy: float

class RampMeterOutputs(TypedDict, total=False):
    program_id: str


@block("Ramp Meter")
class RampMeterController:
    """Hysteretic state-machine ramp meter with four states (OFF / QUARTER / TENTH / CHOKE).
    Transitions are triggered by occupancy thresholds with hysteresis to prevent chattering."""

    class State(Enum):
        OFF = auto()
        QUARTER = auto()
        TENTH = auto()
        CHOKE = auto()

    _PROGRAMS = {State.OFF: "off", State.QUARTER: "1", State.TENTH: "2", State.CHOKE: "3"}

    def __init__(
        self,
        off_up: float = 15,
        quarter_up: float = 20,
        quarter_down: float = 10,
        tenth_up: float = 30,
        tenth_down: float = 15,
        choke_down: float = 25,
    ) -> None:
        self.state = self.State.OFF
        self.off_up = off_up
        self.quarter_up = quarter_up
        self.quarter_down = quarter_down
        self.tenth_up = tenth_up
        self.tenth_down = tenth_down
        self.choke_down = choke_down

    def determine_new_program(self, occupancy: float) -> str | None:
        prev = self.state
        match self.state:
            case self.State.OFF:
                if occupancy >= self.off_up: self.state = self.State.QUARTER
            case self.State.QUARTER:
                if occupancy >= self.quarter_up:     self.state = self.State.TENTH
                elif occupancy <= self.quarter_down: self.state = self.State.OFF
            case self.State.TENTH:
                if occupancy >= self.tenth_up:       self.state = self.State.CHOKE
                elif occupancy <= self.tenth_down:   self.state = self.State.QUARTER
            case self.State.CHOKE:
                if occupancy <= self.choke_down:     self.state = self.State.TENTH
        return self._PROGRAMS[self.state] if self.state != prev else None

    def step(self, adapter: SimulationPort, inputs: RampMeterInputs) -> RampMeterOutputs:
        new_program = self.determine_new_program(inputs["occupancy"])
        return {"program_id": new_program} if new_program else {}


class StaticTLSInputs(TypedDict):
    sim_time: float

class StaticTLSOutputs(TypedDict, total=False):
    state: str


@block("Static TLS", extra_params=[BlockParam("phase_rows", "phase_list", "Phases")])
class StaticTLSController:
    """Fixed-cycle traffic light controller. Cycles through a list of phase states,
    holding each for its configured duration in seconds."""

    def __init__(self, phases: list[str], durations: list[int]) -> None:
        self.phases = phases
        self.durations = durations
        self._phase_index = 0
        self._phase_start: float | None = None

    def step(self, adapter: SimulationPort, inputs: StaticTLSInputs) -> StaticTLSOutputs:
        sim_time = inputs["sim_time"]
        if self._phase_start is None:
            self._phase_start = sim_time
            return {"state": self.phases[0]}
        if sim_time - self._phase_start >= self.durations[self._phase_index]:
            self._phase_index = (self._phase_index + 1) % len(self.phases)
            self._phase_start = sim_time
            return {"state": self.phases[self._phase_index]}
        return {}


class ALINEAProportionalInput(TypedDict):
    occupancy: float

class ALINEAProportionalOutput(TypedDict):
    meter_rate_veh_per_h: float


@block("ALINEA-P")
class ALINEAProportional:
    """ALINEA proportional ramp metering controller.
    Computes r(k) = r(k-1) + Kr * (rho_star - rho(k)), where rho is downstream
    occupancy (%) and Kr is the proportional gain (veh/h per %)."""

    def __init__(self, target_occupancy: float = 20.0, Kr: float = 70.0, saturation: float = 5.0):
        self.r_min = 180.0
        self.r_max = 1800.0
        self.o_hat = target_occupancy
        self.Kr = Kr
        self.last_meter_rate = self.r_max
        self.saturation = saturation

    def step(self, adapter: SimulationPort, inputs: ALINEAProportionalInput) -> ALINEAProportionalOutput:
        new_meter_rate = self.last_meter_rate + self.Kr * min((self.o_hat - inputs["occupancy"]), self.saturation)
        new_meter_rate = max(self.r_min, min(self.r_max, new_meter_rate))
        self.last_meter_rate = new_meter_rate
        return {"meter_rate_veh_per_h": new_meter_rate}


class ALINEAPDInput(TypedDict):
    occupancy: float

class ALINEAPDOutput(TypedDict):
    meter_rate_veh_per_h: float


@block("ALINEA-PD")
class ALINEAProportionalDerivative:
    """PD variant of ALINEA ramp metering.
    r(k) = r(k-1) + Kr*(rho_star - rho(k)) + Kd*(rho(k-1) - rho(k))
    Kr drives occupancy to the setpoint; Kd anticipates trends via the rate of change."""

    def __init__(self, target_occupancy: float = 20.0, Kr: float = 70.0, Kd: float = 35.0, up_saturation: float = 5.0):
        self.r_min = 180.0
        self.r_max = 1800.0
        self.o_hat = target_occupancy
        self.Kr = Kr
        self.Kd = Kd
        self.up_saturation = up_saturation
        self.last_meter_rate = self.r_max
        self.last_occupancy: float | None = None

    def step(self, adapter: SimulationPort, inputs: ALINEAPDInput) -> ALINEAPDOutput:
        occ = inputs["occupancy"]
        p_term = min(self.Kr * (self.o_hat - occ), self.up_saturation)
        d_term = self.Kd * (self.last_occupancy - occ) if self.last_occupancy is not None else 0.0
        new_meter_rate = max(self.r_min, min(self.r_max, self.last_meter_rate + p_term + d_term))
        self.last_meter_rate = new_meter_rate
        self.last_occupancy = occ
        return {"meter_rate_veh_per_h": new_meter_rate}


class ALINEAPIInput(TypedDict):
    occupancy: float

class ALINEAPIOutput(TypedDict):
    meter_rate_veh_per_h: float


@block("ALINEA-PI")
class ALINEAProportionalIntegral:
    """PI variant of ALINEA ramp metering (Smaragdis & Papageorgiou 2003).
    r(k) = r(k-1) + Kp*(rho(k-1) - rho(k)) + Ki*(rho_star - rho(k))
    Ki drives occupancy to the setpoint (same role as Kr in ALINEA-P).
    Kp damps oscillations by reacting to the rate of occupancy change."""

    def __init__(self, target_occupancy: float = 20.0, Kp: float = 35.0, Ki: float = 70.0):
        self.r_min = 180.0
        self.r_max = 1800.0
        self.o_hat = target_occupancy
        self.Kp = Kp
        self.Ki = Ki
        self.last_meter_rate = self.r_max
        self.last_occupancy: float | None = None

    def step(self, adapter: SimulationPort, inputs: ALINEAPIInput) -> ALINEAPIOutput:
        occ = inputs["occupancy"]
        p_term = self.Kp * (self.last_occupancy - occ) if self.last_occupancy is not None else 0.0
        i_term = self.Ki * (self.o_hat - occ)
        new_meter_rate = max(self.r_min, min(self.r_max, self.last_meter_rate + p_term + i_term))
        self.last_meter_rate = new_meter_rate
        self.last_occupancy = occ
        return {"meter_rate_veh_per_h": new_meter_rate}


