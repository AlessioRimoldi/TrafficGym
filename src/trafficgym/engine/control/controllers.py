from enum import Enum, auto
from trafficgym.engine.ports.simulation import SimulationPort

class RampMeterController:
    class State(Enum):
        OFF = auto()
        QUARTER = auto()
        TENTH = auto()
        CHOKE = auto()
    
    _PROGRAMS = {State.OFF: "off", State.QUARTER: "1", State.TENTH: "2", State.CHOKE: "3"}

    def __init__(self, tls_id: str, det_id: str) -> None:
            self.tls_id = tls_id
            self.det_id = det_id
            self.state = self.State.OFF

    def _step(self, occupancy: float) -> str | None:
        prev = self.state
        match self.state:
            case self.State.OFF:
                if occupancy >= 15: self.state = self.State.QUARTER
            case self.State.QUARTER:
                if occupancy >= 20:   self.state = self.State.TENTH
                elif occupancy <= 10: self.state = self.State.OFF
            case self.State.TENTH:
                if occupancy >= 30:   self.state = self.State.CHOKE
                elif occupancy <= 15: self.state = self.State.QUARTER
            case self.State.CHOKE:
                if occupancy <= 25: self.state = self.State.TENTH
        return self._PROGRAMS[self.state] if self.state != prev else None

    def on_tick(self, adapter: SimulationPort, sim_time: float) -> None:
        occ = float(adapter.query("inductionloop", "getLastIntervalOccupancy", self.det_id, {}))
        new_program = self._step(occ)
        if new_program is not None:
            adapter.apply("trafficlight", "setProgram", self.tls_id, {"programID": new_program})


class StaticTLSController:
    def __init__(self, tls_id: str, phases: list[str], durations: list[int]) -> None:
        self.tls_id = tls_id
        self.phases = phases
        self.durations = durations
        self._phase_index = 0
        self._phase_start: float | None = None

    def on_tick(self, adapter: SimulationPort, sim_time: float) -> None:
        if self._phase_start is None:
            self._phase_start = sim_time
            adapter.apply("trafficlight", "setRedYellowGreenState", self.tls_id, {"state": self.phases[0]})
            return
        if sim_time - self._phase_start >= self.durations[self._phase_index]:
            self._phase_index = (self._phase_index + 1) % len(self.phases)
            self._phase_start = sim_time
            adapter.apply("trafficlight", "setRedYellowGreenState", self.tls_id, {"state": self.phases[self._phase_index]})
