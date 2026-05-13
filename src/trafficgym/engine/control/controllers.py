from enum import Enum, auto
from typing import TypedDict


class RampMeterInputs(TypedDict):
    occupancy: float

class RampMeterOutputs(TypedDict, total=False):
    program_id: str


class RampMeterController:
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

    def step(self, inputs: RampMeterInputs) -> RampMeterOutputs:
        new_program = self.determine_new_program(inputs["occupancy"])
        return {"program_id": new_program} if new_program else {}


class StaticTLSInputs(TypedDict):
    sim_time: float

class StaticTLSOutputs(TypedDict, total=False):
    state: str


class StaticTLSController:
    def __init__(self, phases: list[str], durations: list[int]) -> None:
        self.phases = phases
        self.durations = durations
        self._phase_index = 0
        self._phase_start: float | None = None

    def step(self, inputs: StaticTLSInputs) -> StaticTLSOutputs:
        sim_time = inputs["sim_time"]
        if self._phase_start is None:
            self._phase_start = sim_time
            return {"state": self.phases[0]}
        if sim_time - self._phase_start >= self.durations[self._phase_index]:
            self._phase_index = (self._phase_index + 1) % len(self.phases)
            self._phase_start = sim_time
            return {"state": self.phases[self._phase_index]}
        return {}
