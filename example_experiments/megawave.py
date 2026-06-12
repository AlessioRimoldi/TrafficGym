from typing import Any
from trafficgym.engine.experiment import Experiment
from trafficgym.engine.ports.simulation import SimulationPort
from trafficgym.engine.control.controllers import SixSigGreenWave
import contextlib

program = [10, 3, 10, 3, 30, 3, 30, 3]

class grid(Experiment):
    def run(self, adapter: SimulationPort) -> None:
        _blk_0 = SixSigGreenWave(prop_delay_s=17, eight_durations=program, initial_offset_s=17 * 0)
        _blk_1 = SixSigGreenWave(prop_delay_s=17, eight_durations=program, initial_offset_s=17 * 1)
        _blk_2 = SixSigGreenWave(prop_delay_s=17, eight_durations=program, initial_offset_s=17 * 2)
        _blk_3 = SixSigGreenWave(prop_delay_s=17, eight_durations=program, initial_offset_s=17 * 3)
        _blk_4 = SixSigGreenWave(prop_delay_s=17, eight_durations=program, initial_offset_s=17 * 4)
        _blk_5 = SixSigGreenWave(prop_delay_s=17, eight_durations=program, initial_offset_s=17 * 5)

        # -------------------------
        # ctrl 0
        # -------------------------
        def _obs__ctrl_0(a: SimulationPort, t: float) -> dict[str, Any]:
            return {}

        def _act__ctrl_0(a: SimulationPort, r: dict[str, Any]) -> None:
            if "phase_0" in r:
                a.apply("trafficlight", "setPhase", "B1", {"index": r["phase_0"]})
            if "phase_1" in r:
                a.apply("trafficlight", "setPhase", "C1", {"index": r["phase_1"]})
            if "phase_2" in r:
                a.apply("trafficlight", "setPhase", "D1", {"index": r["phase_2"]})
            if "phase_3" in r:
                a.apply("trafficlight", "setPhase", "E1", {"index": r["phase_3"]})
            if "phase_4" in r:
                a.apply("trafficlight", "setPhase", "F1", {"index": r["phase_4"]})
            if "phase_5" in r:
                a.apply("trafficlight", "setPhase", "G1", {"index": r["phase_5"]})

        # -------------------------
        # ctrl 1
        # -------------------------
        def _obs__ctrl_1(a: SimulationPort, t: float) -> dict[str, Any]:
            return {}

        def _act__ctrl_1(a: SimulationPort, r: dict[str, Any]) -> None:
            if "phase_0" in r:
                a.apply("trafficlight", "setPhase", "B2", {"index": r["phase_0"]})
            if "phase_1" in r:
                a.apply("trafficlight", "setPhase", "C2", {"index": r["phase_1"]})
            if "phase_2" in r:
                a.apply("trafficlight", "setPhase", "D2", {"index": r["phase_2"]})
            if "phase_3" in r:
                a.apply("trafficlight", "setPhase", "E2", {"index": r["phase_3"]})
            if "phase_4" in r:
                a.apply("trafficlight", "setPhase", "F2", {"index": r["phase_4"]})
            if "phase_5" in r:
                a.apply("trafficlight", "setPhase", "G2", {"index": r["phase_5"]})

        # -------------------------
        # ctrl 2
        # -------------------------
        def _obs__ctrl_2(a: SimulationPort, t: float) -> dict[str, Any]:
            return {}

        def _act__ctrl_2(a: SimulationPort, r: dict[str, Any]) -> None:
            if "phase_0" in r:
                a.apply("trafficlight", "setPhase", "B3", {"index": r["phase_0"]})
            if "phase_1" in r:
                a.apply("trafficlight", "setPhase", "C3", {"index": r["phase_1"]})
            if "phase_2" in r:
                a.apply("trafficlight", "setPhase", "D3", {"index": r["phase_2"]})
            if "phase_3" in r:
                a.apply("trafficlight", "setPhase", "E3", {"index": r["phase_3"]})
            if "phase_4" in r:
                a.apply("trafficlight", "setPhase", "F3", {"index": r["phase_4"]})
            if "phase_5" in r:
                a.apply("trafficlight", "setPhase", "G3", {"index": r["phase_5"]})

        # -------------------------
        # ctrl 3
        # -------------------------
        def _obs__ctrl_3(a: SimulationPort, t: float) -> dict[str, Any]:
            return {}

        def _act__ctrl_3(a: SimulationPort, r: dict[str, Any]) -> None:
            if "phase_0" in r:
                a.apply("trafficlight", "setPhase", "B4", {"index": r["phase_0"]})
            if "phase_1" in r:
                a.apply("trafficlight", "setPhase", "C4", {"index": r["phase_1"]})
            if "phase_2" in r:
                a.apply("trafficlight", "setPhase", "D4", {"index": r["phase_2"]})
            if "phase_3" in r:
                a.apply("trafficlight", "setPhase", "E4", {"index": r["phase_3"]})
            if "phase_4" in r:
                a.apply("trafficlight", "setPhase", "F4", {"index": r["phase_4"]})
            if "phase_5" in r:
                a.apply("trafficlight", "setPhase", "G4", {"index": r["phase_5"]})

        # -------------------------
        # ctrl 4
        # -------------------------
        def _obs__ctrl_4(a: SimulationPort, t: float) -> dict[str, Any]:
            return {}

        def _act__ctrl_4(a: SimulationPort, r: dict[str, Any]) -> None:
            if "phase_0" in r:
                a.apply("trafficlight", "setPhase", "B5", {"index": r["phase_0"]})
            if "phase_1" in r:
                a.apply("trafficlight", "setPhase", "C5", {"index": r["phase_1"]})
            if "phase_2" in r:
                a.apply("trafficlight", "setPhase", "D5", {"index": r["phase_2"]})
            if "phase_3" in r:
                a.apply("trafficlight", "setPhase", "E5", {"index": r["phase_3"]})
            if "phase_4" in r:
                a.apply("trafficlight", "setPhase", "F5", {"index": r["phase_4"]})
            if "phase_5" in r:
                a.apply("trafficlight", "setPhase", "G5", {"index": r["phase_5"]})

        # -------------------------
        # ctrl 5
        # -------------------------
        def _obs__ctrl_5(a: SimulationPort, t: float) -> dict[str, Any]:
            return {}

        def _act__ctrl_5(a: SimulationPort, r: dict[str, Any]) -> None:
            if "phase_0" in r:
                a.apply("trafficlight", "setPhase", "B6", {"index": r["phase_0"]})
            if "phase_1" in r:
                a.apply("trafficlight", "setPhase", "C6", {"index": r["phase_1"]})
            if "phase_2" in r:
                a.apply("trafficlight", "setPhase", "D6", {"index": r["phase_2"]})
            if "phase_3" in r:
                a.apply("trafficlight", "setPhase", "E6", {"index": r["phase_3"]})
            if "phase_4" in r:
                a.apply("trafficlight", "setPhase", "F6", {"index": r["phase_4"]})
            if "phase_5" in r:
                a.apply("trafficlight", "setPhase", "G6", {"index": r["phase_5"]})

        # -------------------------
        # register
        # -------------------------
        with contextlib.ExitStack() as _stack:
            _stack.enter_context(
                adapter.controlled(_blk_0, observe=_obs__ctrl_0, actuate=_act__ctrl_0)
            )
            _stack.enter_context(
                adapter.controlled(_blk_1, observe=_obs__ctrl_1, actuate=_act__ctrl_1)
            )
            _stack.enter_context(
                adapter.controlled(_blk_2, observe=_obs__ctrl_2, actuate=_act__ctrl_2)
            )
            _stack.enter_context(
                adapter.controlled(_blk_3, observe=_obs__ctrl_3, actuate=_act__ctrl_3)
            )
            _stack.enter_context(
                adapter.controlled(_blk_4, observe=_obs__ctrl_4, actuate=_act__ctrl_4)
            )
            _stack.enter_context(
                adapter.controlled(_blk_5, observe=_obs__ctrl_5, actuate=_act__ctrl_5)
            )

            adapter.run_time(3600)
