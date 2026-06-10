from __future__ import annotations
from collections import deque
from typing import Any
from trafficgym.engine.ports.simulation import SimulationPort
from trafficgym.engine.control.registry import block


@block("Mean")
class Mean:
    """Averages numeric values from all connected inputs.
    Connect multiple observer nodes to fan-in their readings into one averaged
    signal for a downstream controller. Non-numeric inputs raise ValueError."""

    def step(self, adapter: SimulationPort, inputs: dict[str, Any]) -> dict[str, Any]:
        if not inputs:
            return {}
        key = next(iter(inputs))
        values = [float(v) for v in inputs.values()]
        return {key: sum(values) / len(values)}

@block("Max")
class Max:
    """Returns the greatest numeric value from all connected inputs.
    Connect multiple observer nodes to fan-in their readings into one signal
    for a downstream controller. Non-numeric inputs raise ValueError."""

    def step(self, adapter: SimulationPort, inputs: dict[str, Any]) -> dict[str, Any]:
        if not inputs:
            return {}
        key = next(iter(inputs))
        return {key: max(float(v) for v in inputs.values())}


@block("Rolling Avg")
class RollingAverage:
    """Smooths each input key independently with a rolling average
    over a fixed window of recent values."""

    def __init__(self, window_s: int = 10) -> None:
        self._window = window_s
        self._bufs: dict[str, deque[float]] = {}

    def _buf(self, key: str, steps_per_s: int) -> deque[float]:
        if key not in self._bufs:
            self._bufs[key] = deque(maxlen=self._window * steps_per_s)
        return self._bufs[key]

    def step(self, adapter: SimulationPort, inputs: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for k, v in inputs.items():
            buf = self._buf(k, int(adapter.steps_per_second))
            buf.append(float(v))
            result[k] = sum(buf) / len(buf)
        return result


@block("Exp Avg")
class ExponentialMovingAverage:
    """Smooths each input key independently with an exponential moving average.
    alpha close to 1 tracks the signal quickly; alpha close to 0 smooths heavily."""

    def __init__(self, alpha: float = 0.3) -> None:
        self._alpha = alpha
        self._emas: dict[str, float] = {}

    def step(self, adapter: SimulationPort, inputs: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for k, v in inputs.items():
            fv = float(v)
            if k not in self._emas:
                self._emas[k] = fv
            else:
                self._emas[k] = self._alpha * fv + (1 - self._alpha) * self._emas[k]
            result[k] = self._emas[k]
        return result
