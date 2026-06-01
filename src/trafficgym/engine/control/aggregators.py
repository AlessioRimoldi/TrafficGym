from __future__ import annotations
from collections import deque
from typing import Any
from trafficgym.engine.ports.simulation import SimulationPort
from trafficgym.engine.control.registry import block


@block("Mean")
class Mean:
    """Averages all values in the inputs dict and returns a single output key.
    Connect multiple observer nodes to this block's input port to fan-in their
    readings into one averaged signal for a downstream controller."""

    def __init__(self, output_key: str = "value") -> None:
        self._output_key = output_key

    def step(self, adapter: SimulationPort, inputs: dict[str, Any]) -> dict[str, Any]:
        if not inputs:
            return {}
        mean = sum(float(v) for v in inputs.values()) / len(inputs)
        return {self._output_key: mean}


@block("Rolling Avg")
class RollingAverage:
    """Smooths each input key independently with a rolling average
    over a fixed window of recent values."""

    def __init__(self, window: int = 10) -> None:
        self._window = window
        self._bufs: dict[str, deque[float]] = {}

    def _buf(self, key: str) -> deque[float]:
        if key not in self._bufs:
            self._bufs[key] = deque(maxlen=self._window)
        return self._bufs[key]

    def step(self, adapter: SimulationPort, inputs: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for k, v in inputs.items():
            buf = self._buf(k)
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
