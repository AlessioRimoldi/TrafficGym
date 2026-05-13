from typing import Protocol, Any


class ControllerNode(Protocol):
    def step(self, inputs: Any) -> Any: ...
