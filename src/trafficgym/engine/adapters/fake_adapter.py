from trafficgym.engine.ports.simulation import SimulationPort, ValDict
from google.protobuf.struct_pb2 import Value
import logging

FakeStateDict = dict[tuple[str, str], ValDict]


class FakeAdapter(SimulationPort):
    state: FakeStateDict

    def __init__(
        self, step_length_ms: int, initial_config: FakeStateDict | None = None
    ) -> None:
        super().__init__(step_length_ms)
        self.state: FakeStateDict = initial_config or {}

    def start(self) -> None:
        if self.closed:
            raise RuntimeError("fake run already closed")
        self.started = True

    def close(self) -> None:
        if self.closed:
            raise RuntimeError("fake run already closed")
        self.closed = True

    def tick(self) -> tuple[int, float, ValDict]:
        if self.closed:
            raise RuntimeError("fake run already closed")
        self.step += 1

        return self.step, self.step * self.seconds_per_step, {}

    def apply(
        self,
        domain: str,
        setter_name: str,
        args: ValDict,
    ) -> None:
        if self.closed:
            raise RuntimeError("fake run already closed")
        guess_name = setter_name.removeprefix("set")
        self.state[(domain, guess_name)] = args

        logging.debug(f"Invoked fake setter: {domain}.{setter_name}_{args}")

    def query(
        self,
        domain: str,
        getter_name: str,
        args: ValDict,  # not sure what to do with args here...
    ) -> Value:
        if self.closed:
            raise RuntimeError("fake run already closed")
        guess_name = getter_name.removeprefix("get")

        args = self.state[(domain, guess_name)]
        return args["value"]
