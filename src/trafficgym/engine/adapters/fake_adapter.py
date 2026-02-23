from trafficgym.engine.ports.simulation import (
    SimulationPort,
    ValDict,
    InvalidGetterError,
)
from google.protobuf.struct_pb2 import Value
from dataclasses import dataclass
from trafficgym.engine.helpers import extract_value
from libsumo import TraCIException  # type: ignore[import-untyped]
import logging


@dataclass(frozen=True, eq=True)
class FakeStateDictKey:
    domain: str
    object_id: str
    attribute: str


FakeStateDict = dict[FakeStateDictKey, Value]


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
        object_id: str,
        args: ValDict,
    ) -> None:
        """Will set the fake state for the Value of the attribute whose
        name is the setter_name, with the 'set' prefix removed, lowercased.

        The value will be set to what the key 'value' in args is, defaulting to None.

        noNegativeNumber is a special property in the fake which simulates a property for which libsumo
        might reject a certain input. For example, libsumo.setSpeed() might accept -1 to reset the speed control
        but would reject other inputs. The fake will reject all negative numbers with a TraCIException.
        """
        if self.closed:
            raise RuntimeError("fake run already closed")
        guess_name = setter_name.removeprefix("set")

        if guess_name == "NoNegativeNumber":
            value = args.get("value")
            if value is not None and value.WhichOneof("kind") == "number_value":
                if value.number_value < 0:
                    raise TraCIException(
                        "invalid setter argument for noNegativeNumber"
                    )
            else:
                raise Exception("Could not set noNegativeNumber")

        if not "value" in args:
            raise KeyError("'value' must be set in args for FakeState")

        self.state[
            FakeStateDictKey(
                domain=domain, attribute=guess_name, object_id=object_id
            )
        ] = args["value"]

        logging.debug(
            f"Invoked fake setter: {domain}.{object_id}.{setter_name}_{args}"
        )

    def query(
        self,
        domain: str,
        getter_name: str,
        object_id: str,
        args: ValDict,  # not sure what to do with args here...
    ) -> str:
        """Will query the fake state for the Value of the attribute whose
        name is the getter_name, with the 'get' prefix removed, lowercased.

        For now, args are ignored"""
        if self.closed:
            raise RuntimeError("fake run already closed")
        guess_name = getter_name.removeprefix("get")

        state_key = FakeStateDictKey(
            domain=domain, attribute=guess_name, object_id=object_id
        )

        attribute_value = self.state[state_key]
        # if attribute_value is None:
        #     # attribute_value = Value(null_value=Value.null_value)
        #     raise InvalidGetterError("Attribute not found in fake state")

        logging.debug(
            f"Invoked fake getter: {domain}.{object_id}.{getter_name}_{args}"
        )
        # return attribute_value
        return str(extract_value(attribute_value))
