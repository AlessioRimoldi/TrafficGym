from trafficgym.engine.ports.simulation import (
    SimulationPort,
    MappingValue,
    Observation,
    Params,
)
from dataclasses import dataclass
from libsumo import TraCIException  # type: ignore[import-untyped]
import logging


@dataclass(frozen=True, eq=True)
class FakeStateDictKey:
    domain: str
    object_id: str | None
    attribute: str


FakeStateDict = dict[FakeStateDictKey, MappingValue]


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

    def tick(self) -> tuple[int, float, Observation]:
        if self.closed:
            raise RuntimeError("fake run already closed")
        self.step += 1

        return self.step, self.step * self.seconds_per_step, {}

    def apply(
        self,
        domain: str,
        setter_name: str,
        object_id: str | None,
        args: Params,
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

        value = args.get("value")

        if value is None:
            raise KeyError("'value' must be set in args for FakeState")

        if guess_name == "NoNegativeInt":
            if isinstance(value, int):
                if value < 0:
                    raise TraCIException(
                        "invalid setter argument for noNegativeNumber"
                    )
            else:
                raise Exception("Could not set noNegativeNumber")

        elif guess_name == "NoFloat":
            if not isinstance(value, int):
                raise TypeError("Expected an int")

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
        object_id: str | None,
        args: Params,  # not sure what to do with args here...
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
        return str(attribute_value)
