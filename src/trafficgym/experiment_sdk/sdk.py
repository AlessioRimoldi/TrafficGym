from trafficgym.api import engine_pb2
from dataclasses import dataclass
from enum import Enum

ValueType = str | int | float | bool | None

class SDKError(Exception): ...
class InvalidArgumentError(SDKError): ...
class NotFoundError(SDKError): ...
class AbortedError(SDKError): ...
class ServiceUnavailableError(SDKError): ...
class GrpcError(SDKError): ...

@dataclass(frozen=True)
class Value:
    value: ValueType

    def to_proto(self) -> engine_pb2.CustomValue:
        proto = engine_pb2.CustomValue()

        if self.value is None:
            proto.null_value = engine_pb2.CustomValue.null_value

        elif isinstance(self.value, bool):
            proto.bool_value = self.value

        elif isinstance(self.value, int):
            proto.int_value = self.value

        elif isinstance(self.value, float):
            proto.float_value = self.value

        elif isinstance(self.value, str):
            proto.string_value = self.value

        else:
            raise TypeError(f"Unsupported type for protobuf Value: {type(self.value)}")

        return proto


@dataclass(frozen=True)
class Action:
    domain: str
    setter_name: str
    object_id: str
    parameters: list[tuple[str, Value]]


@dataclass(frozen=True)
class ActionBundle:
    bundle: list[Action]

    def to_proto(self) -> engine_pb2.ActionBundle:
        return engine_pb2.ActionBundle(
            actions=[
                engine_pb2.Action(
                    setter=engine_pb2.GenericSetter(
                        domain=a.domain,
                        setter_name=a.setter_name,
                        object_id=a.object_id,
                        parameters=[
                            engine_pb2.Parameter(name=p[0], value=p[1].to_proto())
                            for p in a.parameters
                        ],
                    )
                )
                for a in self.bundle
            ]
        )


@dataclass(frozen=True)
class KeyValue:
    key: str
    value: Value | None

    @staticmethod
    def from_proto(proto: engine_pb2.NamedNullableString) -> KeyValue:
        if proto.has_value:
            return KeyValue(proto.name, Value(proto.value))
        else:
            return KeyValue(proto.name, None)

    def to_proto(self) -> engine_pb2.NamedNullableString:
        if self.value is None:
            return engine_pb2.NamedNullableString(name=self.key, has_value=False)
        else:
            return engine_pb2.NamedNullableString(name=self.key, has_value=True, value=str(self.data))

    @property
    def data(self) -> ValueType:
        if self.value is None:
            return None
        else:
            return self.value.value


@dataclass(frozen=True)
class TelemetryFrame:
    run_id: str
    step: int
    sim_time_s: float
    metrics: list[KeyValue]


class Operation(Enum):
    EQU = engine_pb2.Operation.EQU
    NEQ = engine_pb2.Operation.NEQ
    GRT = engine_pb2.Operation.GRT
    LST = engine_pb2.Operation.LST
    GEQ = engine_pb2.Operation.GEQ
    LEQ = engine_pb2.Operation.LEQ

    def to_proto(self) -> engine_pb2.Operation.ValueType:
        return self.value


@dataclass(frozen=True)
class InterruptEvent:
    run_id: str
    interrupt_id: str
    event_id: str
    observed_value: str


@dataclass(frozen=True)
class TriggerConditions:
    subscription_fingerprint: str
    value: Value
    operation: Operation

    def to_proto(self) -> engine_pb2.TriggerMetricNameAndValue:
        return engine_pb2.TriggerMetricNameAndValue(
            subscription_fingerprint=self.subscription_fingerprint,
            value=self.value.to_proto(),
            op=self.operation.to_proto(),
        )

@dataclass(frozen=True)
class RunResponse:
    new_step: int
    new_time: float

    @staticmethod
    def from_proto(proto: engine_pb2.RunResponse) -> RunResponse:
        return RunResponse(proto.new_step, proto.new_time)

@dataclass(frozen=True)
class ApplyActionsResponse:
    errors: list[str]

    @staticmethod
    def from_proto(proto: engine_pb2.ApplyActionsResponse) -> ApplyActionsResponse:
        return ApplyActionsResponse(list(proto.errors))

@dataclass(frozen=True)
class SubscriptionResponse:
    fingerprint: str

    @staticmethod
    def from_proto(proto: engine_pb2.SubscribeResponse) -> SubscriptionResponse:
        return SubscriptionResponse(proto.fingerprint)
