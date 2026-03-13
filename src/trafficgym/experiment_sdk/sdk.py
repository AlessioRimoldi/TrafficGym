from trafficgym.api import engine_pb2
from trafficgym.engine.helpers import (
    extract_value,
    ExtractedValueType as ValueType,
)
from google.protobuf import message
from dataclasses import dataclass
from typing import Literal
from enum import Enum
from abc import ABC


class SDKError(Exception): ...


class InvalidArgumentError(SDKError): ...


class NotFoundError(SDKError): ...


class AbortedError(SDKError): ...


class ServiceUnavailableError(SDKError): ...


class GrpcError(SDKError): ...


class SdkBase(ABC): ...
    # @abstractmethod
    # def to_proto(self) -> message.Message: ...

    # @staticmethod
    # @abstractmethod
    # def from_proto(proto: message.Message) -> SdkBase: ...

    # @abstractmethod
    # def to_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True)
class Value(SdkBase):
    value: ValueType

    @staticmethod
    def from_proto(proto: engine_pb2.CustomValue) -> Value:
        return Value(extract_value(proto))

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
            raise TypeError(
                f"Unsupported type for protobuf Value: {type(self.value)}"
            )

        return proto

    def to_dict(self) -> dict[str, object]:
        return {"value": self.value}


@dataclass(frozen=True)
class Action(SdkBase):
    domain: str
    setter_name: str
    object_id: str
    parameters: list[tuple[str, Value]]

    @staticmethod
    def from_proto(proto: engine_pb2.Action) -> Action:
        return Action(
            proto.setter.domain,
            proto.setter.setter_name,
            proto.setter.object_id,
            [
                (param.name, Value.from_proto(param.value))
                for param in proto.setter.parameters
            ],
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "setter_name": self.setter_name,
            "object_id": self.object_id,
            "parameters": [
                (name, value.to_dict()) for (name, value) in self.parameters
            ],
        }


@dataclass(frozen=True)
class ActionBundle(SdkBase):
    bundle: list[Action]

    @staticmethod
    def from_proto(proto: engine_pb2.ActionBundle) -> ActionBundle:
        return ActionBundle(
            [Action.from_proto(p_action) for p_action in proto.actions]
        )

    def to_proto(self) -> engine_pb2.ActionBundle:
        return engine_pb2.ActionBundle(
            actions=[
                engine_pb2.Action(
                    setter=engine_pb2.GenericSetter(
                        domain=a.domain,
                        setter_name=a.setter_name,
                        object_id=a.object_id,
                        parameters=[
                            engine_pb2.Parameter(
                                name=p[0], value=p[1].to_proto()
                            )
                            for p in a.parameters
                        ],
                    )
                )
                for a in self.bundle
            ]
        )

    def to_dict(self) -> dict[str, object]:
        return {"bundle": [action.to_dict() for action in self.bundle]}


@dataclass(frozen=True)
class KeyValue(SdkBase):
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
            return engine_pb2.NamedNullableString(
                name=self.key, has_value=False
            )
        else:
            return engine_pb2.NamedNullableString(
                name=self.key, has_value=True, value=str(self.data)
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "value": None if self.value is None else self.value.to_dict(),
        }

    @property
    def data(self) -> ValueType:
        if self.value is None:
            return None
        else:
            return self.value.value


@dataclass(frozen=True)
class TelemetryFrame(SdkBase):
    run_id: str
    step: int
    sim_time_s: float
    metrics: list[KeyValue]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "step": self.step,
            "sim_time_s": self.sim_time_s,
            "metrics": [kv.to_dict() for kv in self.metrics],
        }


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
class InterruptEvent(SdkBase):
    run_id: str
    interrupt_id: str
    event_id: str
    observed_value: str

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "interrupt_id": self.interrupt_id,
            "event_id": self.event_id,
            "observed_value": self.observed_value,
        }


@dataclass(frozen=True)
class TriggerConditions(SdkBase):
    subscription_fingerprint: str
    value: Value
    operation: Operation

    def to_proto(self) -> engine_pb2.TriggerMetricNameAndValue:
        return engine_pb2.TriggerMetricNameAndValue(
            subscription_fingerprint=self.subscription_fingerprint,
            value=self.value.to_proto(),
            op=self.operation.to_proto(),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "subscription_fingerprint": self.subscription_fingerprint,
            "value": self.value.to_dict(),
            "operation": self.operation,
        }


@dataclass(frozen=True)
class CreateRunRequest(SdkBase):
    sumocfg_path: str
    sumo_binary: str
    step_length_ms: int

    @staticmethod
    def from_proto(proto: engine_pb2.CreateRunRequest) -> CreateRunRequest:
        return CreateRunRequest(proto.sumocfg_path, proto.sumo_binary, proto.step_length_ms)

    def to_proto(self) -> engine_pb2.CreateRunRequest:
        return engine_pb2.CreateRunRequest(sumocfg_path=self.sumocfg_path, sumo_binary=self.sumo_binary, step_length_ms=self.step_length_ms)

    def to_dict(self) -> dict[str, object]:
        return { "sumocfg_path": self.sumocfg_path, "sumo_binary": self.sumo_binary, "step_length_ms": self.step_length_ms }

@dataclass(frozen=True)
class CreateRunResponse(SdkBase):
    run_id: str

    @staticmethod
    def from_proto(proto: engine_pb2.CreateRunResponse) -> CreateRunResponse:
        return CreateRunResponse(proto.run_id)

    def to_dict(self) -> dict[str, object]:
        return { "run_id": self.run_id }


@dataclass(frozen=True)
class RunRequest(SdkBase):
    run_id: str
    run_mode: tuple[Literal["max_steps", "max_time", "steps", "time"], float | int]

    @staticmethod
    def from_proto(proto: engine_pb2.RunRequest) -> RunRequest:
        field = proto.WhichOneof("run_mode")

        if field is None:
            raise ValueError("Run Request must have a run mode")

        return RunRequest(proto.run_id, (field, getattr(proto, field)))

    def to_proto(self) -> engine_pb2.RunRequest:
        field, value = self.run_mode

        proto = engine_pb2.RunRequest(run_id=self.run_id)
        setattr(proto, field, value)

        return proto

    def to_dict(self) -> dict[str, object]:
        mode, value = self.run_mode

        return { "run_id": self.run_id, "run_mode": { "type": mode, "value": value } }

@dataclass(frozen=True)
class RunResponse(SdkBase):
    new_step: int
    new_time: float

    @staticmethod
    def from_proto(proto: engine_pb2.RunResponse) -> RunResponse:
        return RunResponse(proto.new_step, proto.new_time)

    def to_dict(self) -> dict[str, object]:
        return {"new_step": self.new_step, "new_time": self.new_time}


@dataclass(frozen=True)
class ApplyActionsRequest(SdkBase):
    run_id: str
    action_bundle: ActionBundle

    @staticmethod
    def from_proto(
        proto: engine_pb2.ApplyActionsRequest,
    ) -> ApplyActionsRequest:
        return ApplyActionsRequest(
            proto.run_id, ActionBundle.from_proto(proto.action_bundle)
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "action_bundle": self.action_bundle.to_dict(),
        }


@dataclass(frozen=True)
class ApplyActionsResponse(SdkBase):
    errors: list[str]

    @staticmethod
    def from_proto(
        proto: engine_pb2.ApplyActionsResponse,
    ) -> ApplyActionsResponse:
        return ApplyActionsResponse(list(proto.errors))

    def to_dict(self) -> dict[str, object]:
        return {"errors": self.errors}


@dataclass(frozen=True)
class SubscriptionResponse(SdkBase):
    fingerprint: str

    @staticmethod
    def from_proto(proto: engine_pb2.SubscribeResponse) -> SubscriptionResponse:
        return SubscriptionResponse(proto.fingerprint)

    def to_dict(self) -> dict[str, object]:
        return {"fingerprint": self.fingerprint}
