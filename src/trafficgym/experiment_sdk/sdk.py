from trafficgym.api import engine_pb2
from trafficgym.engine.helpers import (
    extract_value,
    ExtractedValueType as ValueType,
)
from google.protobuf import message
from dataclasses import dataclass
from typing import Literal, TypeVar, Generic
from typing_extensions import Self
from enum import Enum
from abc import ABC, abstractmethod


class SDKError(Exception): ...


class InvalidArgumentError(SDKError): ...


class NotFoundError(SDKError): ...


class AbortedError(SDKError): ...


class ServiceUnavailableError(SDKError): ...


class GrpcError(SDKError): ...


ProtoT = TypeVar("ProtoT", bound=message.Message)


class SdkBase(ABC, Generic[ProtoT]):
    @abstractmethod
    def to_proto(self) -> ProtoT: ...

    @classmethod
    @abstractmethod
    def from_proto(cls, proto: ProtoT) -> Self: ...

    @abstractmethod
    def to_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True)
class Value(SdkBase[engine_pb2.CustomValue]):
    value: ValueType

    @classmethod
    def from_proto(cls, proto: engine_pb2.CustomValue) -> Self:
        return cls(extract_value(proto))

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
class Parameter(SdkBase[engine_pb2.Parameter]):
    name: str
    value: Value

    @classmethod
    def from_proto(cls, proto: engine_pb2.Parameter) -> Self:
        return cls(proto.name, Value.from_proto(proto.value))

    def to_proto(self) -> engine_pb2.Parameter:
        return engine_pb2.Parameter(name=self.name, value=self.value.to_proto())

    def to_dict(self) -> dict[str, object]:
        return {"name": self.name, "value": self.value.to_dict()}


@dataclass(frozen=True)
class Action(SdkBase[engine_pb2.Action]):
    domain: str
    setter_name: str
    object_id: str
    parameters: list[Parameter]

    @classmethod
    def one_param_action(
        cls,
        domain: str,
        setter_name: str,
        param_name: str,
        value: ValueType,
        object_id: str,
    ) -> Self:
        return cls(
            domain,
            setter_name,
            object_id,
            [Parameter(param_name, Value(value))],
        )

    @classmethod
    def from_proto(cls, proto: engine_pb2.Action) -> Self:
        return cls(
            proto.setter.domain,
            proto.setter.setter_name,
            proto.setter.object_id,
            [Parameter.from_proto(param) for param in proto.setter.parameters],
        )

    def to_proto(self) -> engine_pb2.Action:
        return engine_pb2.Action(
            setter=engine_pb2.GenericSetter(
                domain=self.domain,
                setter_name=self.setter_name,
                object_id=self.object_id,
                parameters=[param.to_proto() for param in self.parameters],
            )
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "domain": self.domain,
            "setter_name": self.setter_name,
            "object_id": self.object_id,
            "parameters": [
                parameter.to_dict() for parameter in self.parameters
            ],
        }


@dataclass(frozen=True)
class ActionBundle(SdkBase[engine_pb2.ActionBundle]):
    bundle: list[Action]

    @classmethod
    def from_proto(cls, proto: engine_pb2.ActionBundle) -> Self:
        return cls([Action.from_proto(p_action) for p_action in proto.actions])

    def to_proto(self) -> engine_pb2.ActionBundle:
        return engine_pb2.ActionBundle(
            actions=[action.to_proto() for action in self.bundle]
        )

    def to_dict(self) -> dict[str, object]:
        return {"bundle": [action.to_dict() for action in self.bundle]}


@dataclass(frozen=True)
class KeyValue(SdkBase[engine_pb2.NamedNullableString]):
    key: str
    value: Value | None

    @classmethod
    def from_proto(cls, proto: engine_pb2.NamedNullableString) -> Self:
        if proto.has_value:
            return cls(proto.name, Value(proto.value))
        else:
            return cls(proto.name, None)

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
class TelemetryFrame(SdkBase[engine_pb2.TelemetryFrame]):
    run_id: str
    step: int
    sim_time_s: float
    metrics: list[KeyValue]

    @classmethod
    def from_proto(cls, proto: engine_pb2.TelemetryFrame) -> Self:
        return cls(
            run_id=proto.run_id,
            step=proto.step,
            sim_time_s=proto.sim_time_s,
            metrics=[KeyValue.from_proto(kv) for kv in proto.metrics],
        )

    def to_proto(self) -> engine_pb2.TelemetryFrame:
        return engine_pb2.TelemetryFrame(
            run_id=self.run_id,
            step=self.step,
            sim_time_s=self.sim_time_s,
            metrics=[metric.to_proto() for metric in self.metrics],
        )

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

    @classmethod
    def from_proto(cls, proto: engine_pb2.Operation.ValueType) -> Self:
        return cls(proto)

    def to_proto(self) -> engine_pb2.Operation.ValueType:
        return self.value

    def to_dict(self) -> str:
        return self.name


@dataclass(frozen=True)
class InterruptEvent(SdkBase[engine_pb2.InterruptEvent]):
    run_id: str
    interrupt_id: str
    event_id: str
    observed_value: str

    @classmethod
    def from_proto(cls, proto: engine_pb2.InterruptEvent) -> Self:
        return cls(
            proto.run_id,
            proto.interrupt_id,
            proto.event_id,
            proto.observed_value,
        )

    def to_proto(self) -> engine_pb2.InterruptEvent:
        return engine_pb2.InterruptEvent(
            run_id=self.run_id,
            interrupt_id=self.interrupt_id,
            event_id=self.event_id,
            observed_value=self.observed_value,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "interrupt_id": self.interrupt_id,
            "event_id": self.event_id,
            "observed_value": self.observed_value,
        }


@dataclass(frozen=True)
class TriggerConditions(SdkBase[engine_pb2.TriggerMetricNameAndValue]):
    subscription_fingerprint: str
    value: Value
    operation: Operation

    @classmethod
    def from_proto(cls, proto: engine_pb2.TriggerMetricNameAndValue) -> Self:
        return cls(
            proto.subscription_fingerprint,
            Value.from_proto(proto.value),
            Operation.from_proto(proto.op),
        )

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
            "operation": self.operation.to_dict(),
        }


@dataclass(frozen=True)
class CreateRunRequest(SdkBase[engine_pb2.CreateRunRequest]):
    sumocfg_path: str
    sumo_binary: str
    step_length_ms: int

    @staticmethod
    def from_proto(proto: engine_pb2.CreateRunRequest) -> CreateRunRequest:
        return CreateRunRequest(
            proto.sumocfg_path, proto.sumo_binary, proto.step_length_ms
        )

    def to_proto(self) -> engine_pb2.CreateRunRequest:
        return engine_pb2.CreateRunRequest(
            sumocfg_path=self.sumocfg_path,
            sumo_binary=self.sumo_binary,
            step_length_ms=self.step_length_ms,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "sumocfg_path": self.sumocfg_path,
            "sumo_binary": self.sumo_binary,
            "step_length_ms": self.step_length_ms,
        }


@dataclass(frozen=True)
class CreateRunResponse(SdkBase[engine_pb2.CreateRunResponse]):
    run_id: str

    @staticmethod
    def from_proto(proto: engine_pb2.CreateRunResponse) -> CreateRunResponse:
        return CreateRunResponse(proto.run_id)

    def to_proto(self) -> engine_pb2.CreateRunResponse:
        return engine_pb2.CreateRunResponse(run_id=self.run_id)

    def to_dict(self) -> dict[str, object]:
        return {"run_id": self.run_id}


@dataclass(frozen=True)
class RunRequest(SdkBase[engine_pb2.RunRequest]):
    run_id: str
    run_mode: tuple[
        Literal["max_steps", "max_time", "steps", "time"], float | int
    ]

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

        return {
            "run_id": self.run_id,
            "run_mode": {"type": mode, "value": value},
        }


@dataclass(frozen=True)
class RunResponse(SdkBase[engine_pb2.RunResponse]):
    run_id: str
    new_step: int
    new_time: float

    @staticmethod
    def from_proto(proto: engine_pb2.RunResponse) -> RunResponse:
        return RunResponse(proto.run_id, proto.new_step, proto.new_time)

    def to_proto(self) -> engine_pb2.RunResponse:
        return engine_pb2.RunResponse(
            run_id=self.run_id, new_step=self.new_step, new_time=self.new_time
        )

    def to_dict(self) -> dict[str, object]:
        return {"new_step": self.new_step, "new_time": self.new_time}


@dataclass(frozen=True)
class ApplyActionsRequest(SdkBase[engine_pb2.ApplyActionsRequest]):
    run_id: str
    action_bundle: ActionBundle

    @staticmethod
    def from_proto(
        proto: engine_pb2.ApplyActionsRequest,
    ) -> ApplyActionsRequest:
        return ApplyActionsRequest(
            proto.run_id, ActionBundle.from_proto(proto.action_bundle)
        )

    def to_proto(self) -> engine_pb2.ApplyActionsRequest:
        return engine_pb2.ApplyActionsRequest(
            run_id=self.run_id, action_bundle=self.action_bundle.to_proto()
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "action_bundle": self.action_bundle.to_dict(),
        }


@dataclass(frozen=True)
class ApplyActionsResponse(SdkBase[engine_pb2.ApplyActionsResponse]):
    errors: list[str]

    @staticmethod
    def from_proto(
        proto: engine_pb2.ApplyActionsResponse,
    ) -> ApplyActionsResponse:
        return ApplyActionsResponse(list(proto.errors))

    def to_proto(self) -> engine_pb2.ApplyActionsResponse:
        return engine_pb2.ApplyActionsResponse(errors=self.errors)

    def to_dict(self) -> dict[str, object]:
        return {"errors": self.errors}


@dataclass(frozen=True)
class SubscriptionRequest(SdkBase[engine_pb2.SubscribeRequest]):
    run_id: str
    domain: str
    getter_name: str
    object_id: str | None
    parameters: list[Parameter]

    @staticmethod
    def from_proto(proto: engine_pb2.SubscribeRequest) -> SubscriptionRequest:
        return SubscriptionRequest(
            proto.run_id,
            proto.domain,
            proto.getter_name,
            proto.object_id,
            [Parameter.from_proto(param) for param in proto.parameters],
        )

    def to_proto(self) -> engine_pb2.SubscribeRequest:
        return engine_pb2.SubscribeRequest(
            run_id=self.run_id,
            domain=self.domain,
            getter_name=self.getter_name,
            object_id=self.object_id if self.object_id is not None else "",
            parameters=[param.to_proto() for param in self.parameters],
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "domain": self.domain,
            "getter_name": self.getter_name,
            "object_id": self.object_id,
            "parameters": [param.to_dict() for param in self.parameters],
        }


@dataclass(frozen=True)
class SubscriptionResponse(SdkBase[engine_pb2.SubscribeResponse]):
    fingerprint: str

    @staticmethod
    def from_proto(proto: engine_pb2.SubscribeResponse) -> SubscriptionResponse:
        return SubscriptionResponse(proto.fingerprint)

    def to_proto(self) -> engine_pb2.SubscribeResponse:
        return engine_pb2.SubscribeResponse(fingerprint=self.fingerprint)

    def to_dict(self) -> dict[str, object]:
        return {"fingerprint": self.fingerprint}


@dataclass(frozen=True)
class CloseRunRequest(SdkBase[engine_pb2.CloseRunRequest]):
    run_id: str

    @staticmethod
    def from_proto(proto: engine_pb2.CloseRunRequest) -> CloseRunRequest:
        return CloseRunRequest(proto.run_id)

    def to_proto(self) -> engine_pb2.CloseRunRequest:
        return engine_pb2.CloseRunRequest(run_id=self.run_id)

    def to_dict(self) -> dict[str, object]:
        return {"run_id": self.run_id}


@dataclass(frozen=True)
class CloseRunResponse(SdkBase[engine_pb2.CloseRunResponse]):
    run_id: str

    @classmethod
    def from_proto(cls, proto: engine_pb2.CloseRunResponse) -> Self:
        return cls(proto.run_id)

    def to_proto(self) -> engine_pb2.CloseRunResponse:
        return engine_pb2.CloseRunResponse(run_id=self.run_id)

    def to_dict(self) -> dict[str, object]:
        return {"run_id": self.run_id}


@dataclass(frozen=True)
class RegisterInterruptRequest(SdkBase[engine_pb2.RegisterInterruptRequest]):
    run_id: str
    trigger_metric: TriggerConditions

    @classmethod
    def from_proto(cls, proto: engine_pb2.RegisterInterruptRequest) -> Self:
        return cls(
            run_id=proto.run_id,
            trigger_metric=TriggerConditions.from_proto(proto.trigger_metric),
        )

    def to_proto(self) -> engine_pb2.RegisterInterruptRequest:
        return engine_pb2.RegisterInterruptRequest(
            run_id=self.run_id, trigger_metric=self.trigger_metric.to_proto()
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "trigger_metric": self.trigger_metric.to_dict(),
        }


@dataclass(frozen=True)
class RegisterInterruptResponse(SdkBase[engine_pb2.RegisterInterruptResponse]):
    interrupt_id: str

    @classmethod
    def from_proto(cls, proto: engine_pb2.RegisterInterruptResponse) -> Self:
        return cls(interrupt_id=proto.interrupt_id)

    def to_proto(self) -> engine_pb2.RegisterInterruptResponse:
        return engine_pb2.RegisterInterruptResponse(
            interrupt_id=self.interrupt_id
        )

    def to_dict(self) -> dict[str, object]:
        return {"interrupt_id": self.interrupt_id}


@dataclass(frozen=True)
class StreamInterruptsRequest(SdkBase[engine_pb2.StreamInterruptsRequest]):
    run_id: str
    interrupt_id: str

    @classmethod
    def from_proto(cls, proto: engine_pb2.StreamInterruptsRequest) -> Self:
        return cls(proto.run_id, proto.interrupt_id)

    def to_proto(self) -> engine_pb2.StreamInterruptsRequest:
        return engine_pb2.StreamInterruptsRequest(
            run_id=self.run_id, interrupt_id=self.interrupt_id
        )

    def to_dict(self) -> dict[str, object]:
        return {"run_id": self.run_id, "interrupt_id": self.interrupt_id}


@dataclass(frozen=True)
class AcknowledgeInterruptRequest(
    SdkBase[engine_pb2.AcknowledgeInterruptRequest]
):
    run_id: str
    interrupt_id: str
    event_id: str
    actions: ActionBundle
    new_interrupt_conditions: TriggerConditions | None

    @classmethod
    def from_proto(cls, proto: engine_pb2.AcknowledgeInterruptRequest) -> Self:
        conditions = (
            TriggerConditions.from_proto(proto.new_interrupt_conditions)
            if proto.HasField("new_interrupt_conditions")
            else None
        )

        return cls(
            proto.run_id,
            proto.interrupt_id,
            proto.event_id,
            ActionBundle.from_proto(proto.actions),
            conditions,
        )

    def to_proto(self) -> engine_pb2.AcknowledgeInterruptRequest:
        return engine_pb2.AcknowledgeInterruptRequest(
            run_id=self.run_id,
            interrupt_id=self.interrupt_id,
            event_id=self.event_id,
            actions=self.actions.to_proto(),
            new_interrupt_conditions=(
                self.new_interrupt_conditions.to_proto()
                if self.new_interrupt_conditions is not None
                else None
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "interrupt_id": self.interrupt_id,
            "event_id": self.event_id,
            "actions": self.actions.to_dict(),
            "new_interrupt_conditions": (
                self.new_interrupt_conditions.to_dict()
                if self.new_interrupt_conditions is not None
                else None
            ),
        }


@dataclass(frozen=True)
class CancelInterruptRequest(SdkBase[engine_pb2.CancelInterruptRequest]):
    run_id: str
    interrupt_id: str

    @classmethod
    def from_proto(cls, proto: engine_pb2.CancelInterruptRequest) -> Self:
        return cls(proto.run_id, proto.interrupt_id)

    def to_proto(self) -> engine_pb2.CancelInterruptRequest:
        return engine_pb2.CancelInterruptRequest(
            run_id=self.run_id, interrupt_id=self.interrupt_id
        )

    def to_dict(self) -> dict[str, object]:
        return {"run_id": self.run_id, "interrupt_id": self.interrupt_id}


@dataclass(frozen=True)
class CancelInterruptResponse(SdkBase[engine_pb2.CancelInterruptResponse]):
    interrupt_id: str

    @classmethod
    def from_proto(cls, proto: engine_pb2.CancelInterruptResponse) -> Self:
        return cls(proto.interrupt_id)

    def to_proto(self) -> engine_pb2.CancelInterruptResponse:
        return engine_pb2.CancelInterruptResponse(
            interrupt_id=self.interrupt_id
        )

    def to_dict(self) -> dict[str, object]:
        return {"interrupt_id": self.interrupt_id}


@dataclass(frozen=True)
class FetchRequest(SdkBase[engine_pb2.FetchRequest]):
    run_id: str
    fingerprint: str
    requires_collect: bool

    @classmethod
    def from_proto(cls, proto: engine_pb2.FetchRequest) -> Self:
        return cls(proto.run_id, proto.fingerprint, proto.requires_collect)

    def to_proto(self) -> engine_pb2.FetchRequest:
        return engine_pb2.FetchRequest(
            run_id=self.run_id,
            fingerprint=self.fingerprint,
            requires_collect=self.requires_collect,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "fingerprint": self.fingerprint,
            "requires_collect": self.requires_collect,
        }


@dataclass(frozen=True)
class FetchResponse(SdkBase[engine_pb2.FetchResponse]):
    fetched: KeyValue

    @classmethod
    def from_proto(cls, proto: engine_pb2.FetchResponse) -> Self:
        return cls(KeyValue.from_proto(proto.fetched))

    def to_proto(self) -> engine_pb2.FetchResponse:
        return engine_pb2.FetchResponse(fetched=self.fetched.to_proto())

    def to_dict(self) -> dict[str, object]:
        return {"fetched": self.fetched.to_dict()}


@dataclass(frozen=True)
class StreamRequest(SdkBase[engine_pb2.StreamRequest]):
    run_id: str

    @classmethod
    def from_proto(cls, proto: engine_pb2.StreamRequest) -> Self:
        return cls(proto.run_id)

    def to_proto(self) -> engine_pb2.StreamRequest:
        return engine_pb2.StreamRequest(run_id=self.run_id)

    def to_dict(self) -> dict[str, object]:
        return {"run_id": self.run_id}
