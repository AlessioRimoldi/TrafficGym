from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class KeyValue(_message.Message):
    __slots__ = ("key", "double_value", "string_value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    double_value: float
    string_value: str
    def __init__(self, key: _Optional[str] = ..., double_value: _Optional[float] = ..., string_value: _Optional[str] = ...) -> None: ...

class TelemetryFrame(_message.Message):
    __slots__ = ("run_id", "step", "sim_time_s", "metrics")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    SIM_TIME_S_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    step: int
    sim_time_s: float
    metrics: _containers.RepeatedCompositeFieldContainer[KeyValue]
    def __init__(self, run_id: _Optional[str] = ..., step: _Optional[int] = ..., sim_time_s: _Optional[float] = ..., metrics: _Optional[_Iterable[_Union[KeyValue, _Mapping]]] = ...) -> None: ...

class TlsSetPhase(_message.Message):
    __slots__ = ("tls_id", "phase_index")
    TLS_ID_FIELD_NUMBER: _ClassVar[int]
    PHASE_INDEX_FIELD_NUMBER: _ClassVar[int]
    tls_id: str
    phase_index: int
    def __init__(self, tls_id: _Optional[str] = ..., phase_index: _Optional[int] = ...) -> None: ...

class GenericSetter(_message.Message):
    __slots__ = ("domain", "setter_name", "object_id", "value", "additional_parameters")
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    SETTER_NAME_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    domain: str
    setter_name: str
    object_id: str
    value: str
    additional_parameters: _containers.RepeatedCompositeFieldContainer[Parameter]
    def __init__(self, domain: _Optional[str] = ..., setter_name: _Optional[str] = ..., object_id: _Optional[str] = ..., value: _Optional[str] = ..., additional_parameters: _Optional[_Iterable[_Union[Parameter, _Mapping]]] = ...) -> None: ...

class Action(_message.Message):
    __slots__ = ("setter",)
    SETTER_FIELD_NUMBER: _ClassVar[int]
    setter: GenericSetter
    def __init__(self, setter: _Optional[_Union[GenericSetter, _Mapping]] = ...) -> None: ...

class ActionBundle(_message.Message):
    __slots__ = ("run_id", "step", "actions")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    step: int
    actions: _containers.RepeatedCompositeFieldContainer[Action]
    def __init__(self, run_id: _Optional[str] = ..., step: _Optional[int] = ..., actions: _Optional[_Iterable[_Union[Action, _Mapping]]] = ...) -> None: ...

class Artifact(_message.Message):
    __slots__ = ("artifact_id", "type", "uri", "sha256")
    ARTIFACT_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    URI_FIELD_NUMBER: _ClassVar[int]
    SHA256_FIELD_NUMBER: _ClassVar[int]
    artifact_id: str
    type: str
    uri: str
    sha256: str
    def __init__(self, artifact_id: _Optional[str] = ..., type: _Optional[str] = ..., uri: _Optional[str] = ..., sha256: _Optional[str] = ...) -> None: ...

class CreateRunRequest(_message.Message):
    __slots__ = ("sumocfg_path", "sumo_binary", "step_length_ms")
    SUMOCFG_PATH_FIELD_NUMBER: _ClassVar[int]
    SUMO_BINARY_FIELD_NUMBER: _ClassVar[int]
    STEP_LENGTH_MS_FIELD_NUMBER: _ClassVar[int]
    sumocfg_path: str
    sumo_binary: str
    step_length_ms: int
    def __init__(self, sumocfg_path: _Optional[str] = ..., sumo_binary: _Optional[str] = ..., step_length_ms: _Optional[int] = ...) -> None: ...

class CreateRunResponse(_message.Message):
    __slots__ = ("run_id", "input_artifacts")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_ARTIFACTS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    input_artifacts: _containers.RepeatedCompositeFieldContainer[Artifact]
    def __init__(self, run_id: _Optional[str] = ..., input_artifacts: _Optional[_Iterable[_Union[Artifact, _Mapping]]] = ...) -> None: ...

class RunRequest(_message.Message):
    __slots__ = ("run_id", "max_steps", "max_time")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    MAX_STEPS_FIELD_NUMBER: _ClassVar[int]
    MAX_TIME_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    max_steps: int
    max_time: float
    def __init__(self, run_id: _Optional[str] = ..., max_steps: _Optional[int] = ..., max_time: _Optional[float] = ...) -> None: ...

class RunResponse(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class CloseRunRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class CloseRunResponse(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class ApplyActionsResponse(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class StreamRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class Parameter(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: str
    def __init__(self, name: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class SubscribeRequest(_message.Message):
    __slots__ = ("run_id", "domain", "getter_name", "object_id", "additional_parameters", "name")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    GETTER_NAME_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    domain: str
    getter_name: str
    object_id: str
    additional_parameters: _containers.RepeatedCompositeFieldContainer[Parameter]
    name: str
    def __init__(self, run_id: _Optional[str] = ..., domain: _Optional[str] = ..., getter_name: _Optional[str] = ..., object_id: _Optional[str] = ..., additional_parameters: _Optional[_Iterable[_Union[Parameter, _Mapping]]] = ..., name: _Optional[str] = ...) -> None: ...

class SubscribeResponse(_message.Message):
    __slots__ = ("subscription_name_or_fingerprint",)
    SUBSCRIPTION_NAME_OR_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    subscription_name_or_fingerprint: str
    def __init__(self, subscription_name_or_fingerprint: _Optional[str] = ...) -> None: ...

class UnsubscribeRequest(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...

class UnsubscribeResponse(_message.Message):
    __slots__ = ("run_id",)
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    def __init__(self, run_id: _Optional[str] = ...) -> None: ...
