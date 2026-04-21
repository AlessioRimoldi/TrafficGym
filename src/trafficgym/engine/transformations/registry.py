from dataclasses import dataclass
from typing import Awaitable, Callable, List, cast
from pathlib import Path
from trafficgym.api.engine_pb2 import (
    DeriveFromArtefactRequest,
    DeriveFromArtefactResponse,
)
from enum import Enum


class InputType(str, Enum):
    FILE = "file"
    JSON = "json"


@dataclass
class InputSpec:
    name: str
    type: InputType
    required: bool = True
    # multiple: bool = False


@dataclass
class OutputSpec:
    name: str
    # multiple: bool = False


@dataclass
class TransformationSpec:
    key: str
    inputs: List[InputSpec]
    outputs: List[OutputSpec]
    handler: Callable[[dict[str, str], Runtime], Awaitable[dict[str, str]]]
    docstring: str


@dataclass
class Runtime:
    base_path: Path


REGISTRY: dict[str, TransformationSpec] = {}


def register(spec: TransformationSpec) -> None:
    if spec.key in REGISTRY:
        raise KeyError("Key already exists in registry")
    REGISTRY[spec.key] = spec


def resolve_inputs(
    spec: TransformationSpec,
    request: DeriveFromArtefactRequest,
    runtime: Runtime,
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    base_path = runtime.base_path

    allowed_keys = {i.name for i in spec.inputs}

    for key in request.target_file_paths:
        if key not in allowed_keys:
            raise ValueError(f"Unexpected input: {key}")

    for input_spec in spec.inputs:
        key = input_spec.name

        if input_spec.type == InputType.FILE:
            if key not in request.target_file_paths:
                if input_spec.required:
                    raise ValueError(f"Missing required input: {key}")
                continue

            resolved[key] = str(base_path / request.target_file_paths[key])

        elif input_spec.type == InputType.JSON:
            if key in request.parameters:
                resolved[key] = request.parameters[key]
            elif input_spec.required:
                raise ValueError(f"Missing required parameter: {key}")

        else:
            raise ValueError(f"Unknown Input Spec Tye: {input_spec.type}")

    return resolved


def build_response(
    spec: TransformationSpec, result: dict[str, str]
) -> DeriveFromArtefactResponse:
    output = {}

    for output_spec in spec.outputs:
        key = output_spec.name

        if key not in result:
            raise ValueError(f"Missing expected output: {key}")

        output[key] = result[key]

    return DeriveFromArtefactResponse(
        errors=[], warnings=[], derived_artefact_paths=output
    )
