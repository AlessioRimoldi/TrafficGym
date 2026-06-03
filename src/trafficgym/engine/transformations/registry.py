import re
from dataclasses import dataclass
from typing import Coroutine, Any, Callable, List, TypedDict
from pathlib import Path
from enum import Enum


class InputType(str, Enum):
    FILE = "FILE"
    JSON = "JSON"


@dataclass
class InputSpec:
    name: str
    type: InputType
    required: bool = True
    multiple: bool = False


@dataclass
class OutputSpec:
    name: str
    # multiple: bool = False


@dataclass
class TransformationSpec:
    key: str
    inputs: List[InputSpec]
    outputs: List[OutputSpec]
    handler: Callable[[dict[str, str], Runtime], Coroutine[Any, Any, dict[str, str]]]
    docstring: str


@dataclass
class Runtime:
    base_path: Path


REGISTRY: dict[str, TransformationSpec] = {}


def register(spec: TransformationSpec) -> None:
    if spec.key in REGISTRY:
        raise KeyError("Key already exists in registry")
    REGISTRY[spec.key] = spec


def _matches_multiple(key: str, prefix: str) -> bool:
    return key == prefix or bool(re.fullmatch(rf"{re.escape(prefix)}_\d+", key))


def resolve_inputs(
    spec: TransformationSpec,
    parameters: dict[str, str],
    input_name_to_path: dict[str, str],
    runtime: Runtime,
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    base_path = runtime.base_path

    single_keys = {i.name for i in spec.inputs if not i.multiple}
    multiple_specs = [i for i in spec.inputs if i.multiple]

    for key in input_name_to_path:
        if key in single_keys:
            continue
        if any(_matches_multiple(key, ms.name) for ms in multiple_specs):
            continue
        raise ValueError(f"Unexpected input: {key}")

    for input_spec in spec.inputs:
        key = input_spec.name

        if input_spec.type == InputType.FILE:
            if input_spec.multiple:
                matched = {k: v for k, v in input_name_to_path.items() if _matches_multiple(k, key)}
                if not matched and input_spec.required:
                    raise ValueError(f"Missing required input: {key}")
                for k, path in matched.items():
                    resolved[k] = str(base_path / path)
            else:
                if key not in input_name_to_path:
                    if input_spec.required:
                        raise ValueError(f"Missing required input: {key}")
                    continue
                resolved[key] = str(base_path / input_name_to_path[key])

        elif input_spec.type == InputType.JSON:
            if key in parameters:
                resolved[key] = parameters[key]
            elif input_spec.required:
                raise ValueError(f"Missing required parameter: {key}")

        else:
            raise ValueError(f"Unknown Input Spec Tye: {input_spec.type}")

    return resolved

class ResponseType(TypedDict):
    errors: list[str]
    warnings: list[str]
    derived_artefact_paths: dict[str, str]

def build_response(
    spec: TransformationSpec, result: dict[str, str]
) -> ResponseType:
    output: dict[str, str] = {}

    for output_spec in spec.outputs:
        key = output_spec.name

        if key not in result:
            raise ValueError(f"Missing expected output: {key}")

        output[key] = result[key]

    return {
        "errors": [], "warnings": [], "derived_artefact_paths": output
    }

def get_spec_or_none(key: str) -> TransformationSpec | None:
    return REGISTRY.get(key)