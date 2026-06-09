from __future__ import annotations
import inspect
import textwrap
import typing
from dataclasses import dataclass, field
from typing import Any, TypedDict


@dataclass
class BlockParam:
    name: str
    type: str   # "number" | "string" | "phase_list" | "select" | "list[int]"
    label: str
    default: Any = None
    choices: list[str] | None = None  # required when type == "select"


class PortDef(TypedDict):
    key: str
    type: str


@dataclass
class BlockDef:
    key: str
    label: str
    module: str
    input_ports: list[PortDef] = field(default_factory=list)
    output_ports: list[PortDef] = field(default_factory=list)
    description: str | None = None
    params: list[BlockParam] = field(default_factory=list)


BLOCK_REGISTRY: dict[str, BlockDef] = {}


def _annotation_to_param_type(annotation: Any) -> str:
    if annotation == "list[int]":
        return "array[int]"
    if annotation is str:
        return "string"
    return "number"


def _label_from_name(name: str) -> str:
    return name.replace("_", " ").title()


def _type_to_str(t: Any) -> str | None:
    """Convert a Python type annotation to a compact display string."""
    if t is None:
        return None
    if t is float:
        return "float"
    if t is int:
        return "int"
    if t is str:
        return "str"
    if t is bool:
        return "bool"
    origin = getattr(t, "__origin__", None)
    if origin is list:
        args = getattr(t, "__args__", ())
        inner = (_type_to_str(args[0]) or "Any") if args else "Any"
        return f"list[{inner}]"
    if origin is tuple:
        args = getattr(t, "__args__", ())
        if args:
            return "tuple[" + ", ".join(_type_to_str(a) or "Any" for a in args) + "]"
        return "tuple"
    name = getattr(t, "__name__", None)
    return str(name) if name else str(t)


def _first_typed_dict_kv(td: Any) -> tuple[str | None, str | None]:
    """Return the first non-private field name and its type string from a TypedDict."""
    try:
        # get_type_hints resolves ForwardRefs that arise from `from __future__ import annotations`.
        ann: dict[str, Any] = typing.get_type_hints(td)
    except Exception:
        ann = getattr(td, "__annotations__", {}) or {}
    if not ann:
        return None, None
    items = [(k, v) for k, v in ann.items() if not k.startswith("_")]
    if not items:
        return None, None
    key, typ = items[0]
    return key, _type_to_str(typ)


def _all_typed_dict_kv(td: Any) -> list[PortDef]:
    """Return all non-private fields from a TypedDict as PortDef list."""
    try:
        ann: dict[str, Any] = typing.get_type_hints(td)
    except Exception:
        ann = getattr(td, "__annotations__", {}) or {}
    return [{"key": k, "type": _type_to_str(v) or "Any"} for k, v in ann.items() if not k.startswith("_")]


def _step_io(cls: type[Any]) -> tuple[list[PortDef], list[PortDef]]:
    """Introspect step() → (input_ports, output_ports)."""
    try:
        hints = typing.get_type_hints(cls.step)
    except Exception:
        return [], []
    return _all_typed_dict_kv(hints.get("inputs")), _all_typed_dict_kv(hints.get("return"))


def _init_params(cls: type[Any]) -> list[BlockParam]:
    """Introspect __init__ → BlockParam list (only params that have defaults)."""
    try:
        init = cls.__init__
        sig = inspect.signature(init)
        hints = typing.get_type_hints(init)
    except Exception:
        return []
    params: list[BlockParam] = []
    for name, param in sig.parameters.items():
        if name == "self" or param.default is inspect.Parameter.empty:
            continue
        params.append(BlockParam(
            name=name,
            type=_type_to_str(hints.get(name)) or "None",
            label=_label_from_name(name),
            default=param.default,
        ))
    return params


def _clean_docstring(cls: type) -> str | None:
    doc = cls.__doc__
    if not doc:
        return None
    # textwrap.dedent + strip collapses indentation from class-body docstrings
    return textwrap.dedent(doc).strip() or None


def block(
    label: str,
    extra_params: list[BlockParam] | None = None,
) -> Any:
    """Decorator that registers a block class in BLOCK_REGISTRY.

    * label       — human-readable name shown in the graph builder UI.
    * input_key   — read from the first field of the step() inputs TypedDict.
    * output_key  — read from the first field of the step() return TypedDict.
    * params      — read from __init__ parameters that have default values.
    * description — read from the class docstring.
    * module      — inferred from cls.__module__ ("controllers" / "aggregators").

    Use extra_params to add or override specific params, e.g. a phase_list
    UI param that has no matching __init__ argument.
    """
    def decorator(cls: type[Any]) -> type[Any]:
        input_ports, output_ports = _step_io(cls)
        params = _init_params(cls)

        if extra_params:
            by_name = {p.name: p for p in params}
            for ep in extra_params:
                by_name[ep.name] = ep
            params = list(by_name.values())

        BLOCK_REGISTRY[cls.__name__] = BlockDef(
            key=cls.__name__,
            label=label,
            module=cls.__module__.split(".")[-1],
            input_ports=input_ports,
            output_ports=output_ports,
            description=_clean_docstring(cls),
            params=params,
        )
        return cls
    return decorator
