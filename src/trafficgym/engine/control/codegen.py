from __future__ import annotations
from typing import TypedDict
import re


class _StaticPhaseRow(TypedDict):
    state: str
    duration: int | float


class _ObserverSpec(TypedDict):
    id: str
    domain: str
    getter: str
    object_id: str


class _ActuatorSpec(TypedDict):
    id: str
    domain: str
    setter: str
    object_id: str


class _ControllerSpec(TypedDict):
    id: str
    key: str
    params: dict[str, int | float]
    static_phase_rows: list[_StaticPhaseRow] | None
    observe_from: str | None
    actuate_to: str | None


class _PipelineSpec(TypedDict):
    id: str
    name: str
    observers: list[_ObserverSpec]
    controllers: list[_ControllerSpec]
    actuators: list[_ActuatorSpec]


class _OnEnterAction(TypedDict):
    domain: str
    setter: str
    object_id: str
    params: dict[str, object]


class _PhaseSpec(TypedDict):
    duration_s: float | None
    active_pipeline_ids: list[str]
    on_enter: list[_OnEnterAction]


class _GraphSpec(TypedDict):
    pipelines: list[_PipelineSpec]
    phases: list[_PhaseSpec]

_CONTROLLER_INPUT_KEY: dict[str, str] = {
    "RampMeterController": "occupancy",
    "StaticTLSController": "sim_time",
}

_CONTROLLER_OUTPUT_KEY: dict[str, str] = {
    "RampMeterController": "program_id",
    "StaticTLSController": "state",
}

_SETTER_PARAM_KEY: dict[str, str] = {
    "setProgram":             "programID",
    "setRedYellowGreenState": "state",
    "setPhase":               "index",
}

_GETTER_CAST: dict[str, str] = {
    "getLastIntervalOccupancy": "float",
    "getLastStepOccupancy":     "float",
    "getLastStepVehicleNumber": "int",
    "getLastStepMeanSpeed":     "float",
    "getSpentDuration":         "float",
    "getPhase":                 "int",
}

_I = "    "  # 4-space indent unit


def class_name_from(graph_name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]", "_", graph_name).strip("_")
    return slug or "GeneratedExperiment"


def generate(graph: _GraphSpec, name: str) -> str:
    pipelines: dict[str, _PipelineSpec] = {p["id"]: p for p in graph["pipelines"]}
    phases: list[_PhaseSpec] = graph["phases"]

    needed_controllers: set[str] = set()
    for p in pipelines.values():
        for c in p["controllers"]:
            needed_controllers.add(c["key"])

    needs_exit_stack = any(
        sum(len(pipelines[pid]["controllers"]) for pid in ph["active_pipeline_ids"] if pid in pipelines) > 1
        for ph in phases
    )

    lines: list[str] = []
    lines.append("from trafficgym.experiment_sdk.experiments.base import Experiment")
    lines.append("from trafficgym.engine.ports.simulation import SimulationPort")
    if needed_controllers:
        lines.append(
            "from trafficgym.engine.control.controllers import "
            + ", ".join(sorted(needed_controllers))
        )
    if needs_exit_stack:
        lines.append("import contextlib")
    lines.append("")
    lines.append("")
    cls = class_name_from(name)
    lines.append(f"class {cls}(Experiment):")
    lines.append(f"{_I}def run(self, adapter: SimulationPort) -> None:")

    body: list[str] = _generate_body(pipelines, phases)

    for line in body:
        lines.append((_I * 2 + line) if line else "")

    return "\n".join(lines) + "\n"


def _generate_body(pipelines: dict[str, _PipelineSpec], phases: list[_PhaseSpec]) -> list[str]:
    out: list[str] = []
    ctrl_counter = 0

    seen: set[tuple[str, str, str]] = set()
    for _pl in pipelines.values():
        for _o in _pl["observers"]:
            obs_key = (_o["domain"], _o["getter"], _o["object_id"])
            if obs_key not in seen:
                seen.add(obs_key)
                fp = f"{_o['domain']}.{_o['getter']}.{_o['object_id']}"
                out.append(f'self.subscribe("{fp}", "{_o["domain"]}", "{_o["getter"]}", "{_o["object_id"]}")')
    if seen:
        out.append("")

    for i, phase in enumerate(phases):
        if i > 0:
            out.append("")
        out.append(f"# Phase {i + 1}")

        for action in phase.get("on_enter", []):
            params = action["params"]
            out.append(
                f'adapter.apply("{action["domain"]}", "{action["setter"]}", '
                f'"{action["object_id"]}", {params!r})'
            )

        active_pids: list[str] = phase.get("active_pipeline_ids", [])
        duration = phase.get("duration_s")
        run_stmt = f"adapter.run_time({duration})" if duration is not None else "adapter.run_until_empty()"

        if not active_pids:
            out.append(run_stmt)
            continue

        controlled: list[tuple[str, str, str]] = []  # (var_name, observe_expr, actuate_expr)

        for pid in active_pids:
            pipe = pipelines.get(pid)
            if pipe is None:
                continue
            obs_by_id: dict[str, _ObserverSpec] = {o["id"]: o for o in pipe["observers"]}
            act_by_id: dict[str, _ActuatorSpec] = {a["id"]: a for a in pipe["actuators"]}

            for ctrl in pipe["controllers"]:
                ctrl_key: str = ctrl["key"]
                var = f"_ctrl_{ctrl_counter}"
                ctrl_counter += 1

                # Constructor
                if ctrl_key == "StaticTLSController":
                    rows = ctrl.get("static_phase_rows") or []
                    phase_strs = [r["state"] for r in rows]
                    durations = [r["duration"] for r in rows]
                    constructor = f"StaticTLSController({phase_strs!r}, {durations!r})"
                else:
                    kwargs = ", ".join(f"{k}={v}" for k, v in ctrl.get("params", {}).items())
                    constructor = f"{ctrl_key}({kwargs})"
                out.append(f"{var} = {constructor}")

                # Observe
                input_key = _CONTROLLER_INPUT_KEY.get(ctrl_key, "value")
                if input_key == "sim_time":
                    observe = 'lambda _, t: {"sim_time": t}'
                else:
                    obs_spec: _ObserverSpec | None = obs_by_id.get(ctrl.get("observe_from") or "")
                    if obs_spec:
                        cast = _GETTER_CAST.get(obs_spec["getter"], "str")
                        observe = (
                            f'lambda a, _: {{"{input_key}": {cast}(a.query('
                            f'"{obs_spec["domain"]}", "{obs_spec["getter"]}", "{obs_spec["object_id"]}", {{}}))}}'
                        )
                    else:
                        observe = f'lambda _, t: {{"{input_key}": t}}'

                # Actuate
                output_key = _CONTROLLER_OUTPUT_KEY.get(ctrl_key, "value")
                act: _ActuatorSpec | None = act_by_id.get(ctrl.get("actuate_to") or "")
                if act:
                    param_key = _SETTER_PARAM_KEY.get(act["setter"], "value")
                    actuate = (
                        f'lambda a, r: a.apply("{act["domain"]}", "{act["setter"]}", '
                        f'"{act["object_id"]}", {{"{param_key}": r["{output_key}"]}}) '
                        f'if "{output_key}" in r else None'
                    )
                else:
                    actuate = "lambda a, r: None"

                controlled.append((var, observe, actuate))

        if len(controlled) == 1:
            var, observe, actuate = controlled[0]
            out.append(f"with adapter.controlled(")
            out.append(f"{_I}{var},")
            out.append(f"{_I}observe={observe},")
            out.append(f"{_I}actuate={actuate},")
            out.append(f"):")
            out.append(f"{_I}{run_stmt}")
        else:
            out.append("with contextlib.ExitStack() as _stack:")
            for var, observe, actuate in controlled:
                out.append(f"{_I}_stack.enter_context(adapter.controlled(")
                out.append(f"{_I * 2}{var},")
                out.append(f"{_I * 2}observe={observe},")
                out.append(f"{_I * 2}actuate={actuate},")
                out.append(f"{_I}))")
            out.append(f"{_I}{run_stmt}")

    return out
