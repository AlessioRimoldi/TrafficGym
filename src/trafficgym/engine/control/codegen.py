from __future__ import annotations
from typing import Any, TypedDict
import re


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


class _BlockSpec(TypedDict):
    id: str
    key: str
    params: dict[str, Any]
    input_from: str | None
    actuate_to: str | None


class _SinkSpec(TypedDict):
    id: str
    label: str
    input_from: str | None


class _PipelineSpec(TypedDict):
    id: str
    name: str
    observers: list[_ObserverSpec]
    blocks: list[_BlockSpec]
    actuators: list[_ActuatorSpec]
    sinks: list[_SinkSpec]


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


_BLOCK_INPUT_KEY: dict[str, str] = {
    "RampMeterController": "occupancy",
    "StaticTLSController": "sim_time",
}

_BLOCK_OUTPUT_KEY: dict[str, str] = {
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

_AGGREGATOR_KEYS = {"RollingAverage", "ExponentialMovingAverage"}

_I = "    "  # 4-space indent unit


def class_name_from(graph_name: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]", "_", graph_name).strip("_")
    return slug or "GeneratedExperiment"


def generate(graph: _GraphSpec, name: str) -> str:
    pipelines: dict[str, _PipelineSpec] = {p["id"]: p for p in graph["pipelines"]}
    phases: list[_PhaseSpec] = graph["phases"]

    needed_keys: set[str] = set()
    for p in pipelines.values():
        for b in p.get("blocks", []):
            needed_keys.add(b["key"])

    needed_aggs = needed_keys & _AGGREGATOR_KEYS
    needed_ctrls = needed_keys - _AGGREGATOR_KEYS

    needs_exit_stack = any(
        sum(
            sum(1 for b in pipelines[pid].get("blocks", []) if b.get("actuate_to"))
            for pid in ph["active_pipeline_ids"] if pid in pipelines
        ) > 1
        for ph in phases
    )

    lines: list[str] = []
    lines.append("from typing import Any")
    lines.append("from trafficgym.experiment_sdk.experiments.base import Experiment")
    lines.append("from trafficgym.engine.ports.simulation import SimulationPort")
    if needed_ctrls:
        lines.append(
            "from trafficgym.engine.control.controllers import "
            + ", ".join(sorted(needed_ctrls))
        )
    if needed_aggs:
        lines.append(
            "from trafficgym.engine.control.aggregators import "
            + ", ".join(sorted(needed_aggs))
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

    # Instantiate all blocks at run() scope so state persists across phases
    blk_var: dict[tuple[str, str], str] = {}
    blk_counter = 0
    for _pl in pipelines.values():
        for blk in _pl.get("blocks", []):
            var = f"_blk_{blk_counter}"
            blk_counter += 1
            blk_var[(_pl["id"], blk["id"])] = var
            if blk["key"] == "StaticTLSController":
                rows = blk.get("params", {}).get("phase_rows") or []
                phase_strs = [r["state"] for r in rows]
                durations = [r["duration"] for r in rows]
                out.append(f"{var} = StaticTLSController({phase_strs!r}, {durations!r})")
            else:
                kwargs = ", ".join(f"{k}={v}" for k, v in blk.get("params", {}).items())
                out.append(f"{var} = {blk['key']}({kwargs})")
    if blk_var:
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

        controlled: list[tuple[str, str, str]] = []

        for pid in active_pids:
            pipe = pipelines.get(pid)
            if pipe is None:
                continue
            obs_by_id: dict[str, _ObserverSpec] = {o["id"]: o for o in pipe["observers"]}
            blk_by_id: dict[str, _BlockSpec] = {b["id"]: b for b in pipe.get("blocks", [])}
            act_by_id: dict[str, _ActuatorSpec] = {a["id"]: a for a in pipe["actuators"]}
            sink_by_input_id: dict[str, str] = {
                s["input_from"]: (s["label"] or s["id"])
                for s in pipe.get("sinks", [])
                if s["input_from"]
            }

            for blk in pipe.get("blocks", []):
                if not blk.get("actuate_to"):
                    continue

                blk_key = blk["key"]
                instance_var = blk_var[(pid, blk["id"])]
                ctrl_var = f"_ctrl_{ctrl_counter}"
                ctrl_counter += 1

                # Observe function
                obs_fn = f"_obs_{ctrl_var}"
                out.append(f"def {obs_fn}(a: SimulationPort, t: float) -> dict[str, Any]:")
                if blk_key == "StaticTLSController":
                    input_key = _BLOCK_INPUT_KEY.get(blk_key, "value")
                    out.append(f'{_I}return {{"{input_key}": t}}')
                else:
                    source_id = blk.get("input_from") or ""
                    if source_id:
                        input_key = _BLOCK_INPUT_KEY.get(blk_key, "value")
                        stmts, final_var = _build_obs_stmts(
                            source_id, obs_by_id, blk_by_id, blk_var, pid,
                            sink_by_input_id, input_key,
                        )
                        for stmt in stmts:
                            out.append(f"{_I}{stmt}")
                        out.append(f"{_I}return {final_var}")
                    else:
                        out.append(f"{_I}return {{}}")

                # Actuate function
                output_key = _BLOCK_OUTPUT_KEY.get(blk_key, "value")
                act: _ActuatorSpec | None = act_by_id.get(blk.get("actuate_to") or "")
                act_fn = f"_act_{ctrl_var}"
                out.append(f"def {act_fn}(a: SimulationPort, r: dict[str, Any]) -> None:")
                out.append(f'{_I}if "{output_key}" not in r:')
                out.append(f"{_I * 2}return")
                if act:
                    param_key = _SETTER_PARAM_KEY.get(act["setter"], "value")
                    out.append(
                        f'{_I}a.apply("{act["domain"]}", "{act["setter"]}", '
                        f'"{act["object_id"]}", {{"{param_key}": r["{output_key}"]}})'
                    )

                controlled.append((instance_var, obs_fn, act_fn))

        if len(controlled) == 1:
            var, observe, actuate = controlled[0]
            out.append(f"with adapter.controlled(")
            out.append(f"{_I}{var},")
            out.append(f"{_I}observe={observe},")
            out.append(f"{_I}actuate={actuate},")
            out.append(f"):")
            out.append(f"{_I}{run_stmt}")
        elif controlled:
            out.append("with contextlib.ExitStack() as _stack:")
            for var, observe, actuate in controlled:
                out.append(f"{_I}_stack.enter_context(adapter.controlled(")
                out.append(f"{_I * 2}{var},")
                out.append(f"{_I * 2}observe={observe},")
                out.append(f"{_I * 2}actuate={actuate},")
                out.append(f"{_I}))")
            out.append(f"{_I}{run_stmt}")
        else:
            out.append(run_stmt)

    return out


def _build_obs_stmts(
    source_id: str,
    obs_by_id: dict[str, _ObserverSpec],
    blk_by_id: dict[str, _BlockSpec],
    blk_var: dict[tuple[str, str], str],
    pid: str,
    sink_by_input_id: dict[str, str],
    obs_wrap_key: str,
) -> tuple[list[str], str]:
    """Walk the input chain, emit sequential statements, return (stmts, final_var_name)."""
    stmts: list[str] = []
    counter = [0]

    def walk(sid: str) -> str:
        if not sid:
            return "{}"
        if sid in obs_by_id:
            _o = obs_by_id[sid]
            _c = _GETTER_CAST.get(_o["getter"], "str")
            raw = (
                f'{_c}(a.query("{_o["domain"]}", "{_o["getter"]}", '
                f'"{_o["object_id"]}", {{}}))'
            )
            var = f"_r{counter[0]}"
            counter[0] += 1
            stmts.append(f'{var} = {{"{obs_wrap_key}": {raw}}}')
            if sid in sink_by_input_id:
                stmts.append(f'self._record("{sink_by_input_id[sid]}", {var}["{obs_wrap_key}"])')
            return var
        if sid in blk_by_id:
            _b = blk_by_id[sid]
            _bv = blk_var.get((pid, _b["id"]), "_missing_blk")
            prev_var = walk(_b.get("input_from") or "")
            var = f"_r{counter[0]}"
            counter[0] += 1
            stmts.append(f"{var} = {_bv}.step(a, {prev_var})")
            if sid in sink_by_input_id:
                # record the first value in the output dict
                stmts.append(f'self._record("{sink_by_input_id[sid]}", next(iter({var}.values()), None))')
            return var
        return "{}"

    final_var = walk(source_id)
    return stmts, final_var
