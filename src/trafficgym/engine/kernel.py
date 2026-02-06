from __future__ import annotations
from dataclasses import dataclass
import sys, os
from typing import Dict, Tuple, Any
from google.protobuf.struct_pb2 import Value
from ..api import engine_pb2
import asyncio
import uuid
import logging

if "SUMO_HOME" in os.environ:
    tools = os.path.join(os.environ["SUMO_HOME"], "tools")
    sys.path.append(tools)
else:
    logging.critical("please declare environment variable 'SUMO_HOME'")
    sys.exit(1)

import libsumo  # type: ignore[import-untyped]

# _original_setPhase = libsumo.trafficlight.setRedYellowGreenState
# def logging_setPhase(tls_id: str, phase: str) -> Any:
#     print(
#         f"[setPhase] tls={tls_id}, phase={phase}, simTime={libsumo.simulation.getTime()}"
#     )
#     return _original_setPhase(tls_id, phase)
# libsumo.trafficlight.setRedYellowGreenState = logging_setPhase

# Domain = Enum("Domain", list(map(lambda x: x.__name__, libsumo.DOMAINS)))


@dataclass
class InterruptEvent:
    event_id = str(uuid.uuid4())
    observed_value: Value


class Interrupt:
    def __init__(
        self,
        trigger_metric_name: str,
        trigger_metric_value: Value,
        trigger_metric_op: engine_pb2.Operation.ValueType,
        interrupt_requests: asyncio.Queue[InterruptEvent | None],
        active_interrupt_event: InterruptEvent | None = None,
    ):
        self.trigger_metric_name = trigger_metric_name
        self.trigger_metric_value = trigger_metric_value
        self.trigger_metric_op = trigger_metric_op
        self.interrupt_requests = interrupt_requests
        self.active_interrupt_event = active_interrupt_event
        self.interrupt_id = str(uuid.uuid4())


@dataclass
class RunConfig:
    sumocfg_path: str
    sumo_binary: str
    step_length_ms: int


class RunState:
    def __init__(self, cfg: RunConfig):
        self.cfg = cfg
        self.run_id = str(uuid.uuid4())
        self.started: bool = False
        self.step: int = 0
        self.edge_ids: list[str] = []
        self.last_metrics: dict[str, float] = {}
        self.max_steps: int | None = None
        self.interrupts: dict[str, Interrupt] = {}

    # def start(self, max_steps: int) -> None:
    def start(self) -> None:
        if self.started:
            return
        cmd = [
            self.cfg.sumo_binary,
            "-c",
            self.cfg.sumocfg_path,
            "--step-length",
            str(self.cfg.step_length_ms / 1000.0),
            "--no-warnings",
        ]
        libsumo.start(cmd)
        self.edge_ids = list(libsumo.edge.getIDList())
        self.started = True
        # self.max_steps = max_steps # will fix later
        self.step = 0

    def close(self) -> None:
        if self.started:
            try:
                libsumo.close()
            finally:
                self.started = False

    # def apply_tls_set_phase(self, tls_id: str, phase_index: int) -> None:
    # libsumo.trafficlight.setPhase(tls_id, int(phase_index))

    def invoke_setter(
        self,
        # domain: Domain,
        domain: str,
        setter_name: str,
        object_id: str,
        value: Value,
        additional_parameters: dict[str, Any],
    ) -> None:
        domain_handle = getattr(libsumo, domain)
        setter_handle = getattr(domain_handle, setter_name)

        kind = value.WhichOneof("kind")

        if object_id == "":
            raise RuntimeError("ObjectID required for setter")

        if kind == "number_value":
            try:
                setter_handle(
                    object_id, value.number_value, **additional_parameters
                )
            except TypeError:
                setter_handle(
                    object_id, int(value.number_value), **additional_parameters
                )
        elif kind == "string_value":
            setter_handle(
                object_id, value.string_value, **additional_parameters
            )

        logging.debug(
            f"Invoked setter: {domain}.{setter_name}_"
            f"{object_id}, {additional_parameters}"
        )

    def tick(self) -> Tuple[int, float, Dict[str, float]]:
        libsumo.simulationStep()
        self.step += 1
        sim_time_s = float(libsumo.simulation.getTime())

        remaining = float(libsumo.simulation.getMinExpectedNumber())
        mean_speed = 0.0
        n = 0
        for eid in self.edge_ids:
            v = float(libsumo.edge.getLastStepMeanSpeed(eid))
            if v >= 0:
                mean_speed += v
                n += 1
        if n > 0:
            mean_speed /= n

        metrics = {
            "sim.remaining_veh": remaining,
            "edges.mean_speed_mps": mean_speed,
        }
        self.last_metrics = metrics
        return self.step, sim_time_s, metrics

    def collect_metric(
        self,
        # domain: Domain,
        domain: str,
        getter_name: str,
        object_id: str,
        additional_parameters: dict[str, str],
    ) -> str:
        domain_handle = getattr(libsumo, domain)
        getter_handle = getattr(domain_handle, getter_name)

        if object_id == "":
            return str(getter_handle(**additional_parameters))
        else:
            return str(getter_handle(object_id, **additional_parameters))

        # try:
        #     return getterHandle()
        # except:
        #     try:
        #         return getterHandle(objectId, additionalParam)
        #     except Exception as e:
        #         raise e
        # return None
