from __future__ import annotations
from dataclasses import dataclass
import sys, os
from typing import Dict, List, Optional, Tuple
import uuid
from enum import Enum
import logging

if "SUMO_HOME" in os.environ:
    tools = os.path.join(os.environ["SUMO_HOME"], "tools")
    sys.path.append(tools)
else:
    logging.critical("please declare environment variable 'SUMO_HOME'")
    sys.exit(1)

import libsumo

Domain = Enum("Domain", list(map(lambda x: x.__name__, libsumo.DOMAINS)))


@dataclass
class RunConfig:
    sumocfg_path: str
    sumo_binary: str
    step_length_ms: int


class RunState:
    def __init__(self, cfg: RunConfig):
        self.cfg = cfg
        self.run_id = str(uuid.uuid4())
        self.started = False
        self.step = 0
        self.edge_ids: List[str] = []
        self.last_metrics: Dict[str, float] = {}
        self.max_steps: Optional[int] = None

    def start(self, max_steps: int):
        if self.started:
            return
        cmd = [
            self.cfg.sumo_binary,
            "-c",
            self.cfg.sumocfg_path,
            "--step-length",
            str(self.cfg.step_length_ms / 1000.0),
        ]
        libsumo.start(cmd)
        self.edge_ids = list(libsumo.edge.getIDList())
        self.started = True
        self.max_steps = max_steps
        self.step = 0

    def close(self):
        if self.started:
            try:
                libsumo.close()
            finally:
                self.started = False

    def apply_tls_set_phase(self, tls_id: str, phase_index: int):
        libsumo.trafficlight.setPhase(tls_id, int(phase_index))

    def invoke_setter(
        self,
        domain: Domain,
        setter_name: str,
        object_id: str,
        value: str,
        additional_parameters: dict,
    ):
        domain_handle = getattr(libsumo, domain)
        setter_handle = getattr(domain_handle, setter_name)

        if object_id == "":
            # not sure if there are any setters which do not need an object id
            return setter_handle(value, **additional_parameters)
        else:
            try:
                return setter_handle(object_id, value, **additional_parameters)
            except TypeError:
                return setter_handle(object_id, int(value), **additional_parameters)

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

        tlsStateIndex = float(libsumo.trafficlight.getPhase("TL0"))

        metrics = {
            "sim.remaining_veh": remaining,
            "edges.mean_speed_mps": mean_speed,
            # "tlsState": tlsStateIndex
        }
        self.last_metrics = metrics
        return self.step, sim_time_s, metrics

    def collect_metric(
        self,
        domain: Domain,
        getter_name: str,
        object_id: str,
        additional_parameters: dict,
    ):
        domain_handle = getattr(libsumo, domain)
        getter_handle = getattr(domain_handle, getter_name)

        if object_id == "":
            return getter_handle(**additional_parameters)
        else:
            return getter_handle(object_id, **additional_parameters)

        # try:
        #     return getterHandle()
        # except:
        #     try:
        #         return getterHandle(objectId, additionalParam)
        #     except Exception as e:
        #         raise e
        # return None
