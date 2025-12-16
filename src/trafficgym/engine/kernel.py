from __future__ import annotations
from dataclasses import dataclass
import sys, os
from typing import Dict, List, Optional, Tuple
import uuid

if 'SUMO_HOME' in os.environ:
    tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
    sys.path.append(tools)        
else:   
    sys.exit("please declare environment variable 'SUMO_HOME'")

import libsumo

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
        cmd = [self.cfg.sumo_binary, "-c", self.cfg.sumocfg_path, "--step-length", str(self.cfg.step_length_ms / 1000.0)]
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
