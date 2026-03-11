from trafficgym.engine.ports.simulation import (
    SimulationPort,
    ValDict,
    RunConfig,
)
from trafficgym.api.engine_pb2 import CustomValue
from trafficgym.engine.helpers import extract_value
import os
import sys
import logging
import libsumo  # type: ignore[import-untyped]


class LibsumoAdapter(SimulationPort):
    def __init__(self, cfg: RunConfig, step_length_ms: int) -> None:
        super().__init__(step_length_ms)
        self.cfg = cfg

        if "SUMO_HOME" in os.environ:
            tools = os.path.join(os.environ["SUMO_HOME"], "tools")
            sys.path.append(tools)
        else:
            logging.critical("please declare environment variable 'SUMO_HOME'")
            sys.exit(1)

    def start(self) -> None:
        if self.started or self.closed:
            return
        cmd = [
            self.cfg.sumo_binary,
            "-c",
            self.cfg.sumocfg_path,
            "--step-length",
            str(self.seconds_per_step),
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
                self.closed = True

    def tick(self) -> tuple[int, float, ValDict]:
        libsumo.simulationStep()
        self.step += 1
        sim_time_s = float(libsumo.simulation.getTime())

        remaining = int(libsumo.simulation.getMinExpectedNumber())

        metrics = {
            "sim.remaining_veh": CustomValue(int_value=remaining),
        }
        self.last_metrics = metrics

        return self.step, sim_time_s, metrics

    def apply(
        self,
        domain: str,
        setter_name: str,
        object_id: str,
        args: ValDict,
    ) -> None:
        domain_handle = getattr(libsumo, domain)
        setter_handle = getattr(domain_handle, setter_name)

        setter_handle(
            object_id,
            **{name: extract_value(value) for name, value in args.items()},
        )

        logging.debug(
            f"Invoked setter: {domain}.{object_id}.{setter_name}_{args}"
        )

    def query(
        self,
        domain: str,
        getter_name: str,
        object_id: str,
        args: ValDict,
    ) -> str:
        domain_handle = getattr(libsumo, domain)
        getter_handle = getattr(domain_handle, getter_name)

        logging.debug(
            f"Invoked getter: {domain}.{object_id}.{getter_name}_{args}"
        )
        return str(getter_handle(object_id, **args))

        # try:
        #     return getterHandle()
        # except:
        #     try:
        #         return getterHandle(objectId, additionalParam)
        #     except Exception as e:
        #         raise e
        # return None
