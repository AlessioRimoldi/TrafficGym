from trafficgym.engine.ports.simulation import (
    SimulationPort,
    ValDict,
    RunConfig,
)
from google.protobuf.struct_pb2 import Value
from dataclasses import dataclass
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

        remaining = float(libsumo.simulation.getMinExpectedNumber())

        metrics = {
            "sim.remaining_veh": Value(number_value=remaining),
        }
        self.last_metrics = metrics

        return self.step, sim_time_s, metrics

    def apply(
        self,
        domain: str,
        setter_name: str,
        args: ValDict,
    ) -> None:
        domain_handle = getattr(libsumo, domain)
        setter_handle = getattr(domain_handle, setter_name)

        try:
            setter_handle(**args)
        except TypeError:
            if "value" in args:
                args_no_value = {k: v for k, v in args.items() if k != "value"}
            setter_handle(value=int(args["value"]), **args_no_value)

        logging.debug(f"Invoked setter: {domain}.{setter_name}_{args}")

    def query(
        self,
        domain: str,
        getter_name: str,
        args: ValDict,
    ) -> Value:
        domain_handle = getattr(libsumo, domain)
        getter_handle = getattr(domain_handle, getter_name)

        return Value(string_value=str(getter_handle(**args)))

        # try:
        #     return getterHandle()
        # except:
        #     try:
        #         return getterHandle(objectId, additionalParam)
        #     except Exception as e:
        #         raise e
        # return None
