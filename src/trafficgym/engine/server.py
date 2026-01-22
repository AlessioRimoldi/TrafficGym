from __future__ import annotations
import asyncio
from typing import Dict

import grpc

from .kernel import RunConfig, RunState
from ..api import engine_pb2, engine_pb2_grpc

def raise_async_except(task):
    if task.exception():
        try:
            task.result()
        except Exception as e:
            raise e

class EngineService(engine_pb2_grpc.EngineServiceServicer):
    def __init__(self):
        self.runs: Dict[str, RunState] = {}
        self.telemetry_queues: Dict[str, asyncio.Queue] = {}
        self.run_tasks: Dict[str, asyncio.Task] = {}

    async def CreateRun(self, request, context):
        cfg = RunConfig(
            sumocfg_path=request.sumocfg_path,
            sumo_binary=request.sumo_binary or "sumo",
            step_length_ms=request.step_length_ms or 1000,
        )
        run = RunState(cfg)
        self.runs[run.run_id] = run
        self.telemetry_queues[run.run_id] = asyncio.Queue()
        return engine_pb2.CreateRunResponse(run_id=run.run_id, input_artifacts=[])

    async def StartRun(self, request, context):
        run_id = request.run_id
        if run_id not in self.runs:
            await context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        if run_id not in self.run_tasks:
            task = asyncio.create_task(self._run_loop(run_id, int(request.max_steps or 1000)))
            self.run_tasks[run_id] = task
            task.add_done_callback(raise_async_except)
        return engine_pb2.StartRunResponse(run_id=run_id)

    async def ApplyActions(self, request, context):
        run_id = request.run_id
        if run_id not in self.runs:
            await context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        run = self.runs[run_id]
        for a in request.actions:
            p = a.WhichOneof("payload")
            if p == "tls_set_phase":
                run.apply_tls_set_phase(a.tls_set_phase.tls_id, a.tls_set_phase.phase_index)
        return engine_pb2.ApplyActionsResponse(run_id=run_id)

    async def StreamTelemetry(self, request, context):
        run_id = request.run_id
        if run_id not in self.telemetry_queues:
            await context.abort(grpc.StatusCode.NOT_FOUND, "run_id not found")
        q = self.telemetry_queues[run_id]
        while True:
            frame = await q.get()
            if frame is None:
                return
            yield frame

    async def _run_loop(self, run_id: str, max_steps: int):
        run = self.runs[run_id]
        q = self.telemetry_queues[run_id]
        try:
            run.start(max_steps=max_steps)
            for _ in range(max_steps):
                step, sim_time_s, metrics = run.tick()
                frame = engine_pb2.TelemetryFrame(
                    run_id=run_id,
                    step=step,
                    sim_time_s=sim_time_s,
                    metrics=[engine_pb2.KeyValue(key=k, value=float(v)) for k, v in metrics.items()],
                )
                await q.put(frame)
                await asyncio.sleep(0)
        except Exception as e:
            print(f"[Error]: {e.__str__()}")
            raise e
        finally:
            run.close()
            await q.put(None)

async def serve(host: str = "127.0.0.1", port: int = 50051):
    server = grpc.aio.server()
    engine_pb2_grpc.add_EngineServiceServicer_to_server(EngineService(), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    await server.wait_for_termination()

def main():
    asyncio.run(serve())

if __name__ == "__main__":
    main()
