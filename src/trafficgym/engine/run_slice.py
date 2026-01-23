from __future__ import annotations
import asyncio
import grpc
import logging

from ..api import engine_pb2, engine_pb2_grpc

def raise_async_except(task):
    if task.cancelled():
        logging.error(f"{task.get_name()} was cancelled")
    elif task.exception():
        try:
            task.result()
        except Exception as e:
            raise e

async def main():
    # sumocfg_path = "/home/r/Code/TrafficGym/sumo_files/single_intersection/sim.sumocfg"
    sumocfg_path = "/home/diego/documents/TrafficGym/sumo_files/single_intersection/sim.sumocfg"

    tls_id = "TL0"

    async with grpc.aio.insecure_channel("127.0.0.1:50051") as channel:
        stub = engine_pb2_grpc.EngineServiceStub(channel)

        cr = await stub.CreateRun(engine_pb2.CreateRunRequest(
            sumocfg_path=sumocfg_path,
            sumo_binary="sumo",
            step_length_ms=1000,
        ))
        run_id = cr.run_id

        async def apply_once():
            await stub.Run(engine_pb2.RunRequest(run_id=run_id, max_steps=20))
            for i in range(50):
                await stub.ApplyActions(engine_pb2.ActionBundle(
                    run_id=run_id,
                    step=0,
                    actions=[
                        engine_pb2.Action(tls_set_phase=engine_pb2.TlsSetPhase(tls_id=tls_id, phase_index=1))
                    ],
                ))
                await stub.Run(engine_pb2.RunRequest(run_id=run_id, max_steps=50))
                await stub.ApplyActions(engine_pb2.ActionBundle(
                    run_id=run_id,
                    step=0,
                    actions=[
                        engine_pb2.Action(tls_set_phase=engine_pb2.TlsSetPhase(tls_id=tls_id, phase_index=0))
                    ],
                ))
                await stub.Run(engine_pb2.RunRequest(run_id=run_id, max_steps=30))

            await stub.CloseRun(engine_pb2.CloseRunRequest(run_id=run_id))

        asyncio.create_task(apply_once()).add_done_callback(raise_async_except)

        stream = stub.StreamTelemetry(engine_pb2.StreamTelemetryRequest(run_id=run_id))
        try:
            async for frame in stream:
                kv = {m.key: m.value for m in frame.metrics}
                print(frame.step, frame.sim_time_s, kv)
        except Exception as e:
            print(f"[ERROR]: {e.__str__()}")

if __name__ == "__main__":
    asyncio.run(main())
