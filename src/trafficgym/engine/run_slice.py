from __future__ import annotations
import asyncio
import grpc

from .proto import engine_pb2, engine_pb2_grpc

async def main():
    sumocfg_path = "/home/r/Code/TrafficGym/sumo_files/single_intersection/sim.sumocfg"
    tls_id = "TL0"

    async with grpc.aio.insecure_channel("127.0.0.1:50051") as channel:
        stub = engine_pb2_grpc.EngineServiceStub(channel)

        cr = await stub.CreateRun(engine_pb2.CreateRunRequest(
            sumocfg_path=sumocfg_path,
            sumo_binary="sumo",
            step_length_ms=1000,
        ))
        run_id = cr.run_id

        await stub.StartRun(engine_pb2.StartRunRequest(run_id=run_id, max_steps=200))

        async def apply_once():
            await asyncio.sleep(0.2)
            await stub.ApplyActions(engine_pb2.ActionBundle(
                run_id=run_id,
                step=0,
                actions=[
                    engine_pb2.Action(payload=engine_pb2.TlsSetPhase(tls_id=tls_id, phase_index=1))
                ],
            ))

        asyncio.create_task(apply_once())

        stream = stub.StreamTelemetry(engine_pb2.StreamTelemetryRequest(run_id=run_id))
        async for frame in stream:
            kv = {m.key: m.value for m in frame.metrics}
            print(frame.step, frame.sim_time_s, kv)

if __name__ == "__main__":
    asyncio.run(main())
