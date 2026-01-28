from __future__ import annotations
import asyncio
import grpc
import logging

from ..api import engine_pb2, engine_pb2_grpc


def raise_async_except(task):
    if task.cancelled():
        logging.error(f"{task.get_name()} was cancelled")
    elif task.exception() is not None:
        exception = task.exception()
        if isinstance(exception, grpc.RpcError):
            if exception.code() == grpc.StatusCode.UNKNOWN:
                logging.error(
                    "An unknown gRPC error occurred, check server console."
                )
                # raise exception
            elif exception.code() == grpc.StatusCode.UNAVAILABLE:
                logging.critical("Lost connection to the server.")
            else:
                raise exception
        else:
            logging.error(f"An exception occurred: {exception}")


async def main():
    sumocfg_path = (
        "/home/diego/documents/"
        # "/home/r/Code"
        "TrafficGym/sumo_files/single_intersection/sim.sumocfg"
    )

    tls_id = "TL0"

    async with grpc.aio.insecure_channel("127.0.0.1:50051") as channel:
        stub = engine_pb2_grpc.EngineServiceStub(channel)

        cr = await stub.CreateRun(
            engine_pb2.CreateRunRequest(
                sumocfg_path=sumocfg_path,
                sumo_binary="sumo",
                step_length_ms=1000,
            )
        )
        run_id = cr.run_id

        async def apply_once():
            # await stub.Subscribe(
            #     engine_pb2.SubscribeRequest(
            #         run_id=run_id,
            #         domain="trafficlight",
            #         getter_name="getPhase",
            #         object_id=tls_id,
            #     )
            # )
            # await stub.Subscribe(
            #     engine_pb2.SubscribeRequest(
            #         run_id=run_id,
            #         domain="lane",
            #         getter_name="getLastStepVehicleNumber",
            #         object_id=":J0_0_0",
            #     )
            # )
            # await stub.Subscribe(
            #     engine_pb2.SubscribeRequest(
            #         run_id=run_id, domain="vehicle", getter_name="getIDList"
            #     )
            # )
            await stub.Subscribe(
                engine_pb2.SubscribeRequest(
                    name="My Favorite Traffic Signal",
                    run_id=run_id,
                    domain="trafficlight",
                    getter_name="getRedYellowGreenState",
                    object_id=tls_id,
                )
            )
            await stub.Run(engine_pb2.RunRequest(run_id=run_id, max_steps=20))
            for _ in range(20):
                await stub.ApplyActions(
                    engine_pb2.ActionBundle(
                        run_id=run_id,
                        step=0,
                        actions=[
                            # engine_pb2.Action(
                            #     setter=engine_pb2.GenericSetter(
                            #         domain="trafficlight",
                            #         setter_name="setPhase",
                            #         object_id=tls_id,
                            #         value="1",
                            #     )
                            # ),
                            engine_pb2.Action(
                                setter=engine_pb2.GenericSetter(
                                    domain="trafficlight",
                                    setter_name="setRedYellowGreenState",
                                    object_id=tls_id,
                                    value="rGrG",
                                )
                            )
                        ],
                    )
                )
                await stub.Run(
                    engine_pb2.RunRequest(run_id=run_id, max_steps=50)
                )
                await stub.ApplyActions(
                    engine_pb2.ActionBundle(
                        run_id=run_id,
                        step=0,
                        actions=[
                            # engine_pb2.Action(
                            #     setter=engine_pb2.GenericSetter(
                            #         domain="trafficlight",
                            #         setter_name="setPhase",
                            #         object_id=tls_id,
                            #         value="0",
                            #     )
                            # ),
                            engine_pb2.Action(
                                setter=engine_pb2.GenericSetter(
                                    domain="trafficlight",
                                    setter_name="setRedYellowGreenState",
                                    object_id=tls_id,
                                    value="GrGr",
                                )
                            )
                        ],
                    )
                )
                await stub.Run(
                    engine_pb2.RunRequest(run_id=run_id, max_steps=30)
                )

            await stub.CloseRun(engine_pb2.CloseRunRequest(run_id=run_id))

        asyncio.create_task(apply_once()).add_done_callback(
            raise_async_except
        )  # crash hard for now

        telemetry_stream = stub.StreamTelemetry(
            engine_pb2.StreamRequest(run_id=run_id)
        )

        subscription_stream = stub.StreamSubscriptions(
            engine_pb2.StreamRequest(run_id=run_id)
        )

        async def handle_stream(stream, prefix=""):
            async for frame in stream:
                kv = {}
                for m in frame.metrics:
                    field = m.WhichOneof("value")
                    if field == "string_value":
                        kv[m.key] = m.string_value
                    elif field == "double_value":
                        kv[m.key] = m.double_value

                # print('T: ', frame.step, frame.sim_time_s, kv)
                print(prefix, frame.step, kv)

        try:
            await asyncio.gather(
                handle_stream(telemetry_stream, "T: "),
                handle_stream(subscription_stream, "S: "),
            )
        except Exception as e:
            logging.error(str(e))


if __name__ == "__main__":
    asyncio.run(main())
