from trafficgym.experiment_sdk.experiments.base import Experiment

class service_station(Experiment):
    async def run_experiment(self) -> None:
        tls_id = "TL0"

        # await stub.Subscribe(
        #     engine_pb2.SubscribeRequest(
        #         name="CAPass",
        #         run_id=run_id,
        #         domain="calibrator",
        #         getter_name="getPassed",
        #         object_id="ca_0",
        #     )
        # )
        # await stub.Subscribe(
        #     engine_pb2.SubscribeRequest(
        #         name="CAFlow",
        #         run_id=run_id,
        #         domain="calibrator",
        #         getter_name="getVehsPerHour",
        #         object_id="ca_0",
        #     )
        # )

        await self.run.tls.set_program(tls_id, "off")

        await self.run.run_time(300)

        controller = await self.run.tls.meter_controller(tls_id, "e1_1")

        await self.run.run_time(600)

        # logging.info("Starting Calibrator at flow rate 0")

        # await stub.ApplyActions(
        #     engine_pb2.ApplyActionsRequest(
        #         run_id=run_id,
        #         action_bundle=engine_pb2.ActionBundle(actions=[
        #             engine_pb2.Action(
        #                 setter=engine_pb2.GenericSetter(
        #                     domain="calibrator",
        #                     setter_name="setFlow",
        #                     object_id="ca_0",
        #                     value=Value(number_value=after_run.new_time),
        #                     additional_parameters=[
        #                         engine_pb2.Parameter(
        #                             name="end",
        #                             value=Value(
        #                                 number_value=float(
        #                                     after_run.new_time + 600
        #                                 )
        #                             ),
        #                         ),
        #                         engine_pb2.Parameter(
        #                             name="vehsPerHour",
        #                             value=Value(number_value=100),
        #                         ),
        #                         engine_pb2.Parameter(
        #                             name="speed",
        #                             value=Value(number_value=10),
        #                         ),
        #                         engine_pb2.Parameter(
        #                             name="typeID",
        #                             value=Value(
        #                                 string_value="DEFAULT_VEHTYPE"
        #                             ),
        #                         ),
        #                         engine_pb2.Parameter(
        #                             name="routeID",
        #                             value=Value(string_value="f_2"),
        #                         ),
        #                     ],
        #                 )
        #             )
        #         ])
        #     )
        # )

        await self.run.run_time(600)

        # await stub.ApplyActions(
        #     engine_pb2.ActionBundle(
        #         run_id=run_id,
        #         actions=[
        #             engine_pb2.Action(
        #                 setter=engine_pb2.GenericSetter(
        #                     domain="calibrator",
        #                     setter_name="setFlow",
        #                     object_id="ca_0",
        #                     value=Value(number_value=after_run.new_time),
        #                     additional_parameters=[
        #                         engine_pb2.Parameter(
        #                             name="end",
        #                             value=Value(
        #                                 number_value=float(
        #                                     after_run.new_time + 600
        #                                 )
        #                             ),
        #                         ),
        #                         engine_pb2.Parameter(
        #                             name="vehsPerHour",
        #                             value=Value(number_value=250),
        #                         ),
        #                         engine_pb2.Parameter(
        #                             name="speed",
        #                             value=Value(number_value=-1),
        #                         ),
        #                         engine_pb2.Parameter(
        #                             name="typeID",
        #                             value=Value(string_value="DEFAULT_VEHTYPE"),
        #                         ),
        #                         engine_pb2.Parameter(
        #                             name="routeID",
        #                             value=Value(string_value="f_2"),
        #                         ),
        #                     ],
        #                 )
        #             )
        #         ],
        #     )
        # )
        # logging.info("Calibrator should stop by now")

        await self.run.run_time(1000)

        controller.cancel()
