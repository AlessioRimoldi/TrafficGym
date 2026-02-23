import pytest
import pytest_asyncio

import asyncio
import logging

from trafficgym.engine.server import EngineService
from trafficgym.engine.client import sumocfg_path
from trafficgym.engine.adapters.factories import (
    FakeAdapterFactory,
    LibsumoAdapterFactory,
)
from trafficgym.engine.adapters.fake_adapter import (
    FakeStateDict,
    FakeStateDictKey,
)
from trafficgym.engine.ports.adapter_factory import AdapterFactory
from google.protobuf.struct_pb2 import Value, NullValue
from trafficgym.api.engine_pb2 import (
    CreateRunRequest,
    CreateRunResponse,
    RunRequest,
    CloseRunRequest,
    ActionBundle,
    ApplyActionsRequest,
    ApplyActionsResponse,
    Action,
    GenericSetter,
    Parameter,
    SubscribeRequest,
    UnsubscribeRequest,
    FetchRequest,
    StreamRequest,
    RegisterInterruptRequest,
    MetricNameAndValue,
    Operation,
    InterruptEvent,
    AcknowledgeInterruptRequest,
    CancelInterruptRequest,
)
from grpc import ServicerContext, StatusCode
from typing import (
    Literal,
    TypedDict,
    Protocol,
    Callable,
    cast,
    Awaitable,
    AsyncIterator,
)
from typing_extensions import Never, NotRequired
from dataclasses import dataclass, asdict
from contextlib import suppress


class SupportsAbort(Protocol):
    def abort(self, code: StatusCode, details: str) -> Never: ...


class CreateRunParams(TypedDict, total=False):
    sumocfg_path: str
    sumo_binary: Literal["sumo", "sumo-gui"]
    step_length_ms: int


class ServiceParams(TypedDict):
    kind: Literal["fake", "libsumo"]
    initial_state: NotRequired[FakeStateDict]


class GrpcAbort(Exception):
    def __init__(self, code: StatusCode, details: str):
        self.code = code
        self.details = details
        super().__init__(f"{code.name}: {details}")


class FakeContext:
    def abort(self, code: StatusCode, details: str) -> Never:
        raise GrpcAbort(code, details)


ServiceFactoryType = Callable[[AdapterFactory], EngineService]


@pytest.fixture
def service(request: pytest.FixtureRequest) -> EngineService:
    """Generate an EngineService.

    By default, a FakeAdapter will be used to fake libsumo.
    By passing `"kind": "libsumo"` via a parametrized fixture,
    the real libsumo library will be used.
    By passing `"kind": "fake"`, an initial internal virtual state
    can be set via `"initial_state": FakeStateDict`, for instance to check subscriptions
    retrieve correct values"""
    params = getattr(request, "param", {})
    if not isinstance(params, dict):
        raise TypeError("Fixture parameter should be a dict")

    kind = params.get("kind", "fake")

    if kind == "fake":
        # deep copy initial state to prevent test cross-contaminatio n
        # when the fake state is mutated
        initial_state = {
            k: v for k, v in params.get("initial_state", {}).items()
        }
        return EngineService(FakeAdapterFactory(initial_state))
    elif kind == "libsumo":
        adapter_factory = LibsumoAdapterFactory()
    else:
        raise ValueError(f"Unknown service kind: {kind}")

    return EngineService(adapter_factory)


@pytest.fixture
def fake_context() -> ServicerContext:
    return cast(ServicerContext, FakeContext())


CreateRunFactoryType = Callable[
    [CreateRunParams | None], Awaitable[CreateRunResponse]
]


@pytest_asyncio.fixture
async def create_run_factory(
    service: EngineService,
    fake_context: ServicerContext,
) -> CreateRunFactoryType:
    """Fixture which returns a function to create runs
    whose parameters can be overriden at creation time"""

    async def _exec(
        overrides: CreateRunParams | None = None,
    ) -> CreateRunResponse:
        overrides = overrides or {}
        defaults: CreateRunParams = dict(
            sumocfg_path=sumocfg_path,
            sumo_binary="sumo",
            step_length_ms=1000,
        )

        param: CreateRunParams = {**defaults, **overrides}

        request = CreateRunRequest(**param)
        response = await service.CreateRun(
            request=request, context=fake_context
        )

        return response

    return _exec


@pytest.mark.parametrize(
    "override,service",
    [
        ({"sumocfg_path": "/dev/null"}, {"kind": "libsumo"}),
        ({"step_length_ms": -1}, {"kind": "libsumo"}),
        ({"step_length_ms": 0}, {"kind": "libsumo"}),
    ],
    indirect=["service"],
)
@pytest.mark.asyncio
async def test_create_run_fails_invalid_sumo_parameters(
    override: CreateRunParams,
    service: EngineService,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Test invalid parameters which should raise a TraCIException,
    which is repackaged into a GrpcAbort. The run should not be
    added into the list of runs because it will never start"""

    assert len(service.runs) == 0

    with pytest.raises(GrpcAbort):
        await create_run_factory(override)

    assert len(service.runs) == 0


# @pytest.mark.asyncio
# async def test_create_run_invalid_sumo_binary_name(
#     created_run_factory: Callable[[CreateRunParams], CreatedRunHandle],
# ):

#     with pytest.raises(GrpcAbort):
#         await created_run_factory({"sumo_binary": "fumo"})

#     pass # for some reason this does not fail?


@pytest.mark.asyncio
async def test_create_run_adds_to_runs(
    service: EngineService, create_run_factory: CreateRunFactoryType
) -> None:
    """Ensure that creating a run adds elements to the runs list"""
    assert len(service.runs) == 0

    await create_run_factory(None)

    assert len(service.runs) == 1


@pytest.mark.parametrize(
    "service", [{"kind": "fake"}, {"kind": "libsumo"}], indirect=True
)
@pytest.mark.asyncio
async def test_run_after_create(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensures run state is correctly modified after
    successive run commands are issued"""
    RUN_TIME_S = 10
    RUN_STEP = 10
    STEP_LENGTH_MS = 1000

    create_factory_response = await create_run_factory(
        {"step_length_ms": STEP_LENGTH_MS}
    )

    run_request = RunRequest(
        run_id=create_factory_response.run_id, time=RUN_TIME_S
    )

    first_step_response = await service.Run(
        request=run_request, context=fake_context
    )

    assert first_step_response.new_time == RUN_TIME_S
    assert first_step_response.new_step == RUN_TIME_S * 1000 // STEP_LENGTH_MS

    run_request = RunRequest(
        run_id=create_factory_response.run_id, steps=RUN_STEP
    )

    second_step_response = await service.Run(
        request=run_request, context=fake_context
    )

    assert (
        second_step_response.new_time
        == first_step_response.new_time + RUN_STEP * STEP_LENGTH_MS / 1000
    )
    assert (
        second_step_response.new_step == first_step_response.new_step + RUN_STEP
    )


@pytest.mark.asyncio
async def test_max_run_modes_unsupported(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure max_steps and max_time run modes are not used"""
    create_run_response = await create_run_factory(None)

    run_request_max_step = RunRequest(
        run_id=create_run_response.run_id, max_steps=100
    )

    run_request_max_time = RunRequest(
        run_id=create_run_response.run_id, max_time=100
    )

    with pytest.raises(GrpcAbort) as e:
        await service.Run(request=run_request_max_step, context=fake_context)

    assert e.value.code == StatusCode.UNIMPLEMENTED

    with pytest.raises(GrpcAbort) as e:
        await service.Run(request=run_request_max_time, context=fake_context)

    assert e.value.code == StatusCode.UNIMPLEMENTED


@pytest.mark.asyncio
async def test_run_missing_run_mode(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that starting a run without
    a run type issues an error"""
    response = await create_run_factory(None)

    malformed_request = RunRequest(run_id=response.run_id)

    with pytest.raises(GrpcAbort):
        await service.Run(request=malformed_request, context=fake_context)

    normal_request = RunRequest(run_id=response.run_id, steps=15)

    run_response = await service.Run(
        request=normal_request, context=fake_context
    )

    assert run_response.new_step == normal_request.steps


@pytest.mark.parametrize("invalid_id", ["", "yo"])
@pytest.mark.asyncio
async def test_run_invalid_run_id(
    invalid_id: str,
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that starting a run with an invalid run_id
    issues an abort"""
    response = await create_run_factory(None)

    invalid_run_id_request = RunRequest(run_id=invalid_id, steps=10)

    with pytest.raises(GrpcAbort):
        await service.Run(invalid_run_id_request, fake_context)

    normal_request = RunRequest(run_id=response.run_id, steps=15)

    run_response = await service.Run(normal_request, fake_context)

    assert run_response.new_step == normal_request.steps


@pytest.mark.asyncio
async def test_run_again_before_exec_end(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that executing a run during another's
    excution issues an abort."""
    response = await create_run_factory(None)

    first_request = RunRequest(run_id=response.run_id, steps=15)
    second_request = RunRequest(run_id=response.run_id, steps=20)

    first_task = asyncio.create_task(service.Run(first_request, fake_context))
    await asyncio.sleep(0)  # potentially flaky

    second_task = asyncio.create_task(service.Run(second_request, fake_context))
    with pytest.raises(GrpcAbort) as e:
        await second_task

    assert e.value.code == StatusCode.ALREADY_EXISTS

    first_response = await first_task

    assert first_response.new_step == first_request.steps


@pytest.mark.parametrize("invalid_run_id", ["", "heyho"])
@pytest.mark.asyncio
async def test_close_run_fails_invalid_run_id(
    invalid_run_id: str,
    service: EngineService,
    fake_context: ServicerContext,
) -> None:
    """Ensure close_run fails cleanly when provided invalid arguments"""
    request = CloseRunRequest(run_id=invalid_run_id)

    with pytest.raises(GrpcAbort):
        await service.CloseRun(request, fake_context)


@pytest.mark.asyncio
async def test_close_run(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that running a closed run fails"""
    create_run_response = await create_run_factory(None)

    close_run_request = CloseRunRequest(run_id=create_run_response.run_id)

    await service.CloseRun(close_run_request, fake_context)

    run_request = RunRequest(run_id=create_run_response.run_id, steps=50)

    with pytest.raises(GrpcAbort):
        await service.Run(run_request, fake_context)


@pytest.mark.asyncio
async def test_close_run_during_exec_triggers_warning(
    caplog: pytest.LogCaptureFixture,
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensures that a warning is issued on the server side
    when a client tries to close a run which is currently stepping"""
    create_run_response = await create_run_factory(None)

    run_request = RunRequest(run_id=create_run_response.run_id, steps=100)
    close_run_request = CloseRunRequest(run_id=create_run_response.run_id)

    asyncio.create_task(service.Run(run_request, fake_context))
    await asyncio.sleep(0)

    with caplog.at_level(logging.WARNING):
        await service.CloseRun(close_run_request, fake_context)

    assert caplog.record_tuples == [
        (
            "root",
            logging.WARNING,
            f"Closing Run {create_run_response.run_id}, "
            f"despite running task for that run",
        )
    ]


@pytest.mark.asyncio
async def test_close_run_twice(
    caplog: pytest.LogCaptureFixture,
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that closing an already closed run triggers an abort"""
    create_run_response = await create_run_factory(None)

    close_run_request = CloseRunRequest(run_id=create_run_response.run_id)

    await service.CloseRun(close_run_request, fake_context)

    with pytest.raises(GrpcAbort):
        await service.CloseRun(close_run_request, fake_context)


@dataclass
class GenericSetterType:
    domain: str
    setter_name: str
    object_id: str
    parameters: list[Parameter] | None = None


ActionFactoryType = Callable[[GenericSetterType], Action]


@pytest.fixture
def action_factory() -> ActionFactoryType:
    def _make_action(gs: GenericSetterType) -> Action:
        action = Action(setter=GenericSetter(**asdict(gs)))
        return action

    return _make_action


ActionBundleFactoryType = Callable[
    [list[GenericSetterType] | None], ActionBundle
]

DEFAULT_LIBSUMO_ACTIONS = [
    GenericSetterType(
        domain="trafficlight",
        setter_name="setProgram",
        object_id="TL0",
        parameters=[
            Parameter(name="programID", value=Value(string_value="10")),
        ],
    ),
    GenericSetterType(
        domain="trafficlight",
        setter_name="setRedYellowGreenState",
        object_id="TL0",
        parameters=[
            Parameter(name="state", value=Value(string_value="rGrG")),
        ],
    ),
]

DEFAULT_FAKE_ACTIONS = [
    GenericSetterType(
        domain="fake_domain",
        setter_name="setDefaultNumber",
        object_id="default",
        parameters=[Parameter(name="value", value=Value(number_value=42))],
    ),
    GenericSetterType(
        domain="fake_domain",
        setter_name="setDefaultString",
        object_id="default",
        parameters=[
            Parameter(name="value", value=Value(string_value="foobar"))
        ],
    ),
]


@pytest.fixture
def action_bundle_factory(
    action_factory: ActionFactoryType,
) -> ActionBundleFactoryType:
    """Generate an Action Bundle for the given run_id.

    Will add to the bundles the list of actions, and the list of
    GenericSetterTypes, passed through action_factory, in that order."""

    def _make_action_bundle(
        gsts: list[GenericSetterType] | None,
    ) -> ActionBundle:
        gsts = gsts or []
        gst_actions = [action_factory(gst) for gst in gsts]

        action_bundle = ActionBundle(actions=gst_actions)

        return action_bundle

    return _make_action_bundle


@pytest.mark.parametrize("invalid_run_id", ["", "graha"])
@pytest.mark.asyncio
async def test_apply_action_invalid_run_id(
    invalid_run_id: str,
    service: EngineService,
    fake_context: ServicerContext,
) -> None:
    """Ensure that apply action fails cleanly with invalid run_id"""
    apply_actions_request = ApplyActionsRequest(
        run_id=invalid_run_id, action_bundle=None
    )

    with pytest.raises(GrpcAbort):
        await service.ApplyActions(apply_actions_request, fake_context)


@pytest.mark.asyncio
async def test_apply_action_step_unimplemented(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that apply action fails when trying to use step"""
    create_run_response = await create_run_factory(None)

    apply_actions_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=ActionBundle(step=10, actions=[]),
    )

    with pytest.raises(GrpcAbort) as e:
        await service.ApplyActions(apply_actions_request, fake_context)

    assert e.value.code == StatusCode.UNIMPLEMENTED


FAKE_SERVICE_CONFIG = {
    "kind": "fake",
    "initial_state": {
        FakeStateDictKey(
            domain="fake_domain",
            object_id="fake_object",
            attribute="Program",
        ): Value(string_value="off")
    },
}


@pytest.mark.parametrize(
    "service, requires_collect, expected",
    [
        (FAKE_SERVICE_CONFIG, False, [None, "off", "off", "42"]),
        (FAKE_SERVICE_CONFIG, True, ["off", "off", "42", "42"]),
    ],
    indirect=["service"],
)
@pytest.mark.asyncio
async def test_apply_actions_requires_collect_true_false(
    action_bundle_factory: ActionBundleFactoryType,
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
    requires_collect: bool,
    expected: list[str | None],
) -> None:
    """Ensure that subscriptions are collected after a
    run when the requires_collect flag is not set.
    When the requires_collect flag is set, ensure the collection
    occurs despite no run being commanded"""
    create_run_response = await create_run_factory(None)

    subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        object_id="fake_object",
        getter_name="getProgram",
    )

    subscribe_response = await service.Subscribe(
        subscribe_request, fake_context
    )

    fetch_request = FetchRequest(
        fingerprint=subscribe_response.fingerprint,
        requires_collect=requires_collect,
    )

    first_fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    run_one_step_request = RunRequest(
        run_id=create_run_response.run_id, steps=1
    )

    await service.Run(run_one_step_request, fake_context)

    second_fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    apply_actions_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=action_bundle_factory(
            [
                GenericSetterType(
                    "fake_domain",
                    "setProgram",
                    "fake_object",
                    [Parameter(name="value", value=Value(string_value="42"))],
                )
            ],
        ),
    )

    await service.ApplyActions(apply_actions_request, fake_context)

    third_fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    await service.Run(run_one_step_request, fake_context)

    fourth_fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    assert first_fetch_response.fetched.has_value == (expected[0] is not None)
    assert second_fetch_response.fetched.has_value
    assert third_fetch_response.fetched.has_value
    assert fourth_fetch_response.fetched.has_value

    assert first_fetch_response.fetched.value == (expected[0] or "")
    assert second_fetch_response.fetched.value == expected[1]
    assert third_fetch_response.fetched.value == expected[2]
    assert fourth_fetch_response.fetched.value == expected[3]


@pytest.mark.asyncio
async def test_apply_actions_legal_illegal_negative(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
    action_bundle_factory: ActionBundleFactoryType,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """noNegativeNumber is a special property in the fake which simulates a property for which libsumo
    might reject a certain input. For example, libsumo.setSpeed() might accept -1 to reset the speed control
     but would reject other inputs. The fake will reject all negative numbers with a TraCIException.

    This test ensures that rejects are correctly handled, by issuing a warning to the server console, and returning
    evidence of the exception in the ApplyActions response."""
    create_run_response = await create_run_factory(None)

    subscription_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="test_domain",
        getter_name="getNoNegativeNumber",
        object_id="veh1",
    )

    subscribe_response = await service.Subscribe(
        subscription_request, fake_context
    )

    legal_action_bundle = action_bundle_factory(
        [
            GenericSetterType(
                "test_domain",
                "setNoNegativeNumber",
                "veh1",
                [Parameter(name="value", value=Value(number_value=10))],
            )
        ],
    )

    legal_action_bundle_response = await service.ApplyActions(
        ApplyActionsRequest(
            run_id=create_run_response.run_id, action_bundle=legal_action_bundle
        ),
        fake_context,
    )

    illegal_action_bundle = action_bundle_factory(
        [
            GenericSetterType(
                "test_domain",
                "setNoNegativeNumber",
                "veh1",
                [Parameter(name="value", value=Value(number_value=-1))],
            )
        ],
    )

    with caplog.at_level(logging.WARNING):
        illegal_action_bundle_response = await service.ApplyActions(
            ApplyActionsRequest(
                run_id=create_run_response.run_id,
                action_bundle=illegal_action_bundle,
            ),
            fake_context,
        )

    assert len(legal_action_bundle_response.errors) == 0
    assert len(illegal_action_bundle_response.errors) == 1
    assert (
        illegal_action_bundle_response.errors[0]
        == "Setter: invalid setter argument for noNegativeNumber"
    )

    fetch_request = FetchRequest(
        fingerprint=subscribe_response.fingerprint, requires_collect=True
    )

    fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    assert fetch_response.fetched.has_value
    assert float(fetch_response.fetched.value) == 10


@pytest.mark.asyncio
async def test_apply_actions_empty(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
    action_bundle_factory: ActionBundleFactoryType,
) -> None:
    create_run_response = await create_run_factory(None)

    empty_actions_request = ApplyActionsRequest(
        run_id=create_run_response.run_id
    )

    empty_apply_actions_response = await service.ApplyActions(
        empty_actions_request, fake_context
    )

    assert empty_apply_actions_response.errors == []

    empty_bundle_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=ActionBundle(actions=[]),
    )

    empty_action_bundle_response = await service.ApplyActions(
        empty_bundle_request, fake_context
    )

    assert empty_action_bundle_response.errors == []


@pytest.mark.parametrize("step_length_ms", [1000, 100, 10])
@pytest.mark.parametrize("steps", [0, 10, 100])
@pytest.mark.asyncio
async def test_telemetry_stream_reports_steps(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
    steps: int,
    step_length_ms: int,
) -> None:
    """Ensures that telemetry stream reports every step and simulation time in order"""
    create_run_response = await create_run_factory(
        CreateRunParams(step_length_ms=step_length_ms)
    )
    run = service.runs[create_run_response.run_id]

    stream_telemetry_request = StreamRequest(run_id=create_run_response.run_id)

    telemetry_stream = service.StreamTelemetry(
        stream_telemetry_request, fake_context
    )

    run_request = RunRequest(run_id=create_run_response.run_id, steps=steps)

    run_response = await service.Run(run_request, fake_context)

    close_run_request = CloseRunRequest(run_id=create_run_response.run_id)

    await service.CloseRun(close_run_request, fake_context)

    expected_step = 1
    expected_time = run.seconds_per_step

    async for frame in telemetry_stream:
        assert frame.run_id == create_run_response.run_id
        assert frame.step == expected_step
        assert frame.sim_time_s - expected_time == pytest.approx(0)

        expected_step += 1
        expected_time += run.seconds_per_step

    assert expected_step - 1 == run_response.new_step
    assert expected_time == pytest.approx(
        run_response.new_time + run.seconds_per_step
    )


@pytest.mark.asyncio
async def test_subscription_stream(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
    action_bundle_factory: ActionBundleFactoryType,
) -> None:
    """Ensure subscription stream transmits expected subscription values
    in order, with the correct timestamps."""
    create_run_response = await create_run_factory(None)

    stream_request = StreamRequest(run_id=create_run_response.run_id)

    subscription_stream = service.StreamSubscriptions(
        stream_request, fake_context
    )

    run_ten_steps_request = RunRequest(
        run_id=create_run_response.run_id, steps=10
    )

    first_run_response = await service.Run(run_ten_steps_request, fake_context)

    subscription_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getDefaultNumber",
        object_id="default",
    )

    await service.Subscribe(subscription_request, fake_context)

    second_run_response = await service.Run(run_ten_steps_request, fake_context)

    first_apply_action_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=action_bundle_factory(DEFAULT_FAKE_ACTIONS),
    )

    await service.ApplyActions(first_apply_action_request, fake_context)

    third_run_response = await service.Run(run_ten_steps_request, fake_context)

    second_apply_action_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=action_bundle_factory(
            [
                GenericSetterType(
                    domain="fake_domain",
                    setter_name="setDefaultNumber",
                    object_id="default",
                    parameters=[
                        Parameter(name="value", value=Value(number_value=0))
                    ],
                )
            ],
        ),
    )

    await service.ApplyActions(second_apply_action_request, fake_context)

    fourth_run_response = await service.Run(run_ten_steps_request, fake_context)

    close_run_request = CloseRunRequest(run_id=create_run_response.run_id)

    await service.CloseRun(close_run_request, fake_context)

    expected_step = 1

    # The subscription is not initiated, so no metrics are expected in the subscription stream
    for _ in range(first_run_response.new_step):
        frame = await anext(subscription_stream)

        assert frame.step == expected_step
        assert len(frame.metrics) == 0

        expected_step += 1

    # Here, the subscription should be established, but apply action has not yet occured,
    # and no value exists in the state, therefore, the subscription should have collected no value
    for _ in range(second_run_response.new_step - first_run_response.new_step):
        frame = await anext(subscription_stream)

        assert frame.step == expected_step
        assert len(frame.metrics) == 1
        assert not frame.metrics[0].has_value

        expected_step += 1

    # By this point, ApplyActions should be applied with the default fake actions
    # The subscription should have collected this with at this time
    for _ in range(third_run_response.new_step - second_run_response.new_step):
        frame = await anext(subscription_stream)

        assert frame.step == expected_step
        assert len(frame.metrics) == 1
        assert frame.metrics[0].has_value
        assert float(frame.metrics[0].value) == pytest.approx(42)

        expected_step += 1

    # By this point, the second ApplyActions should have been applied and the
    # subscription collected value updated accordingly
    for _ in range(fourth_run_response.new_step - third_run_response.new_step):
        frame = await anext(subscription_stream)

        assert frame.step == expected_step
        assert len(frame.metrics) == 1
        assert frame.metrics[0].has_value
        assert float(frame.metrics[0].value) == pytest.approx(0)

        expected_step += 1

    # The run should have been closed by now. This should have sent a poison pill
    # to the stream, and closed it. Calling the iterator should therefore raise StopAsyncIteration
    with pytest.raises(StopAsyncIteration):
        await anext(subscription_stream)


@pytest.mark.asyncio
async def test_subscription_stream_fetch_subscription_with_requires_collect(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
    action_bundle_factory: ActionBundleFactoryType,
) -> None:
    """Ensure that calling fetch_subscription with requires_collect updates the internal state
    via a collection, but that this collection is not published to subscribers until the next run step.
    """
    create_run_response = await create_run_factory(None)

    stream_request = StreamRequest(run_id=create_run_response.run_id)

    subscription_stream = service.StreamSubscriptions(
        stream_request, fake_context
    )

    subscription_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getDefaultNumber",
        object_id="default",
    )

    subscription_response = await service.Subscribe(
        subscription_request, fake_context
    )

    run_ten_steps_request = RunRequest(
        run_id=create_run_response.run_id, steps=10
    )

    first_run_response = await service.Run(run_ten_steps_request, fake_context)

    first_apply_action_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=action_bundle_factory(DEFAULT_FAKE_ACTIONS),
    )

    await service.ApplyActions(first_apply_action_request, fake_context)

    fetch_subscription_request = FetchRequest(
        fingerprint=subscription_response.fingerprint, requires_collect=True
    )

    fetch_subscription_response = await service.FetchSubscription(
        fetch_subscription_request, fake_context
    )

    expected_step = 1

    for _ in range(first_run_response.new_step):
        frame = await anext(subscription_stream)

        assert frame.step == expected_step
        assert len(frame.metrics) == 1
        assert not frame.metrics[0].has_value

        expected_step += 1

    # This pattern is required becase of asyncio.shield() in the wait_for function:
    # Without asyncio.shield, a timeout cancels the anext(), which in turn cancels the stream
    # With asyncio.shield, anext() is not cancelled, but stays pending, until frames are added
    # at the next run, causing one frame to be consumed silently.
    # This pattern means that when the wait_for times out, we can still retrieve the awaited
    # frame by awaiting the future.
    await_next = asyncio.ensure_future(anext(subscription_stream))

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(await_next), timeout=0.01)
        # Ensure reasonably likely that the queue is empty. Need to
        # shield to prevent cancellation of the stream on timeout

    second_run_response = await service.Run(run_ten_steps_request, fake_context)

    close_request = CloseRunRequest(run_id=create_run_response.run_id)

    await service.CloseRun(close_request, fake_context)

    frame = await await_next
    for _ in range(
        second_run_response.new_step - first_run_response.new_step - 1
    ):
        assert frame.step == expected_step
        assert len(frame.metrics) == 1
        assert frame.metrics[0].has_value
        assert fetch_subscription_response.fetched.has_value
        assert (
            frame.metrics[0].value == fetch_subscription_response.fetched.value
        )
        assert float(frame.metrics[0].value) == pytest.approx(
            float(fetch_subscription_response.fetched.value)
        )

        expected_step += 1
        frame = await anext(subscription_stream)

    with pytest.raises(StopAsyncIteration):
        await anext(subscription_stream)


@pytest.mark.asyncio
async def test_stream_invalid_run_id(
    service: EngineService, fake_context: ServicerContext
) -> None:
    """Ensure that both streams raise a grpc error when the run_id is invalid"""
    stream_request = StreamRequest(run_id="bahahahhahaha")
    telemetry_stream = service.StreamTelemetry(stream_request, fake_context)
    subscription_stream = service.StreamSubscriptions(
        stream_request, fake_context
    )

    with pytest.raises(GrpcAbort):
        async for frame in telemetry_stream:
            pass

    with pytest.raises(GrpcAbort):
        async for frame in subscription_stream:
            pass


# The following contract is challenging to implement so is deferred for now.
@pytest.mark.xfail
@pytest.mark.asyncio
async def test_ensure_reject_actions_bundles_with_at_least_one_malformed_action(
    action_bundle_factory: ActionBundleFactoryType,
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that if an action is misformed, the entire bundle
    is rejected, without partial application.
    This requires either a 'dry run' or for already applied actions
    to be rolled back to before the change."""
    create_run_response = await create_run_factory(None)

    subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getDefaultString",
    )

    default_string_subscription_response = await service.Subscribe(
        subscribe_request, fake_context
    )

    valid_apply_actions_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=action_bundle_factory(DEFAULT_FAKE_ACTIONS),
    )

    valid_apply_actions_response = await service.ApplyActions(
        valid_apply_actions_request, fake_context
    )

    fetch_default_number_request = FetchRequest(
        fingerprint=default_string_subscription_response.fingerprint,
        requires_collect=True,
    )

    first_fetch_response = await service.FetchSubscription(
        fetch_default_number_request, fake_context
    )

    invalid_apply_actions_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=action_bundle_factory(
            [
                GenericSetterType(
                    "fake_domain",
                    "setDefaultString",
                    "default",
                    [Parameter(name="value", value=Value(string_value="baz"))],
                ),
                GenericSetterType(
                    "fake_domain",
                    "noNegativeNumber",
                    "default_object",
                    [Parameter(name="value", value=Value(number_value=-10))],
                ),
            ],
        ),
    )

    invalid_apply_actions_response = await service.ApplyActions(
        invalid_apply_actions_request, fake_context
    )

    second_fetch_response = await service.FetchSubscription(
        fetch_default_number_request, fake_context
    )

    assert len(valid_apply_actions_response.errors) == 0
    assert first_fetch_response.fetched.has_value
    assert first_fetch_response.fetched.value == "foobar"

    assert len(invalid_apply_actions_response.errors) == 1
    assert second_fetch_response.fetched.has_value
    assert second_fetch_response.fetched.value == "foobar"


@pytest.mark.asyncio
async def test_subscribe_rejects_private_name(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that subscriptions with private / dunder getters are rejected.
    Ensure non-'get' functions are rejected."""
    create_run_response = await create_run_factory(None)

    private_subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="_youShallNotSubscribe",
        object_id="default_object",
    )

    non_get_subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="setSpeed",
        object_id="default_object",
    )

    with pytest.raises(
        GrpcAbort,
        match="Getter names starting with '_' are blocked from being collected.",
    ):
        await service.Subscribe(private_subscribe_request, fake_context)

    with pytest.raises(
        GrpcAbort,
        match="Collection only possible for functions beginning with 'get'.",
    ):
        await service.Subscribe(non_get_subscribe_request, fake_context)


@pytest.mark.xfail
@pytest.mark.asyncio
async def test_thorough_getter_valdation(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Currently, any name starting with 'get' is allowed even though it may not be a
    proper getter. In the future, more thorough getter validation will be required.
    """
    create_run_response = await create_run_factory(None)

    cheeky_user_subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getsetSpeed",
        object_id="default_object",
    )

    with pytest.raises(GrpcAbort):
        await service.Subscribe(cheeky_user_subscribe_request, fake_context)


@pytest.mark.asyncio
async def test_rejects_duplicate_subscription_fingerprint(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensures new subscriptions with identical fingerprints are rejected.
    As fingerprints as pseudo-hashes of the subscription, this will block repeated
    collections for the same arguments for an object."""
    create_run_response = await create_run_factory(None)

    subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getDefaultString",
    )

    default_string_subscription_response = await service.Subscribe(
        subscribe_request, fake_context
    )

    assert default_string_subscription_response.fingerprint is not None
    with pytest.raises(GrpcAbort, match="ALREADY_EXISTS.*fingerprint"):
        await service.Subscribe(subscribe_request, fake_context)


@pytest.mark.skip(reason="Support for subscription names removed.")
@pytest.mark.asyncio
async def test_rejects_duplicate_subscription_name(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensures new subscriptions are rejected if their optional
    name (user-friendly) matches an existing subscription name."""
    create_run_response = await create_run_factory(None)

    mysub_request_1 = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getDefaultString",
        name="mysub",
    )
    mysub_request_2 = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getDefaultNumber",
        name="mysub",
    )

    default_string_subscription_response = await service.Subscribe(
        mysub_request_1, fake_context
    )

    assert default_string_subscription_response.fingerprint is not None
    with pytest.raises(GrpcAbort, match="ALREADY_EXISTS.*name"):
        await service.Subscribe(mysub_request_2, fake_context)


@pytest.mark.xfail
@pytest.mark.parametrize("service", [FAKE_SERVICE_CONFIG], indirect=True)
@pytest.mark.asyncio
async def test_unsubscribe_unimplemented(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure calling unsubscribe throws an error.
    When implemented, the fetchedf response should have no value"""
    create_run_response = await create_run_factory(None)

    subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getProgram",
        object_id="fake_object",
    )

    subscribe_response = await service.Subscribe(
        subscribe_request, fake_context
    )

    fetch_request = FetchRequest(
        fingerprint=subscribe_response.fingerprint, requires_collect=True
    )

    fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    assert fetch_response.fetched.has_value
    assert fetch_response.fetched.value == "off"

    unsubscribe_request = UnsubscribeRequest(run_id=create_run_response.run_id)

    with pytest.raises(GrpcAbort, match="UNIMPLEMENTED"):
        await service.Unsubscribe(unsubscribe_request, fake_context)

    with pytest.raises(GrpcAbort):
        fetch_response = await service.FetchSubscription(
            fetch_request, fake_context
        )

    fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    assert not fetch_response.fetched.has_value


@dataclass
class InterruptSession:
    service: EngineService
    fake_context: ServicerContext
    stream: AsyncIterator[InterruptEvent]
    event_callback: Callable[
        [InterruptEvent], tuple[ActionBundle | None, InterruptMetric | None]
    ] = lambda _: (None, None)

    async def handle_interrupt(
        self,
        # new_conditions: InterruptMetric | None = None,
        # actions: ActionBundle | None = None,
        # ) -> ApplyActionsResponse:
    ) -> None:
        while True:
            interrupt_event = await anext(self.stream)

            actions, new_conditions = self.event_callback(interrupt_event)

            # if new_conditions is None or actions is None:
            #     new_actions, new_new_conditions = self.event_callback(interrupt_event)
            #     if actions is None:
            #         actions = new_actions
            #     if new_conditions is None:
            #         new_conditions = new_new_conditions

            interrupt_acknowledge_request = AcknowledgeInterruptRequest(
                run_id=interrupt_event.run_id,
                interrupt_id=interrupt_event.interrupt_id,
                event_id=interrupt_event.event_id,
                actions=actions,
                new_interrupt_conditions=(
                    MetricNameAndValue(**new_conditions)
                    if new_conditions is not None
                    else None
                ),
            )

            await self.service.AcknowledgeInterrupt(
                interrupt_acknowledge_request, self.fake_context
            )


class InterruptMetric(TypedDict, total=False):
    subscription_fingerprint: str
    """If not provided, a subscription will be created"""
    value: Value
    op: Operation.ValueType


SetupInterruptFactoryType = Callable[
    [str, InterruptMetric | None], Awaitable[InterruptSession]
]


class SubscribeParameters(TypedDict):
    domain: str
    getter_name: str
    object_id: str
    parameters: list[Parameter] | None


DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT: SubscribeParameters = {
    "domain": "fake_domain",
    "getter_name": "getInterruptMetric",
    "object_id": "default",
    "parameters": None,
}


@pytest.fixture
def setup_interrupt(
    service: EngineService,
    fake_context: ServicerContext,
) -> SetupInterruptFactoryType:
    """Factory to setup interrupt testing.

    Default interrupt trigger on:

        subscription: fake_domain.getInterruptMetric.default
        value: number_value 10
        operation: GEQ

    Can optionally provide overrides for any/all trigger parameters.

    If the name is not overriden, then a subscription is automatically registered
    for the default name"""

    async def _exec(
        run_id: str,
        override_metric: InterruptMetric | None = None,
    ) -> InterruptSession:
        name: str
        override_metric = override_metric or {}
        if "subscription_fingerprint" in override_metric:
            name = override_metric["subscription_fingerprint"]
        else:
            subscribe_request = SubscribeRequest(
                run_id=run_id,
                **DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT,
            )
            subscription_response = await service.Subscribe(
                subscribe_request, fake_context
            )
            name = subscription_response.fingerprint

        value = override_metric.get("value", Value(number_value=10))
        op = override_metric.get("op", Operation.GEQ)

        metric = InterruptMetric(
            subscription_fingerprint=name, value=value, op=op
        )

        register_interrupt_request = RegisterInterruptRequest(
            run_id=run_id,
            trigger_metric=MetricNameAndValue(**metric),
        )

        return InterruptSession(
            service,
            fake_context,
            service.RegisterInterrupt(register_interrupt_request, fake_context),
        )

    return _exec


@pytest.mark.asyncio
async def test_register_interrupt_rejects_invalid_run_id(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    await create_run_factory(None)

    register_interrupt_request = RegisterInterruptRequest(
        run_id="boo",
        trigger_metric=MetricNameAndValue(
            subscription_fingerprint="fake",
            value=Value(number_value=1),
            op=Operation.GEQ,
        ),
    )

    interrupt_stream = service.RegisterInterrupt(
        register_interrupt_request, fake_context
    )

    with pytest.raises(GrpcAbort, match="run.*not found."):
        await anext(interrupt_stream)


@pytest.mark.asyncio
async def test_register_interrupt_rejects_invalid_subscription(
    service: EngineService,
    fake_context: ServicerContext,
    setup_interrupt: SetupInterruptFactoryType,
    action_bundle_factory: ActionBundleFactoryType,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that interrupts registered with non-existing subscriptions
    do not fail silently. Because Registering an interrupt is not unary-unary,
    the only way to see an exception occured is by await the stream. The first 
    element in this case should be the exception raised by the subscription issue."""
    create_run_response = await create_run_factory(None)

    interrupt_session = await setup_interrupt(
        create_run_response.run_id,
        InterruptMetric(subscription_fingerprint="non-existing subscription"),
    )

    interrupt_session.event_callback = lambda _: (
        action_bundle_factory(
            [
                GenericSetterType(
                    "fake_domain",
                    "setInterruptMetric",
                    "default_object",
                    [Parameter(name="value", value=Value(number_value=10))],
                )
            ],
        ),
        None,
    )

    handler_task = asyncio.create_task(interrupt_session.handle_interrupt())

    await service.Run(
        RunRequest(run_id=create_run_response.run_id, steps=10),
        fake_context,
    )

    with pytest.raises(
        GrpcAbort,
        match="FAILED_PRECONDITION.*The interrupt metric must first be subscribed to.",
    ):
        await handler_task


@pytest.mark.parametrize("service", [FAKE_SERVICE_CONFIG], indirect=True)
@pytest.mark.asyncio
async def test_register_interrupt_autocancel(
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
    action_bundle_factory: ActionBundleFactoryType,
    setup_interrupt: SetupInterruptFactoryType,
) -> None:
    """Ensure interrupt is triggered on run after apply action which matches its
    trigger condition. Ensure it applies actions on acknowledge. Ensure deregistration
    when no new trigger condition provided.

    Unfortunately, this is a contrived example where we need to check that the interrupt event
    queue is empty, without peeking the next item yielded by the AsyncIterator. We therefore cannot
    use the interrupt_session handler in this case."""
    create_run_response = await create_run_factory(None)

    subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="fake_domain",
        getter_name="getProgram",
        object_id="fake_object",
    )

    subscribe_response = await service.Subscribe(
        subscribe_request, fake_context
    )

    interrupt_session = await setup_interrupt(
        create_run_response.run_id,
        InterruptMetric(
            subscription_fingerprint=subscribe_response.fingerprint,
            value=Value(string_value="on"),
            op=Operation.EQU,
        ),
    )

    run_ten_steps_request = RunRequest(
        run_id=create_run_response.run_id, steps=10
    )

    await service.Run(run_ten_steps_request, fake_context)

    apply_action_request = ApplyActionsRequest(
        run_id=create_run_response.run_id,
        action_bundle=action_bundle_factory(
            [
                GenericSetterType(
                    "fake_domain",
                    "setProgram",
                    "fake_object",
                    [Parameter(name="value", value=Value(string_value="on"))],
                )
            ],
        ),
    )

    apply_action_response = await service.ApplyActions(
        apply_action_request, fake_context
    )

    assert len(apply_action_response.errors) == 0

    fetch_request = FetchRequest(
        fingerprint=subscribe_response.fingerprint, requires_collect=True
    )

    first_fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    assert first_fetch_response.fetched.has_value
    assert first_fetch_response.fetched.value == "on"

    next_interrupt_event = asyncio.ensure_future(
        anext(interrupt_session.stream)
    )

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(next_interrupt_event), 0.05)

    run_task_during_interrupt = service.Run(run_ten_steps_request, fake_context)

    async def handle_interrupt() -> None:
        interrupt_event = await next_interrupt_event

        interrupt_acknowledge_request = AcknowledgeInterruptRequest(
            run_id=create_run_response.run_id,
            interrupt_id=interrupt_event.interrupt_id,
            event_id=interrupt_event.event_id,
            actions=action_bundle_factory(
                [
                    GenericSetterType(
                        "fake_domain",
                        "setProgram",
                        "fake_object",
                        [
                            Parameter(
                                name="value", value=Value(string_value="off")
                            )
                        ],
                    )
                ],
            ),
        )

        await service.AcknowledgeInterrupt(
            interrupt_acknowledge_request, fake_context
        )

    await asyncio.gather(run_task_during_interrupt, handle_interrupt())

    fetch_response = await service.FetchSubscription(
        fetch_request, fake_context
    )

    assert fetch_response.fetched.has_value
    assert fetch_response.fetched.value == "off"

    with pytest.raises(StopAsyncIteration):
        await anext(interrupt_session.stream)



def should_trigger(op: Operation.ValueType, i: int, trigger: int) -> bool:
    if op == Operation.EQU:
        return i == trigger
    elif op == Operation.NEQ:
        return i != trigger
    elif op == Operation.GEQ:
        return i >= trigger
    elif op == Operation.LEQ:
        return i <= trigger
    elif op == Operation.GRT:
        return i > trigger
    elif op == Operation.LST:
        return i < trigger
    else:
        raise ValueError(f"Unsupported operation: {op}")

@pytest.mark.parametrize(
    "op",
    [
        Operation.EQU,
        Operation.NEQ,
        Operation.GEQ,
        Operation.LEQ,
        Operation.GRT,
        Operation.LST,
    ],
)
@pytest.mark.parametrize(
    "service",
    [
        {
            "kind": "fake",
            "initial_state": {
                FakeStateDictKey(
                    domain=DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT[
                        "domain"
                    ],
                    object_id=DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT[
                        "object_id"
                    ],
                    attribute="InterruptMetric",
                ): Value(number_value=9)
            },
        }
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_all_interrupt_triggers(
    service: EngineService,
    fake_context: ServicerContext,
    setup_interrupt: SetupInterruptFactoryType,
    create_run_factory: CreateRunFactoryType,
    action_bundle_factory: ActionBundleFactoryType,
    op: Operation.ValueType,
) -> None:
    INTERRUPT_TRIGGER = 10

    create_run_response = await create_run_factory(None)

    interrupt_trigger_subscribe_request = SubscribeRequest(
        **DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT,
        run_id=create_run_response.run_id,
    )

    interrupt_trigger_subscribe_response = await service.Subscribe(
        interrupt_trigger_subscribe_request, fake_context
    )

    interrupt_report_subscribe_request = SubscribeRequest(
        domain=DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT["domain"],
        object_id=DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT["object_id"],
        run_id=create_run_response.run_id,
        getter_name="getInterruptReport",
    )

    interrupt_report_subscribe_response = await service.Subscribe(
        interrupt_report_subscribe_request, fake_context
    )

    interrupt_metric = InterruptMetric(
        subscription_fingerprint=interrupt_trigger_subscribe_response.fingerprint,
        value=Value(number_value=INTERRUPT_TRIGGER),
        op=op,
    )

    interrupt_session = await setup_interrupt(
        create_run_response.run_id, interrupt_metric
    )

    fetch_interrupt_report_value_request = FetchRequest(
        fingerprint=interrupt_report_subscribe_response.fingerprint,
        requires_collect=True,
    )

    fetch_interrupt_trigger_value_request = FetchRequest(
        fingerprint=interrupt_trigger_subscribe_response.fingerprint,
        # requires_collect=True,
    )

    run_nine_steps_request = RunRequest(
        run_id=create_run_response.run_id, steps=9
    )

    run_response = await service.Run(run_nine_steps_request, fake_context)

    assert run_response.new_step == 9

    run_one_step_request = RunRequest(
        run_id=create_run_response.run_id, steps=1
    )

    handler_task = asyncio.create_task(interrupt_session.handle_interrupt())

    for i in range(9, 12):
        interrupt_session.event_callback = lambda _: (
            action_bundle_factory(
                [
                    GenericSetterType(
                        domain=DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT[
                            "domain"
                        ],
                        setter_name="setInterruptReport",
                        object_id=DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT[
                            "object_id"
                        ],
                        parameters=[
                            Parameter(name="value", value=Value(number_value=i))
                        ],
                    )
                ]
            ),
            interrupt_metric,
        )

        run_response = await service.Run(run_one_step_request, fake_context)

        assert run_response.new_step == i + 1
        # assert len(apply_interrupt_actions_response.errors) == 0

        fetched_interrupt_trigger_value = (
            await service.FetchSubscription(
                fetch_interrupt_trigger_value_request, fake_context
            )
        ).fetched

        assert fetched_interrupt_trigger_value.has_value
        assert float(fetched_interrupt_trigger_value.value) == float(i)

        tick_interrupt_trigger_metric = await service.ApplyActions(
            ApplyActionsRequest(
                run_id=create_run_response.run_id,
                action_bundle=action_bundle_factory(
                    [
                        GenericSetterType(
                            domain=DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT[
                                "domain"
                            ],
                            object_id=DEFAULT_INTERRUPT_SUBSCRIBE_DOMAIN_GETTER_OBJECT[
                                "object_id"
                            ],
                            setter_name="setInterruptMetric",
                            parameters=[
                                Parameter(
                                    name="value",
                                    value=Value(
                                        number_value=float(
                                            fetched_interrupt_trigger_value.value
                                        )
                                        + 1
                                    ),
                                )
                            ],
                        )
                    ]
                ),
            ),
            fake_context,
        )

        assert len(tick_interrupt_trigger_metric.errors) == 0

        fetched_interrupt_report_value = (
            await service.FetchSubscription(
                fetch_interrupt_report_value_request, fake_context
            )
        ).fetched


        if should_trigger(op, i, INTERRUPT_TRIGGER):
            assert fetched_interrupt_report_value.has_value
            assert float(
                fetched_interrupt_report_value.value
            ) == pytest.approx(float(i))
        else:
            if fetched_interrupt_report_value.has_value:
                assert float(
                    fetched_interrupt_report_value.value
                ) != pytest.approx(float(i))

        
    handler_task.cancel()
    with suppress(asyncio.CancelledError):
        await handler_task


# #Fails correctly with fetch before subscription
# @pytest.mark.asyncio
# async def test_fetch_subscription_fails_before_subscription() -> None:
#     pass


## ENSURE ALL FACTORIES CALL SUPER INIT

# interrupt acknowledge type melding
