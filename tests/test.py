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
from trafficgym.engine.adapters.fake_adapter import FakeStateDict
from trafficgym.engine.ports.adapter_factory import AdapterFactory
from google.protobuf.struct_pb2 import Value
from trafficgym.api.engine_pb2 import (
    CreateRunRequest,
    CreateRunResponse,
    RunRequest,
    CloseRunRequest,
    ActionBundle,
    Action,
    GenericSetter,
    Parameter,
    SubscribeRequest,
)
from grpc import ServicerContext, StatusCode
from typing import (
    Literal,
    TypedDict,
    Protocol,
    Callable,
    cast,
    Awaitable,
)
from typing_extensions import Never, NotRequired
from dataclasses import dataclass, asdict


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
        initial_state = params.get("initial_state", {})
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

    response = await create_run_factory({"step_length_ms": STEP_LENGTH_MS})

    run_request = RunRequest(run_id=response.run_id, time=RUN_TIME_S)

    first_step_response = await service.Run(
        request=run_request, context=fake_context
    )

    assert first_step_response.new_time == RUN_TIME_S
    assert first_step_response.new_step == RUN_TIME_S * 1000 // STEP_LENGTH_MS

    run_request = RunRequest(run_id=response.run_id, steps=RUN_STEP)

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


@dataclass
class GenericSetterType:
    domain: str
    setter_name: str
    parameters: list[Parameter] | None = None


ActionFactoryType = Callable[[GenericSetterType], Action]


@pytest.fixture
def action_factory() -> ActionFactoryType:
    def _make_action(gs: GenericSetterType) -> Action:
        action = Action(setter=GenericSetter(**asdict(gs)))
        return action

    return _make_action


ActionBundleFactoryType = Callable[[str, list[Action] | None], ActionBundle]


@pytest.fixture
def action_bundle_factory(
    action_factory: ActionFactoryType,
) -> ActionBundleFactoryType:
    def _make_action_bundle(
        run_id: str, actions: list[Action] | None
    ) -> ActionBundle:
        if actions is None:
            default_actions = [
                GenericSetterType(
                    domain="trafficlight",
                    setter_name="setProgram",
                    parameters=[
                        Parameter(
                            name="tlsID", value=Value(string_value="TL0")
                        ),
                        Parameter(
                            name="programID", value=Value(string_value="10")
                        ),
                    ],
                ),
                GenericSetterType(
                    domain="trafficlight",
                    setter_name="setRedYellowGreenState",
                    parameters=[
                        Parameter(
                            name="tlsID", value=Value(string_value="TL0")
                        ),
                        Parameter(
                            name="state", value=Value(string_value="rGrG")
                        ),
                    ],
                ),
            ]
            actions = [action_factory(gst) for gst in default_actions]

        action_bundle = ActionBundle(run_id=run_id, actions=actions)

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
    apply_actions_request = ActionBundle(run_id=invalid_run_id, actions=[])

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

    apply_actions_request = ActionBundle(
        run_id=create_run_response.run_id, step=10, actions=[]
    )

    with pytest.raises(GrpcAbort) as e:
        await service.ApplyActions(apply_actions_request, fake_context)

    assert e.value.code == StatusCode.UNIMPLEMENTED


@pytest.mark.parametrize(
    "service",
    [
        {"kind": "fake", "intial_state": {"test": 0}},
    ],
    indirect=True,
)
@pytest.mark.asyncio
async def test_apply_actions(
    action_bundle_factory: ActionBundleFactoryType,
    service: EngineService,
    fake_context: ServicerContext,
    create_run_factory: CreateRunFactoryType,
) -> None:
    """Ensure that if an action is misformed, the entire bundle
    is rejected, without partial application"""
    create_run_response = await create_run_factory(None)

    subscribe_request = SubscribeRequest(
        run_id=create_run_response.run_id,
        domain="trafficlight",
        getter_name="getProgramID",
        parameters=[Parameter(name="tlsID", value=Value(string_value="TL0"))],
    )

    await service.Subscribe(subscribe_request, fake_context)

    apply_actions_request = action_bundle_factory(
        create_run_response.run_id, None
    )

    await service.ApplyActions(apply_actions_request, fake_context)


# @pytest.mark.asyncio
# async def test_ensure_malformed_actions_are_rejected(
#     action_bundle_factory: ActionBundleFactoryType,
#     service: EngineService,
#     fake_context: SupportsAbort,
#     create_run_factory: CreateRunFactoryType,
# ) -> None:
#     """Ensure that if an action is misformed, the entire bundle
#     is rejected, without partial application"""
#     create_run_response = await create_run_factory(None)

#     apply_actions_request = action_bundle_factory(
#         create_run_response.run_id, None
#     )


## ENSURE ALL FACTORIES CALL SUPER INIT
