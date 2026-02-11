import pytest
import pytest_asyncio

# import libsumo
from trafficgym.engine.server import EngineService
from trafficgym.engine.client import *
from trafficgym.api.engine_pb2 import (
    CreateRunRequest,
    CreateRunResponse,
    RunRequest,
)
from grpc import ServicerContext, StatusCode
from typing import Literal, TypedDict, Protocol, Callable, cast, Awaitable
from typing_extensions import Never

# class MockServicerContext(ServicerContext):


class SupportsAbort(Protocol):
    def abort(self, code: StatusCode, details: str) -> Never: ...


class CreateRunParams(TypedDict, total=False):
    sumocfg_path: str
    sumo_binary: Literal["sumo", "sumo-gui"]
    step_length_ms: int


class GrpcAbort(Exception):
    def __init__(self, code: StatusCode, details: str):
        self.code = code
        self.details = details
        super().__init__(f"{code.name}: {details}")


class FakeContext:
    def abort(self, code: StatusCode, details: str) -> Never:
        raise GrpcAbort(code, details)


@pytest.fixture
def service() -> EngineService:
    return EngineService()


@pytest.fixture
def context() -> SupportsAbort:
    return FakeContext()


@pytest_asyncio.fixture
async def create_run_factory(
    service: EngineService,
    context: SupportsAbort,
) -> Callable[[CreateRunParams | None], Awaitable[CreateRunResponse]]:
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
            request=request, context=cast(ServicerContext, context)
        )

        return response

    return _exec


@pytest.mark.parametrize(
    "override",
    [
        {"sumocfg_path": "/dev/null"},
        {"step_length_ms": -1},
        {"step_length_ms": 0},
    ],
)
@pytest.mark.asyncio
async def test_create_run_fails_invalid_sumo_parameters(
    override: CreateRunParams,
    service: EngineService,
    create_run_factory: Callable[[CreateRunParams], Awaitable[CreateRunResponse]],
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
async def test_run_after_create(
    service: EngineService,
    context: SupportsAbort,
    create_run_factory: Callable[[CreateRunParams], Awaitable[CreateRunResponse]],
) -> None:
    RUN_TIME_S = 10
    RUN_STEP = 10
    STEP_LENGTH_MS = 1000

    context = cast(ServicerContext, context)

    response = await create_run_factory({"step_length_ms": STEP_LENGTH_MS})

    run_request = RunRequest(run_id=response.run_id, max_time=RUN_TIME_S)

    first_step_response = await service.Run(
        request=run_request, context=context
    )

    assert first_step_response.new_time == RUN_TIME_S
    assert first_step_response.new_step == RUN_TIME_S * 1000 // STEP_LENGTH_MS

    run_request = RunRequest(run_id=response.run_id, max_steps=RUN_STEP)

    second_step_response = await service.Run(
        request=run_request, context=context
    )

    assert (
        second_step_response.new_time
        == first_step_response.new_time + RUN_STEP * STEP_LENGTH_MS / 1000
    )
    assert (
        second_step_response.new_step == first_step_response.new_step + RUN_STEP
    )

    # run_request = RunRequest(
    #     run_id=created_run.response.run_id,
    #     max_steps=RUN_STEP,
    #     max_time=RUN_TIME_S,
    # )

    # with pytest.raises(grpc.RpcError):
    #     await created_run.service.Run(
    #         request=run_request,
    #         context=created_run.context
    #     )
