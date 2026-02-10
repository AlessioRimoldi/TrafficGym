import pytest
import pytest_asyncio
import libsumo
from src.trafficgym.engine.server import EngineService
from src.trafficgym.engine.client import *
from src.trafficgym.api.engine_pb2 import (
    CreateRunRequest,
    CreateRunResponse,
    RunRequest,
    RunResponse,
)
from grpc import ServicerContext
from unittest.mock import MagicMock
from dataclasses import dataclass
from typing import Literal, TypedDict

# class MockServicerContext(ServicerContext):


@dataclass
class CreatedRunHandle:
    response: engine_pb2.CreateRunResponse
    service: EngineService
    context: ServicerContext


class CreateRunParams(TypedDict, total=False):
    sumocfg_path: str
    sumo_binary: Literal["sumo", "sumo-gui"]
    step_length_ms: int


@pytest.fixture
def service():
    return EngineService()

# @pytest.MonkeyPatch.setattr
@pytest_asyncio.fixture
async def created_run_factory(
    service: EngineService,
) -> Callable[[CreateRunParams], CreatedRunHandle]:
    async def _create_run(overrides: CreateRunParams = {}) -> CreatedRunHandle:
        defaults: CreateRunParams = dict(
            sumocfg_path=sumocfg_path,
            sumo_binary="sumo",
            step_length_ms=1000,
        )

        param: CreateRunParams = {**defaults, **overrides}

        create_run_request = CreateRunRequest(**param)
        context = MagicMock()
        response = await service.CreateRun(
            request=create_run_request, context=context
        )

        return CreatedRunHandle(
            response=response, service=service, context=context
        )

    return _create_run


@pytest.mark.parametrize(
    "override",
    [
        {"sumocfg_path": "/dev/null"},
        # {"sumo_binary": "zumo"},
        {"step_length_ms": -1},
    ],
)
@pytest.mark.asyncio
async def test_create_run_invalid_sumo_cfg_path(
    override,
    created_run_factory: Callable[[CreateRunParams], CreatedRunHandle],
):
    with pytest.raises(libsumo.TraCIException):
        await created_run_factory(override)


@pytest.mark.asyncio
async def test_run_after_create(created_run_factory):
    RUN_TIME_S = 10
    RUN_STEP = 10
    STEP_LENGTH_MS = 1000

    created_run: CreatedRunHandle = await created_run_factory({"step_length_ms": STEP_LENGTH_MS})

    run_request = RunRequest(
        run_id=created_run.response.run_id, max_time=RUN_TIME_S
    )

    first_step_response = await created_run.service.Run(
        request=run_request, context=created_run.context
    )

    assert first_step_response.new_time == RUN_TIME_S
    assert first_step_response.new_step == RUN_TIME_S * 1000 // STEP_LENGTH_MS

    run_request = RunRequest(
        run_id=created_run.response.run_id, max_steps=RUN_STEP
    )

    second_step_response = await created_run.service.Run(
        request=run_request, context=created_run.context
    )

    assert (
        second_step_response.new_time
        == first_step_response.new_time + RUN_TIME_S * STEP_LENGTH_MS / 1000
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
