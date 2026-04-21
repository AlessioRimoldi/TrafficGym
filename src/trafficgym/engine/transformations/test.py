from .registry import (
    register,
    TransformationSpec,
    InputSpec,
    OutputSpec,
    Runtime,
    InputType,
)


async def test_handler(
    inputs: dict[str, str], runtime: Runtime
) -> dict[str, str]:
    return {"nothing at all": "nothing"}


register(
    TransformationSpec(
        key="nothing",
        inputs=[
            InputSpec("yo", InputType.FILE, True),
            InputSpec("hi", InputType.FILE, False),
            InputSpec("foo", InputType.JSON, True),
            InputSpec("bar", InputType.JSON, False),
        ],
        outputs=[OutputSpec("nothing at all")],
        handler=test_handler,
        docstring="Does litterally nothing",
    )
)
