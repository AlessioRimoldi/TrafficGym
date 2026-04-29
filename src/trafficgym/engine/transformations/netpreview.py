from .registry import (
    register,
    TransformationSpec,
    InputSpec,
    OutputSpec,
    Runtime,
    InputType,
)

async def netpreview_handler(
    inputs: dict[str, str], runtime: Runtime
) -> dict[str, str]:
    import uuid
    import asyncio
    from pathlib import Path
    import matplotlib.pyplot as plt
    import SumoNetVis  # type: ignore

    net_xml_path = Path(inputs["net_xml"])
    output_path = runtime.base_path / f"net_preview_{uuid.uuid4().hex[:8]}.png"

    def render() -> None:
        net = SumoNetVis.Net(str(net_xml_path))
        _, ax = plt.subplots()
        net.plot(ax=ax)
        ax.set_aspect("equal")
        ax.axis("off")
        plt.savefig(output_path, dpi=500, bbox_inches="tight")
        plt.close()

    await asyncio.to_thread(render)

    return {"preview": str(output_path)}


register(
    TransformationSpec(
        key="netpreview",
        inputs=[
            InputSpec("net_xml", InputType.FILE, True),
            # InputSpec("parameters", InputType.JSON, False),
        ],
        outputs=[OutputSpec("preview")],
        handler=netpreview_handler,
        docstring="Transform a SUMO network into a .jpg preview of it using Matplotlib.",
    )
)
