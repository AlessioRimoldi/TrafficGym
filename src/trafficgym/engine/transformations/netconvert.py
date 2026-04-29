from .registry import (
    register,
    TransformationSpec,
    InputSpec,
    OutputSpec,
    Runtime,
    InputType,
)
from typing import cast
from pathlib import Path

import subprocess
import uuid
import asyncio


async def netconvert_handler(
    inputs: dict[str, str], runtime: Runtime
) -> dict[str, str]:
    breakpoint()
    osm_xml = inputs["osm_xml"]

    output_path = (
        runtime.base_path / f"osm_converted_{uuid.uuid4().hex[:8]}.net.xml"
    )

    cmd = [
        "netconvert",
        "--osm",
        osm_xml,
        "-o",
        str(output_path),
    ]

    await asyncio.to_thread(
        subprocess.run,
        cmd,
        capture_output=True,
        text=True,
        check=True,
    )

    return {"network": str(output_path)}


register(
    TransformationSpec(
        key="netconvert",
        inputs=[
            InputSpec("osm_xml", InputType.FILE, True),
            # InputSpec("parameters", InputType.JSON, False),
        ],
        outputs=[OutputSpec("network")],
        handler=netconvert_handler,
        docstring="Transform and osm_xml file into a SUMO .net.xml file using netconvert.",
    )
)
