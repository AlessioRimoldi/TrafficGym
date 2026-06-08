from .registry import (
    register,
    TransformationSpec,
    InputSpec,
    OutputSpec,
    Runtime,
    InputType,
)

import subprocess
import uuid
import asyncio
import os

async def random_routes_handler(inputs: dict[str, str], runtime: Runtime) -> dict[str, str]:
    net_xml = inputs["net_xml"]
    end_step = inputs.get("end_step", "3600")
    if end_step == "":
        end_step = "3600"
    output_path = runtime.base_path / f"routes_{uuid.uuid4().hex[:8]}.rou.xml"
    sumo_home = os.environ["SUMO_HOME"]
    
    cmd = [
        "python",
        f"{sumo_home}/tools/randomTrips.py",
        "-n", net_xml,
        "-o", str(output_path),
        "--end", end_step,
    ]

    await asyncio.to_thread(
        subprocess.run,
        cmd,
        capture_output=True,
        text=True,
        check=True,
    )

    return {"rou_xml": str(output_path)}

register(
    TransformationSpec(
        key="randomTrips",
        inputs=[
            InputSpec("net_xml", InputType.FILE, True),
            InputSpec("end_step", InputType.JSON, False)
        ],
        outputs=[OutputSpec("rou_xml")],
        handler=random_routes_handler,
        docstring="Transform and osm_xml file into a SUMO .net.xml file using netconvert.",
    )
)
