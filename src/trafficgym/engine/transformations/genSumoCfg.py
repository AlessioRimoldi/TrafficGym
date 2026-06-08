from .registry import (
    register,
    TransformationSpec,
    InputSpec,
    OutputSpec,
    Runtime,
    InputType,
)
import uuid
import asyncio
from pathlib import Path


async def sumocfg_handler(inputs: dict[str, str], runtime: Runtime) -> dict[str, str]:
    net_xml = inputs["net_xml"]
    rou_xml = inputs["rou_xml"]
    add_xml = inputs.get("add_xml")
    # step_length = inputs.get("step_length", "0.01")
    # if step_length == "":
    #     step_length = "0.01"

    net_name = Path(net_xml).name
    rou_name = Path(rou_xml).name

    additional_line = f'\n        <additional-files value="{Path(add_xml).name}"/>' if add_xml else ""

    output_path = runtime.base_path / f"simulation_{uuid.uuid4().hex[:8]}.sumocfg"

    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<sumoConfiguration xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xsi:noNamespaceSchemaLocation="http://sumo.dlr.de/xsd/sumoConfiguration.xsd">
    <input>
        <net-file value="{net_name}"/>
        <route-files value="{rou_name}"/>{additional_line}
    </input>
    <time>
        <step-length value="0.01"/>
    </time>
</sumoConfiguration>"""

    await asyncio.to_thread(output_path.write_text, content, "utf-8")
    return {"sumocfg": str(output_path)}


register(
    TransformationSpec(
        key="sumocfg",
        inputs=[
            InputSpec("net_xml", InputType.FILE, True),
            InputSpec("rou_xml", InputType.FILE, True),
            InputSpec("add_xml", InputType.FILE, False),
            # InputSpec("step_length", InputType.JSON, False),
        ],
        outputs=[OutputSpec("sumocfg")],
        handler=sumocfg_handler,
        docstring="Generate a SUMO .sumocfg configuration file from a network and route file, with an optional additional-files entry and configurable step length.",
    )
)