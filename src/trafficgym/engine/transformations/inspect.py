import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path

from .registry import InputSpec, InputType, OutputSpec, Runtime, TransformationSpec, register


async def handler(inputs: dict[str, str], runtime: Runtime) -> dict[str, str]:
    net_bytes = open(inputs["net_xml"], "rb").read()
    digest = hashlib.sha256(net_bytes).hexdigest()[:8]

    net_root = ET.fromstring(net_bytes)

    tls_ids = list(dict.fromkeys(
        id_ for el in net_root.iter("tlLogic") if (id_ := el.get("id"))
    ))

    edge_ids = [
        id_
        for el in net_root.iter("edge")
        if el.get("function") != "internal"
        if (id_ := el.get("id"))
    ]

    lane_ids = [
        id_
        for edge in net_root.iter("edge")
        if edge.get("function") != "internal"
        for el in edge.iter("lane")
        if (id_ := el.get("id"))
    ]

    detector_ids: list[str] = []
    if "add_xml" in inputs:
        add_root = ET.fromstring(open(inputs["add_xml"], "rb").read())
        detector_ids = [
            id_
            for el in add_root.iter()
            if el.tag in ("e1Detector", "inductionLoop")
            if (id_ := el.get("id"))
        ]

    output_path = runtime.base_path / f"inspection_{digest}.json"
    output_path.write_text(json.dumps({
        "tls_ids": tls_ids,
        "edge_ids": edge_ids,
        "lane_ids": lane_ids,
        "detector_ids": detector_ids,
    }))

    return {"inspection": str(output_path)}


register(
    TransformationSpec(
        key="inspect",
        inputs=[
            InputSpec("net_xml", InputType.FILE, required=True),
            InputSpec("add_xml", InputType.FILE, required=False),
        ],
        outputs=[OutputSpec("inspection")],
        handler=handler,
        docstring="Parse .net.xml and optionally .add.xml to enumerate TLS IDs, edges, lanes, and detectors.",
    )
)
