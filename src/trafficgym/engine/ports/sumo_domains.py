"""SUMO getter / setter definitions used by the UI and codegen.

Add new getters or setters here only — both the graph builder and the code
generator derive their type/key information from this module.
"""

from __future__ import annotations

from typing import TypedDict


class GetterDef(TypedDict):
    getter: str
    type: str
    output_key: str


class SetterDef(TypedDict):
    setter: str
    sumo_param: str   # SUMO TraCI API parameter name (used by codegen)
    type: str
    input_key: str    # semantic variable name for port compatibility


class ObserverDomain(TypedDict):
    domain: str
    getters: list[GetterDef]


class ActuatorDomain(TypedDict):
    domain: str
    setters: list[SetterDef]


OBSERVER_DOMAINS: dict[str, ObserverDomain] = {
    "detector": {
        "domain": "inductionloop",
        "getters": [
            {"getter": "getLastIntervalOccupancy",  "type": "float",     "output_key": "occupancy"},
            {"getter": "getLastStepOccupancy",      "type": "float",     "output_key": "occupancy"},
            {"getter": "getLastStepVehicleNumber",  "type": "int",       "output_key": "vehicle_count"},
        ],
    },
    "tls": {
        "domain": "trafficlight",
        "getters": [
            {"getter": "getRedYellowGreenState",    "type": "str",       "output_key": "state"},
            {"getter": "getSpentDuration",          "type": "float",     "output_key": "duration"},
            {"getter": "getPhase",                  "type": "int",       "output_key": "phase"},
        ],
    },
    "edge": {
        "domain": "edge",
        "getters": [
            {"getter": "getLastStepVehicleIDs",     "type": "list[str]", "output_key": "vehicle_ids"},
            {"getter": "getLastStepMeanSpeed",      "type": "float",     "output_key": "mean_speed"},
            {"getter": "getLastStepOccupancy",      "type": "float",     "output_key": "occupancy"},
        ],
    },
    "lane": {
        "domain": "lane",
        "getters": [
            {"getter": "getLastStepOccupancy",      "type": "float",     "output_key": "occupancy"},
            {"getter": "getLastStepVehicleNumber",  "type": "int",       "output_key": "vehicle_count"},
            {"getter": "getLastStepMeanSpeed",      "type": "float",     "output_key": "mean_speed"},
        ],
    },
}

ACTUATOR_DOMAINS: dict[str, ActuatorDomain] = {
    "tls": {
        "domain": "trafficlight",
        "setters": [
            {"setter": "setProgram",             "sumo_param": "programID", "type": "str", "input_key": "program_id"},
            {"setter": "setRedYellowGreenState", "sumo_param": "state",     "type": "str", "input_key": "state"},
            {"setter": "setPhase",               "sumo_param": "index",     "type": "int", "input_key": "phase"},
        ],
    },
}

# Flat lookup dicts derived from the above — used by codegen.
GETTER_CAST: dict[str, str] = {
    g["getter"]: g["type"]
    for obs in OBSERVER_DOMAINS.values()
    for g in obs["getters"]
}

SETTER_PARAM_KEY: dict[str, str] = {
    s["setter"]: s["sumo_param"]
    for act in ACTUATOR_DOMAINS.values()
    for s in act["setters"]
}
