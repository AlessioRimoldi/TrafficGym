from trafficgym.api.engine_pb2 import CustomValue

ExtractedValueType = int | float | str | bool | None


def extract_value(value: CustomValue) -> ExtractedValueType:
    kind = value.WhichOneof("kind")
    if kind == "null_value":
        return None
    elif kind == "int_value":
        return value.int_value
    elif kind == "float_value":
        return value.float_value
    elif kind == "string_value":
        return value.string_value
    elif kind == "bool_value":
        return value.bool_value

    raise TypeError(f"Unsupported Value kind: {kind}")
