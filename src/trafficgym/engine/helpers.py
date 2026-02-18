from google.protobuf.struct_pb2 import Value

ExtractedValueType = float | str | bool | None


def extract_value(value: Value) -> ExtractedValueType:
    kind = value.WhichOneof("kind")
    if kind == "null_value":
        return None
    elif kind == "number_value":
        return value.number_value
    elif kind == "string_value":
        return value.string_value
    elif kind == "bool_value":
        return value.bool_value

    raise TypeError(f"Unsupported Value kind: {kind}")
