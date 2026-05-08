from dataclasses import dataclass


class SDKError(Exception): ...


class InvalidArgumentError(SDKError): ...


class NotFoundError(SDKError): ...


@dataclass(frozen=True)
class AbortedError(SDKError):
    message: str
    server_traceback: str | None
    error_type: str


class ServiceUnavailableError(SDKError): ...


class GrpcError(SDKError): ...


