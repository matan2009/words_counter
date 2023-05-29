from enum import Enum


class ResponseStatus(Enum):
    Ok = 0
    Partial = 1
    Error = 2
