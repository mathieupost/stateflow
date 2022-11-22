from typing import Optional, Dict
from enum import Enum
from stateflow.dataflow.address import FunctionAddress


class _Request(Enum):
    InvokeStateless = "InvokeStateless"
    InvokeStateful = "InvokeStateful"
    InitClass = "InitClass"

    FindClass = "FindClass"

    GetState = "GetState"
    SetState = "SetState"
    UpdateState = "UpdateState"
    DeleteState = "DeleteState"
    PrepareState = "PrepareState"
    IsPrepared = "IsPrepared"
    CommitState = "CommitState"

    EventFlow = "EventFlow"
    DeadlockCheck = "DeadlockCheck"

    Ping = "Ping"

    def __str__(self):
        return f"Request.{self.value}"


class _Reply(Enum):
    SuccessfulInvocation = "SuccessfulInvocation"
    SuccessfulCreateClass = "SuccessfulCreateClass"

    FoundClass = "FoundClass"
    KeyNotFound = "KeyNotFound"

    SuccessfulStateRequest = "SuccessfulStateRequest"
    FailedInvocation = "FailedInvocation"

    Pong = "Pong"

    def __str__(self):
        return f"Reply.{self.value}"


class EventType:
    Request = _Request
    Reply = _Reply

    @staticmethod
    def from_str(input_str: str) -> Optional["EventType"]:
        if input_str in EventType.Request._member_names_:
            return EventType.Request[input_str]
        elif input_str in EventType.Reply._member_names_:
            return EventType.Reply[input_str]
        else:
            return None


class Event:
    from stateflow.dataflow.args import Arguments

    __slots__ = "event_id", "fun_address", "event_type", "payload"

    def __init__(
        self,
        event_id: str,
        fun_address: FunctionAddress,
        event_type: EventType,
        payload: Dict,
    ):
        self.event_id: str = event_id
        self.fun_address: FunctionAddress = fun_address
        self.event_type: EventType = event_type
        self.payload: Dict = payload

    def get_arguments(self) -> Optional[Arguments]:
        if "args" in self.payload:
            return self.payload["args"]
        else:
            return None

    def copy(self, **kwargs) -> "Event":
        new_args = {}
        for key, value in kwargs.items():
            if key in self.__slots__:
                new_args[key] = value

        for key in self.__slots__:
            if key not in new_args:
                new_args[key] = getattr(self, key)

        return Event(**new_args)
