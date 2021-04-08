from src.dataflow.dataflow import Operator, Edge, FunctionType, EventType
from src.dataflow.event import Event
from src.dataflow.state import State
from src.wrappers import ClassWrapper, MetaWrapper, InvocationResult
from typing import NewType, List, Tuple, Optional
from src.serialization.json_serde import SerDe, JsonSerializer

NoType = NewType("NoType", None)


class StatefulOperator(Operator):
    def __init__(
        self,
        incoming_edges: List[Edge],
        outgoing_edges: List[Edge],
        function_type: FunctionType,
        class_wrapper: ClassWrapper,
        meta_wrapper: MetaWrapper,
        serializer: SerDe = JsonSerializer(),
    ):
        super().__init__(incoming_edges, outgoing_edges, function_type)
        self.class_wrapper = class_wrapper
        self.meta_wrapper = meta_wrapper
        self.serializer = serializer

    def handle_create(self, event: Event) -> Event:
        res: InvocationResult = self.class_wrapper.init_class(event.payload["args"])

        key: str = res.return_results[0]
        created_state: State = res.updated_state

        event.fun_address.key = key
        event.payload["init_class_state"] = created_state.get()

        return event

    def handle(self, event: Event, state: Optional[bytes]) -> Tuple[Event, bytes]:
        if state is not None:
            state = State(self.serializer.deserialize_dict(state))

        updated_state = None
        return_event = None

        # initialize class
        if event.event_type == EventType.Request.InitClass:
            new_state = event.payload["init_class_state"]

            if state is not None:
                return_event = Event(
                    event.event_id,
                    event.fun_address,
                    EventType.Reply.FailedInvocation,
                    {
                        "error_message": f"{event.fun_address.function_type.get_full_name()} class "
                        f"with key={event.fun_address.key} already exists."
                    },
                )
            else:
                return_event = Event(
                    event.event_id,
                    event.fun_address,
                    EventType.Reply.SuccessfulCreateClass,
                    {"key": f"{event.fun_address.key}"},
                )
                updated_state = State(new_state)

        elif event.event_type == EventType.Request.InvokeStateful:
            pass

        if updated_state is not None:
            return return_event, bytes(self.serializer.serialize_dict(updated_state.get()), 'utf-8')
        return return_event, updated_state
