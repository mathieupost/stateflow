from stateflow.dataflow.dataflow import Operator, Edge, FunctionType, EventType
from stateflow.dataflow.address import FunctionAddress
from stateflow.dataflow.event import Event
from stateflow.dataflow.event_flow import (
    ReturnNode,
    EventFlowGraph,
)
from stateflow.dataflow.state import State, Store, Version, WriteSet
from stateflow.wrappers.class_wrapper import (
    ClassWrapper,
    InvocationResult,
    FailedInvocation,
)
from stateflow.wrappers.meta_wrapper import MetaWrapper
from typing import Generator, Iterator, NewType, List, Tuple, Optional
from stateflow.serialization.pickle_serializer import SerDe, PickleSerializer

NoType = NewType("NoType", None)


class StatefulGenerator:
    """Wraps a generator function to retain its return value.
    
    This makes it possible to yield values in a generator function, while also
    storing the final return value for later use."""
    def __init__(self, gen):
        self.gen = gen

    def __iter__(self):
        self.state = yield from self.gen


class StatefulOperator(Operator):
    def __init__(
        self,
        incoming_edges: List[Edge],
        outgoing_edges: List[Edge],
        function_type: FunctionType,
        class_wrapper: ClassWrapper,
        meta_wrapper: MetaWrapper,
        serializer: SerDe = PickleSerializer(),
    ):
        super().__init__(incoming_edges, outgoing_edges, function_type)
        self.class_wrapper = class_wrapper
        self.meta_wrapper = meta_wrapper
        self.serializer = serializer

    def handle_create(self, event: Event) -> Event:
        """Handles a request to create a new class.
        We assume that State does not yet exist for this requested class instance.

        We initialize the class with the 'args' key from the event payload.
        We return a event with the state encoded in the payload["init_class_state"].
        Moreover, we set the key of the function address.

        This event will eventually be propagated to the 'actual' stateful operator,
        where it will call `handle(event, state)` and subsequently `_handle_create_with_state(event, state)`

        :param event: the Request.InitClass event.
        :return: the created instance, embedded in an Event.
        """
        args = event.payload["args"]
        res: InvocationResult = self.class_wrapper.init_class(args)

        print(f"handle_create, {res}")
        key: str = res.return_results[0]
        created_state: State = res.updated_state

        # Set the key and the state of this class.
        event.fun_address.key = key
        event.payload["init_class_state"] = created_state.get()

        # We can get rid of the arguments.
        del event.payload["args"]

        return event

    def _dispatch_event(
        self, event_type: EventType, event: Event, state: State
    ) -> Tuple[Event, State]:
        """Dispatches an event to the correct method to execute/handle it.

        :param event_type: the event_type to find the correct handle.
        :param event: the incoming event.
        :param state: the incoming state.
        :return: a tuple of outgoing event + updated state.
        """

        if event_type == EventType.Request.InvokeStateful:
            return self._handle_invoke_stateful(event, state)
        elif event_type == EventType.Request.GetState:
            return self._handle_get_state(event, state)
        elif event_type == EventType.Request.UpdateState:
            return self._handle_update_state(event, state)
        elif event_type == EventType.Request.FindClass:
            return self._handle_find_class(event, state)
        elif event_type == EventType.Request.EventFlow:
            return self._handle_event_flow(event, state)
        else:
            raise AttributeError(f"Unknown event type: {event_type}.")

    def _update_version(self, store: Store, version: Version, updated_state: State, write_set: WriteSet) -> Store:
        if updated_state is not None:
            # If the event is an EventFlow, the payload will have a WriteSet to
            # keep track of which objects were involved in the event/transaction.
            version.update(updated_state, write_set)
            store.set_version(version.id, version)

        return store

    def handle(self, event: Event, serialized_state: Optional[bytes]) -> Generator[Event, None, Optional[bytes]]:
        """Handles incoming event and current state.

        Depending on the event type, a method is executed or a instance is created, or state is updated, etc.

        :param event: the incoming event.
        :param serialized_state: the incoming state (in bytes). If this is None, we assume this 'key' does not exist.
        :return: a generator that yields outgoing events and returns the updated state (in bytes).
        """
        print("event", event.event_id[:8], event.fun_address, event.event_type, event.payload)

        if event.event_type == EventType.Request.InitClass:
            return_event, updated_state = self._handle_create_with_state(event, serialized_state)
            yield return_event
            return updated_state

        if serialized_state:  # If state exists, we can deserialize it.
            store: Store = self.serializer.deserialize_store(serialized_state)
        else:  # If state does not exists we can't execute these methods, so we return a KeyNotFound reply.
            yield event.copy(
                event_type=EventType.Reply.KeyNotFound,
                payload={
                    "error_message": f"Stateful instance with key={event.fun_address.key} does not exist."
                }
            )
            return serialized_state

        if event.event_type == EventType.Request.CommitState:
            store.commit_version_for_event_id(event.event_id)
            return self.serializer.serialize_store(store)

        version = store.get_or_create_version_for_event(event.event_id)

        # We dispatch the event to find the correct execution method.
        return_event, updated_state = self._dispatch_event(
            event.event_type, event, version.state
        )
        write_set: WriteSet = return_event.payload.get("write_set")
        store = self._update_version(store, version, updated_state, write_set)

        # If we have an EventFlowGraph, we need to commit the state in all
        # involved operators. Otherwise, we directly commit the state of the
        # current operator since no others were involved in this operation.
        flow_graph: EventFlowGraph = return_event.payload.get("flow", None)
        if flow_graph is not None and isinstance(flow_graph.current_node, ReturnNode):
            yield from self.generate_commit_events(return_event.event_id, write_set)
        elif flow_graph is None:
            store.commit_version(version.id)

        event = return_event
        print("return", event.event_id[:8], event.fun_address, event.event_type, event.payload)

        yield return_event
        return self.serializer.serialize_store(store)

    def generate_commit_events(self, event_id: str, write_set: WriteSet) -> Iterator[Event]:
        for address in write_set.iterate_operators():
            yield Event(
                event_id=event_id,
                fun_address=address,
                event_type=EventType.Request.CommitState,
                payload={"write_set": write_set},
            )

    def _handle_create_with_state(
        self, event: Event, state: Optional[bytes]
    ) -> Tuple[Event, bytes]:
        """Will 'create' this instance, by verifying if the state exists already.

        1. If state exists, we return an FailedInvocation because we can't construct the same key twice.
        2. Otherwise, we unpack the created state from the payload and return it.
            In the outgoing event, we put the key in the payload.

        :param event: the incoming InitClass event.
        :param state: the current state (in bytes), might be None.
        :return: the outgoing event and (updated) state.
        """
        if state:  # In this case, we already created a class before, so we will return an error.
            return (
                event.copy(
                    event_type=EventType.Reply.FailedInvocation,
                    payload={
                        "error_message": f"{event.fun_address.function_type.get_full_name()} class "
                        f"with key={event.fun_address.key} already exists."
                    },
                ),
                state,
            )

        return_event = event.copy(
            event_type=EventType.Reply.SuccessfulCreateClass,
            payload={"key": f"{event.fun_address.key}"},
        )
        new_state = event.payload["init_class_state"]
        new_store = Store(initial_state=new_state)

        return return_event, self.serializer.serialize_store(new_store)

    def _handle_get_state(self, event: Event, state: State) -> Tuple[Event, State]:
        """Gets a field/attribute of the current state.

         The incoming event needs to have an 'attribute' field in the payload.
         We assume this attribute is available in the state and in the correct format.
         We don't check this explicitly for performance reasons.

        :param event: the incoming event.
        :param state: the current state.
        :return: a tuple of outgoing event + updated state.
        """
        return (
            event.copy(
                event_type=EventType.Reply.SuccessfulStateRequest,
                payload={"state": state[event.payload["attribute"]]},
            ),
            state,
        )

    def _handle_find_class(self, event: Event, state: State) -> Tuple[Event, State]:
        """Check if the instance of a class exists.

        If this is the case, we simply return with an empty payload and `EventType.Reply.FoundClass`.
        We assume the instance _exists_ if we get in this method. If it did not exist,
        state would have been None and we would have returned a KeyNotFound event.
        # TODO Consider renaming to FindInstance, which makes more sense from a semantic perspective.

        :param event: event: the incoming event.
        :param state: the current state.
        :return: a tuple of outgoing event + current state.
        """
        return event.copy(event_type=EventType.Reply.FoundClass, payload={}), state

    def _handle_update_state(self, event: Event, state: State) -> Tuple[Event, State]:
        """Update one attribute of the state.

        The incoming event needs to have an 'attribute' field in the payload aswell as 'attribute_value'.
        The 'attribute' field is the key of the state, whereas 'attribute_value' is the value.
        We assume this attribute is available in the state and in the correct format.
        We don't check this explicitly for performance reasons.

        :param event: the incoming event.
        :param state: the current state.
        :return: a tuple of outgoing event + updated state.
        """
        state[event.payload["attribute"]] = event.payload["attribute_value"]
        return_event = event.copy(
            event_type=EventType.Reply.SuccessfulStateRequest,
            payload={},
        )
        return return_event, state

    def _handle_invoke_stateful(
        self, event: Event, state: State
    ) -> Tuple[Event, State]:
        """Invokes a stateful method.

        The incoming event needs to have a `method_name` and `args` in its payload for the invocation.
        We assume these keys are there and in the correct format.
        We don't check this explicitly for performance reasons.

        Returns:
        1. Current state + failed event, in case the invocation failed for whatever reason.
        2. Updated state + success event, in case of successful invocation.

        :param event: the incoming event.
        :param state: the current state.
        :return: a tuple of outgoing event + updated state.
        """
        invocation: InvocationResult = self.class_wrapper.invoke(
            event.payload["method_name"], state, event.payload["args"]
        )

        if isinstance(invocation, FailedInvocation):
            return (
                event.copy(
                    event_type=EventType.Reply.FailedInvocation,
                    payload={"error_message": invocation.message},
                ),
                state,
            )
        else:
            return (
                event.copy(
                    event_type=EventType.Reply.SuccessfulInvocation,
                    payload={"return_results": invocation.return_results},
                ),
                invocation.updated_state,
            )

    def _handle_event_flow(self, event: Event, state: State) -> Tuple[Event, State]:
        flow_graph: EventFlowGraph = event.payload["flow"]
        current_address: FunctionAddress = flow_graph.current_node.fun_addr

        write_set = event.payload.get("write_set", WriteSet())
        write_set.add(current_address, state.get_version_id())
        event.payload["write_set"] = write_set

        updated_state, instance = flow_graph.step(self.class_wrapper, state)

        # Keep stepping :)
        while flow_graph.current_node.fun_addr == current_address:
            if (
                isinstance(flow_graph.current_node, ReturnNode)
                and flow_graph.current_node.next == []
                or flow_graph.current_node.next == -1
            ):
                break

            # print(
            #     f"Stepping again {flow_graph.current_node.typ} and {flow_graph.current_node.to_dict()}"
            # )
            updated_state, _ = flow_graph.step(
                self.class_wrapper, updated_state, instance
            )

        # print(
        #     f"Now going to {flow_graph.current_node.fun_addr.to_dict()} {flow_graph.current_node.typ}"
        # )

        return event, updated_state
