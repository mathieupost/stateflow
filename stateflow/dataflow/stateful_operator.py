from typing import Generator, Iterator, List, NewType, Optional, Tuple

from stateflow.dataflow.address import FunctionAddress
from stateflow.dataflow.dataflow import Edge, EventType, FunctionType, Operator
from stateflow.dataflow.event import Event
from stateflow.dataflow.event_flow import EventFlowGraph, ReturnNode
from stateflow.dataflow.state import State, Store, WriteSet
from stateflow.serialization.pickle_serializer import PickleSerializer, SerDe
from stateflow.wrappers.class_wrapper import (ClassWrapper, FailedInvocation,
                                              InvocationResult)
from stateflow.wrappers.meta_wrapper import MetaWrapper

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
        else:
            raise AttributeError(f"Unknown event type: {event_type}.")

    def handle(self, event: Event, serialized_state: Optional[bytes]) -> Generator[Event, None, Optional[bytes]]:
        """Handles incoming event and current state.

        Depending on the event type, a method is executed or a instance is created, or state is updated, etc.

        :param event: the incoming event.
        :param serialized_state: the incoming state (in bytes). If this is None, we assume this 'key' does not exist.
        :return: a generator that yields outgoing events and returns the updated state (in bytes).
        """
        print("event", event.event_id[:8], event.fun_address, event.event_type, event.payload)

        if event.event_type == EventType.Request.InitClass:
            event, updated_state = self._handle_create_with_state(event, serialized_state)
            yield event
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
            version = store.get_version_for_event_id(event.event_id)
            write_set = event.payload["write_set"]
            version.set_write_set(write_set)
            store.update_version(version)
            store.commit_version(version.id)
            return self.serializer.serialize_store(store)

        if event.event_type == EventType.Request.EventFlow:
            yield from self._handle_event_flow(event, store)
            return self.serializer.serialize_store(store)

        version = store.create_version()

        # We dispatch the event to find the correct execution method.
        event, updated_state = self._dispatch_event(
            event.event_type, event, version.state
        )
        if updated_state is None:
            yield event
            return None

        print("return", event.event_id[:8], event.fun_address, event.event_type, event.payload)

        store.update_version(version, updated_state)
        # Since we are not in an EventFlow, we directly commit the version,
        # because we don't need to wait for other operators to commit.
        store.commit_version(version.id)

        yield event
        return self.serializer.serialize_store(store)

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

    def _handle_event_flow(self, event: Event, store: Store) -> Iterator[Event]:
        flow_graph: EventFlowGraph = event.payload["flow"]
        current_address: FunctionAddress = flow_graph.current_node.fun_addr

        write_set: WriteSet = event.payload.get("write_set", WriteSet())
        last_write_set: WriteSet = event.payload.get("last_write_set", WriteSet())
        if not write_set.address_exists(current_address):
            # If the current operator is not in the write_set, it is not
            # yet used in the event flow and we need to:
            # - check if a minimum version is set in the last_write_set
            min_parent_id = last_write_set.get_address(current_address)
            # - create a new version
            version = store.create_version_for_event_id(event.event_id, min_parent_id)
            # - add it to the write_set
            write_set.add_address(current_address, version.id)
            last_write_set.add_address(current_address, version.parent_id)
            # - add the write_set to the event
            event.payload["write_set"] = write_set
            # - check if parent_write_set has newer versions of operators
            #   that are in the write_set and last_write_set.
            parent_write_set = store.get_version(version.parent_id).write_set or WriteSet()
            # - loop over all addresses in the write_set to check if they
            #   are consistent with the parent_write_set.
            is_consistent = True
            for ns, o, k, min_parent_version in parent_write_set.iterate():
                if write_set.exists(ns, o, k):
                    # If the operator is in the write_set, it is used in
                    # this flow. So, we need to make sure that the version
                    # of the operator is at least as high as the minimum
                    # version in the parent_write_set. If it is lower, then
                    # we know that a newer version existed before the flow
                    # started, and we should restart the flow to use the
                    # newer version.
                    parent_version = last_write_set.get(ns, o, k)
                    if parent_version < min_parent_version:
                        print(f"Inconsistent version for {ns}:{o}:{k} ({parent_version} < {min_parent_version})")
                        is_consistent = False
                # - update the last_write_set to include the other operators
                last_write_set.add(ns, o, k, min_parent_version)
            event.payload["last_write_set"] = last_write_set
            # - if the version is inconsistent with previous operator
            #   versions, we need to restart the flow.
            if not is_consistent:
                flow_graph.reset()
                del event.payload["write_set"]
                print("return", event.event_id[:8], event.fun_address, event.event_type, event.payload)
                yield event
                return
        else:
            # If the current operator is in the write set, we need to
            # get the version for this event flow.
            version = store.get_version_for_event_id(event.event_id)

        updated_state, instance = flow_graph.step(self.class_wrapper, version.state)

        # Keep stepping :)
        while flow_graph.current_node.fun_addr == current_address:
            if (
                isinstance(flow_graph.current_node, ReturnNode)
                and flow_graph.current_node.next == []
                or flow_graph.current_node.next == -1
            ):
                break

            updated_state, _ = flow_graph.step(
                self.class_wrapper, updated_state, instance
            )

        version.set_write_set(write_set)
        store.update_version(version, updated_state)

        if isinstance(flow_graph.current_node, ReturnNode):
            # If the current node in the return event is a ReturnNode, directly
            # commit the version of this operator.
            store.commit_version(version.id)
            # And yield CommitState events for all other involved operators.
            for address in write_set.iterate_addresses():
                if address == current_address:
                    continue
                yield event.copy(
                    fun_address=address,
                    event_type=EventType.Request.CommitState,
                    payload={"write_set": write_set}
                )

        print("return", event.event_id[:8], event.fun_address, event.event_type, event.payload)
        yield event
        return
