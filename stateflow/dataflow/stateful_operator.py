from enum import Enum
from typing import Iterator, List, NewType, Optional, Tuple

from stateflow.dataflow.address import FunctionAddress
from stateflow.dataflow.dataflow import Edge, EventType, FunctionType, Operator
from stateflow.dataflow.event import Event
from stateflow.dataflow.event_flow import EventFlowGraph, ReturnNode
from stateflow.dataflow.state import (AddressEventSet, EventAddressTuple,
                                      State, Store, Version, WriteSet)
from stateflow.serialization.pickle_serializer import PickleSerializer, SerDe
from stateflow.util.generator_wrapper import (WrappedGenerator,
                                              keep_return_value)
from stateflow.wrappers.class_wrapper import (ClassWrapper, FailedInvocation,
                                              InvocationResult)
from stateflow.wrappers.meta_wrapper import MetaWrapper

NoType = NewType("NoType", None)


class IsolationType(Enum):
    """Enum to different IsolationMode."""

    # Abort the current transaction if an operator is in an unfinished transaction.
    ABORT = "Abort"
    # Queue the current transaction if an operator is in an unfinished transaction.
    QUEUE = "Queue"
    # Use two phase commit to commit transactions.
    TWO_PHASE_COMMIT = "2PC"

    def is_abort(self) -> bool:
        """Helper function to check if the IsolationType is ABORT."""
        return self == IsolationType.ABORT

    def is_queue(self) -> bool:
        """Helper function to check if the IsolationType is QUEUE."""
        return self == IsolationType.QUEUE

    def is_2pc(self) -> bool:
        """Helper function to check if the IsolationType is TWO_PHASE_COMMIT."""
        return self == IsolationType.TWO_PHASE_COMMIT


IsolationMode = IsolationType.QUEUE


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
        where it will call `handle(event, state)` and subsequently
        `_handle_create_with_state(event, state)`.

        :param event: the Request.InitClass event.
        :return: the created instance, embedded in an Event.
        """
        args = event.payload["args"]
        res: InvocationResult = self.class_wrapper.init_class(args)

        assert res.return_results is not None
        key: str = res.return_results[0]
        assert res.updated_state is not None
        created_state: State = res.updated_state

        # Set the key and the state of this class.
        event.fun_address.key = key
        event.payload["init_class_state"] = created_state.get()

        # We can get rid of the arguments.
        del event.payload["args"]

        return event

    def _dispatch_event(self, event: Event, store: Store) -> Iterator[Event]:
        """Dispatches an event to the correct method to execute/handle it.

        :param event_type: the event_type to find the correct handle.
        :param event: the incoming event.
        :param state: the incoming state.
        :return: a tuple of outgoing event + updated state.
        """

        event_type = event.event_type
        if event_type == EventType.Request.GetState:
            yield self._handle_get_state(event, store)
        elif event_type == EventType.Request.FindClass:
            yield self._handle_find_class(event, store)
        elif event_type == EventType.Request.InvokeStateful:
            yield self._handle_invoke_stateful(event, store)
        elif event_type == EventType.Request.UpdateState:
            yield self._handle_update_state(event, store)
        elif event_type == EventType.Request.Prepare:
            yield from self._handle_prepare(event, store)
        elif event_type == EventType.Request.VoteYes:
            yield from self._handle_vote_yes(event, store)
        elif event_type == EventType.Request.VoteNo:
            yield from self._handle_vote_no(event, store)
        elif event_type == EventType.Request.Commit:
            yield from self._handle_commit(event, store)
        elif event_type == EventType.Request.DeadlockCheck:
            yield from self._handle_deadlock_check(event, store)
        elif event_type == EventType.Request.EventFlow:
            yield from self._handle_event_flow(event, store)
        else:
            raise AttributeError(f"Unknown event type: {event_type}.")

    @keep_return_value
    def handle(
        self, event: Event, serialized_state: Optional[bytes]
    ) -> WrappedGenerator[Event, None, Optional[bytes]]:
        """Handles incoming event and current state.

        Depending on the event type, a method is executed or a instance is created, or state is
        updated, etc.

        :param event: the incoming event.
        :param serialized_state: the incoming state (in bytes). If this is None, we assume this
            'key' does not exist.
        :return: a generator that yields outgoing events and returns the updated state (in bytes).
        """

        if event.event_type == EventType.Request.InitClass:
            event, updated_state = self._handle_create_with_state(
                event, serialized_state
            )
            yield event
            return updated_state

        if serialized_state:  # If state exists, we can deserialize it.
            store: Store = self.serializer.deserialize_store(serialized_state)
        else:  # If state does not exists we return a KeyNotFound reply.
            key = event.fun_address.key
            yield event.copy(
                event_type=EventType.Reply.KeyNotFound,
                payload={
                    "error_message": f"Stateful instance with key={key} does not exist."
                },
            )
            return serialized_state

        try:
            # We dispatch the event to find the correct execution method.
            yield from self._dispatch_event(event, store)
        except AbortException:
            retries = event.payload.get("retries", 0) + 1
            event.payload["retries"] = retries
            print("RETRY", retries)
            yield event
            return serialized_state

        return self.serializer.serialize_store(store)

    def _handle_create_with_state(
        self, event: Event, state: Optional[bytes]
    ) -> Tuple[Event, bytes]:
        """Will 'create' this instance, by verifying if the state exists already.

        1. If state exists, we return an FailedInvocation because we can't construct the same key
            twice.
        2. Otherwise, we unpack the created state from the payload and return it.
            In the outgoing event, we put the key in the payload.

        :param event: the incoming InitClass event.
        :param state: the current state (in bytes), might be None.
        :return: the outgoing event and (updated) state.
        """
        if (
            state
        ):  # In this case, we already created a class before, so we will return an error.
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

    def _handle_get_state(self, event: Event, store: Store) -> Event:
        """Gets a field/attribute of the current state.

         The incoming event needs to have an 'attribute' field in the payload.
         We assume this attribute is available in the state and in the correct format.
         We don't check this explicitly for performance reasons.

        :param event: the incoming event.
        :param state: the current state.
        :return: a tuple of outgoing event + updated state.
        """
        version = store.get_last_committed_version()
        state = version.state
        return event.copy(
            event_type=EventType.Reply.SuccessfulStateRequest,
            payload={"state": state[event.payload["attribute"]]},
        )

    def _handle_find_class(self, event: Event, store: Store) -> Event:
        """Check if the instance of a class exists.

        If this is the case, we simply return with an empty payload and `EventType.Reply.FoundClass`.
        We assume the instance _exists_ if we get in this method. If it did not exist,
        state would have been None and we would have returned a KeyNotFound event.
        # TODO Consider renaming to FindInstance, which makes more sense from a semantic perspective.

        :param event: event: the incoming event.
        :param state: the current state.
        :return: a tuple of outgoing event + current state.
        """
        return event.copy(event_type=EventType.Reply.FoundClass, payload={})

    def _handle_update_state(self, event: Event, store: Store) -> Event:
        """Update one attribute of the state.

        The incoming event needs to have an 'attribute' field in the payload aswell as 'attribute_value'.
        The 'attribute' field is the key of the state, whereas 'attribute_value' is the value.
        We assume this attribute is available in the state and in the correct format.
        We don't check this explicitly for performance reasons.

        :param event: the incoming event.
        :param state: the current state.
        :return: a tuple of outgoing event + updated state.
        """
        version = store.create_version()
        if version.id - version.parent_id > 1:
            raise AbortException()

        version.state[event.payload["attribute"]
                      ] = event.payload["attribute_value"]
        return_event = event.copy(
            event_type=EventType.Reply.SuccessfulStateRequest,
            payload={},
        )

        store.update_version(version, version.state)
        store.commit_version(version.id)
        return return_event

    def _handle_invoke_stateful(self, event: Event, store: Store) -> Event:
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
        version = store.create_version()
        if version.id - version.parent_id > 1:
            raise AbortException()

        invocation: InvocationResult = self.class_wrapper.invoke(
            event.payload["method_name"], version.state, event.payload["args"]
        )

        if isinstance(invocation, FailedInvocation):
            return event.copy(
                event_type=EventType.Reply.FailedInvocation,
                payload={"error_message": invocation.message},
            )
        else:
            store.update_version(version, invocation.updated_state)
            store.commit_version(version.id)
            return event.copy(
                event_type=EventType.Reply.SuccessfulInvocation,
                payload={"return_results": invocation.return_results},
            )

    def _commit(
        self,
        store: Store,
        version: Version,
        write_set: WriteSet,
        updated_state: Optional[State] = None,
    ) -> Iterator[Event]:
        store.update_version(version, updated_state, write_set)
        store.commit_version(version.id)
        yield from self._handle_queue(store)

    def _handle_prepare(self, event: Event, store: Store) -> Iterator[Event]:
        transaction_operator: FunctionAddress = event.payload["transaction_operator"]

        version = store.get_version_if_not_outdated_for_event_id(
            event.event_id)
        if version == None:
            yield event.copy(
                event_type=EventType.Request.VoteNo,
                fun_address=transaction_operator,
                payload={"address": event.fun_address},
            )
            store.delete_version_for_event_id(event.event_id)
            return

        if len(store.waiting_for) > 0:
            self._queue_event(event, store)
            return

        write_set: WriteSet = event.payload["write_set"]
        store.update_version(version, write_set=write_set)
        store.waiting_for.add_address(transaction_operator, event.event_id)
        yield event.copy(
            event_type=EventType.Request.VoteYes,
            fun_address=transaction_operator,
            payload={"address": event.fun_address},
        )

    def _handle_vote_yes(self, event: Event, store: Store) -> Iterator[Event]:
        from_operator: FunctionAddress = event.payload["address"]
        store.waiting_for.remove_address(from_operator)

        if len(store.waiting_for) == 0:
            version = store.get_version_for_event_id(event.event_id)
            store.commit_version(version.id)
            for address in version.write_set.iterate_addresses():
                if address == event.fun_address:
                    continue
                yield Event(event.event_id, address, EventType.Request.Commit, None)
            yield from self._handle_queue(store)

    def _handle_vote_no(self, event: Event, store: Store) -> Iterator[Event]:
        raise "TODO"

    def _handle_commit(self, event: Event, store: Store) -> Iterator[Event]:
        version = store.get_version_for_event_id(event.event_id)
        if version.parent_id < store.last_committed_version_id:
            print("New version is committed before this commit was handled!")
        write_set = event.payload["write_set"]
        yield from self._commit(store, version, write_set)

    def _create_deadlock_check_event(
        self, cur_addr: FunctionAddress, event: Event, store: Store
    ) -> Event:
        """Creates a DeadlockCheck event for the operator we are waiting for.

        :param cur_addr: the address of the current operator.
        :param event: the current event.
        :param store: the current state.
        :return: the outgoing event.
        """
        assert len(store.waiting_for) == 1
        wf_addr, wf_event_id = store.waiting_for.get_one()
        assert isinstance(wf_event_id, str)
        path: List[EventAddressTuple] = event.payload.get("path", [])
        path.append(EventAddressTuple(wf_event_id, cur_addr))
        return event.copy(
            event_type=EventType.Request.DeadlockCheck,
            fun_address=wf_addr,
            payload={"path": path},
        )

    def _handle_deadlock_check(self, event: Event, store: Store) -> Iterator[Event]:
        """Checks if the current state is in a deadlock state.

        We do this by checking if the current state is in a cycle. If it is, we
        abort the transaction and continue with the next event in the queue.

        :param event: the incoming event.
        :param state: the current state.
        :return: None.
        """
        if len(store.waiting_for) == 0:
            # We are not waiting for anything, so no need to handle deadlocks!
            return

        if event.event_type == EventType.Request.EventFlow:
            cur_addr = event.payload["flow"].current_node.fun_addr
        else:
            cur_addr = event.fun_address

        wf_addr, wf_event_id = store.waiting_for.get_one()
        assert isinstance(wf_event_id, str)
        path: List[EventAddressTuple] = event.payload.get("path", [])
        for i in range(len(path) - 1, -1, -1):  # traverse backwards
            _, addr = path[i]
            if addr != wf_addr:
                # Continue until we find the operator we are waiting for.
                continue
            # We found a cycle. This operator is waiting for an event
            # (wf_event_id) to continue or commit/abort, but that event is
            # queued in an operator (addr/wf_addr) which in turn is waiting
            # for this event (event_id) to finish.
            path.append(EventAddressTuple(wf_event_id, wf_addr))
            # Select the event with the highest id.
            abort_event_id = min(path[i:], key=lambda x: x[0])[0]
            if abort_event_id == wf_event_id:
                # If that event is the event that the current operator is
                # waiting for, we stop waiting for it.
                store.waiting_for = AddressEventSet()
                store.delete_version_for_event_id(abort_event_id)
                # Continue with the next event from the queue.
                yield from self._handle_queue(store)
            else:
                # Otherwise, we remove the event from the queue and retry
                # it, if it is there.
                aborted_event = self._remove_from_queue(abort_event_id, store)
                if aborted_event is not None:
                    yield self._reset_and_retry_event(aborted_event)
            return

        # The operator we are waiting for is not in the path. So we didn't find
        # a cycle. Forward a DeadlockCheck event to the operator we are waiting
        # for to check if we can detect a deadlock there.
        yield self._create_deadlock_check_event(cur_addr, event, store)

    def _remove_from_queue(self, event_id: str, store: Store) -> Optional[Event]:
        for i, serialized_event in enumerate(store.queue):
            event = self.serializer.deserialize_event(serialized_event)
            if event.event_id == event_id:
                store.queue.pop(i)
                return event

    def _reset_and_retry_event(self, event: Event) -> Event:
        """Retries an event.

        :param event: the event to retry.
        :return: the retried event.
        """
        payload = {}
        # Set or increase the retry count.
        payload["retries"] = event.payload.get("retries", 0) + 1
        # Reset the EventFlowGraph
        flow_graph: EventFlowGraph = event.payload["flow"]
        flow_graph.reset()
        payload["flow"] = flow_graph
        if "last_write_set" in event.payload:
            # Pass the last_write_set to the event, so that it can use it to
            # determine the minimum parent versions of operators.
            payload["last_write_set"] = event.payload["last_write_set"]
        return event.copy(payload=payload)

    def _create_version(
        self, event: Event, store: Store, min_parent_id: int
    ) -> Tuple[Version, bool]:
        flow_graph: EventFlowGraph = event.payload["flow"]
        current_address: FunctionAddress = flow_graph.current_node.fun_addr
        write_set: WriteSet = event.payload.get("write_set", WriteSet())
        last_write_set: WriteSet = event.payload.get(
            "last_write_set", WriteSet())

        # - create a new version
        version = store.create_version_for_event_id(
            event.event_id, min_parent_id)
        # - add it to the write_set
        write_set.add_address(current_address, version.id)
        last_write_set.add_address(current_address, version.parent_id)
        # - add the write_set to the event
        event.payload["write_set"] = write_set
        # - check if parent_write_set has newer versions of operators
        #   that are in the write_set and last_write_set.
        parent_write_set = store.get_version(
            version.parent_id).write_set or WriteSet()
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
                    print(
                        f"Inconsistent version for {ns}:{o}:{k} \
                        ({parent_version} < {min_parent_version})"
                    )
                    is_consistent = False
            # - update the last_write_set to include the other operators
            last_write_set.add(ns, o, k, min_parent_version)
        if not is_consistent:
            del store.event_version_map[event.event_id]
        event.payload["last_write_set"] = last_write_set
        return version, is_consistent

    def _handle_event_flow(self, event: Event, store: Store) -> Iterator[Event]:
        flow_graph: EventFlowGraph = event.payload["flow"]
        current_address: FunctionAddress = flow_graph.current_node.fun_addr

        write_set: WriteSet = event.payload.get("write_set", WriteSet())
        last_write_set: WriteSet = event.payload.get(
            "last_write_set", WriteSet())
        if not write_set.address_exists(current_address):
            # If the current operator is not in the write_set, it is not used in
            # the current attempt to execute the event flow and we need to:
            # - to check if a minimum version is set in the last_write_set
            min_parent_id = last_write_set.get_address(current_address)
            has_unfinished_transaction = store.get_highest_version_id() > max(
                min_parent_id, store.last_committed_version_id
            )
            if has_unfinished_transaction and IsolationMode.is_abort():
                # If there is an unfinished transaction, we can't continue
                # because the other transaction should finish first. We'll abort
                # the event and try again.
                yield self._reset_and_retry_event(event)
                return
            if has_unfinished_transaction and IsolationMode.is_queue():
                # If there is an unfinished transaction, we can't continue
                # because the other transaction should finish first. We'll add
                # the event to the queue and check if we are in a deadlock.
                self._queue_event(event, store)
                if "path" in event.payload:
                    # Only if we have started the transaction in a previous
                    # operator, we need to check if we are in a deadlock.
                    yield from self._handle_deadlock_check(event, store)
                return
            # - create a new version
            version, is_consistent = self._create_version(
                event, store, min_parent_id)
            # - if the version is inconsistent with previous operator
            #   versions, we need to restart the flow.
            if not is_consistent:
                yield self._reset_and_retry_event(event)
                return
        else:
            # If the current operator is in the write set, we need to
            # get the version for this event flow.
            version = store.get_version_for_event_id(event.event_id)
            # TODO check if a newer version is already committed, so we can abort

        # Leave a breadcrumb for deadlock detection.
        path: List[EventAddressTuple] = event.payload.get("path", [])
        path.append(EventAddressTuple(event.event_id, current_address))
        event.payload["path"] = path

        # Initial node
        node = flow_graph.current_node
        # Initial step parameters
        instance, updated_state = None, version.state
        # Keep stepping through the flow as long as the next node should be
        # executed in the current operator.
        while node.fun_addr == current_address and not node.is_last():
            updated_state, instance = flow_graph.step(
                self.class_wrapper, updated_state, instance
            )
            # Update the next node.
            node = flow_graph.current_node

        if node.is_last():
            if IsolationMode.is_2pc():
                if len(store.waiting_for) > 0:
                    # If the operator is already prepared to commit another
                    # transaction we'll queue the current event.
                    self._queue_event(event, store)
                    return
                else:
                    store.update_version(version, updated_state, write_set)
                    yield from self._generate_prepare_events(store, event)
            else:
                store.waiting_for = AddressEventSet()
                # And yield Commit events for all other involved operators.
                yield from self._generate_commit_events(event)
                # Return result.
                yield event
                # Commit if this was the last node in the flow
                yield from self._commit(store, version, write_set, updated_state)
        else:
            # Update the version otherwise.
            store.update_version(version, updated_state)
            if IsolationMode.is_abort() or IsolationMode.is_queue():
                # Set waiting_for to the next operator in the flow.
                store.waiting_for.add_address(node.fun_addr, event.event_id)
            # Continue the event flow in the next operator.
            yield event

        return

    def _queue_event(self, event: Event, store: Store):
        serialized_event = self.serializer.serialize_event(event)
        store.queue.append(serialized_event)

    def _handle_queue(self, store: Store) -> Iterator[Event]:
        if len(store.queue) > 0:
            # And continue with the next event in the queue, if any.
            serialized_event = store.queue.pop(0)
            event = self.serializer.deserialize_event(serialized_event)
            yield from self._handle_event_flow(event, store)

    def _generate_prepare_events(self, store: Store, event: Event) -> Iterator[Event]:
        write_set: WriteSet = event.payload["write_set"]
        flow_graph: EventFlowGraph = event.payload["flow"]
        current_address: FunctionAddress = flow_graph.current_node.fun_addr

        # Prepare all operators in this transaction
        for address in write_set.iterate_addresses():
            if address == current_address:
                continue
            # Indicate that the other operator is not yet prepared.
            store.waiting_for.add_address(address, False)
            # Send a PREPARE message to the other operator.
            yield event.copy(
                fun_address=address,
                event_type=EventType.Request.Prepare,
                payload={
                    "transaction_operator": current_address,
                    "write_set": write_set,
                },
            )

    def _generate_commit_events(self, event: Event) -> Iterator[Event]:
        write_set: WriteSet = event.payload["write_set"]
        flow_graph: EventFlowGraph = event.payload["flow"]
        current_address: FunctionAddress = flow_graph.current_node.fun_addr

        for address in write_set.iterate_addresses():
            if address == current_address:
                continue
            yield event.copy(
                fun_address=address,
                event_type=EventType.Request.Commit,
                payload={"write_set": write_set},
            )


class AbortException(Exception):
    pass
