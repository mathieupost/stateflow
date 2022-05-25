import copy
from typing import List
import uuid

import pytest
from stateflow.dataflow.address import FunctionAddress, FunctionType
from stateflow.dataflow.args import Arguments
from stateflow.dataflow.dataflow import Dataflow, EgressRouter, IngressRouter, Operator
from stateflow.dataflow.event import Event, EventType
from stateflow.dataflow.event_flow import (EventFlowGraph, InternalClassRef)
from stateflow.dataflow.state import Store
from stateflow.dataflow.stateful_operator import StatefulOperator
from stateflow.descriptors.class_descriptor import ClassDescriptor
from stateflow.serialization.json_serde import JsonSerializer
from tests.common.common_classes import stateflow


@pytest.mark.usefixtures("setup_ingress_egress")
class TestCommitState:
    def step(self, model: "Model", event: Event) -> List[Event]:
        operator = model.operator
        ingress: IngressRouter = self.ingress
        egress: EgressRouter = self.egress

        route = ingress.route(event)
        serialized_store = model.get_serialized_store()
        handler = operator.handle(route.value, serialized_store)

        events = list(handler)
        if handler.return_value is not None:
            model.update_store(handler.return_value)

        if len(events) == 0:
            return events

        for i, event in enumerate(events):
            route = egress.route_and_serialize(event)
            events[i] = route.value

        return events

    def test_commit_state(self, sender: "Model", receiver: "Model", transfer_balance_event1):
        initial_state_sndr = sender.store.get_last_committed_version().state.get()
        initial_state_rcvr = receiver.store.get_last_committed_version().state.get()

        # INVOKE_SPLIT_FUN User(sender).transfer_balance_0(...)
        events = self.step(sender, transfer_balance_event1)
        assert len(events) == 1
        assert events[0].event_type == EventType.Request.EventFlow
        expected_write_set_1 = {"global": {"User": {
            "sender": 1,
        }}}
        assert events[0].payload["write_set"] == expected_write_set_1
        expected_last_write_set_1 = {"global": {"User": {
            "sender": 0,
        }}}
        assert events[0].payload["last_write_set"] == expected_last_write_set_1
        assert sender.store.last_committed_version_id == 0

        # INVOKE_EXTERNAL User(receiver).update_balance(...)
        events = self.step(receiver, events[0])
        assert len(events) == 1
        assert events[0].event_type == EventType.Request.EventFlow
        expected_write_set_2 = {"global": {"User": {
            "sender": 1,
            "receiver": 1,
        }}}
        assert events[0].payload["write_set"] == expected_write_set_2
        expected_last_write_set_2 = {"global": {"User": {
            "sender": 0,
            "receiver": 0,
        }}}
        assert events[0].payload["last_write_set"] == expected_last_write_set_2
        assert receiver.store.last_committed_version_id == 0
        updated_state = receiver.get_state(events[0])
        assert updated_state["balance"] - initial_state_rcvr["balance"] == 10

        # INVOKE_SPLIT_FUN User(sender).transfer_balance_7(...)
        events = self.step(sender, events[0])
        assert len(events) == 2
        # COMMIT_STATE User(receiver)
        assert events[0].event_type == EventType.Request.CommitState
        assert events[0].payload["write_set"] == expected_write_set_2
        assert events[1].event_type == EventType.Reply.SuccessfulInvocation
        assert events[1].payload["return_results"] == [True]
        # Version was committed.
        assert sender.store.last_committed_version_id == 1
        updated_state = sender.get_state(events[0])
        assert updated_state["balance"] - initial_state_sndr["balance"] == -10

        # COMMIT_STATE User(receiver)
        _ = self.step(receiver, events[0])
        # Version was committed.
        assert receiver.store.last_committed_version_id == 1

        item_write_set = sender.store.get_last_committed_version().write_set
        user_write_set = receiver.store.get_last_committed_version().write_set
        assert item_write_set == user_write_set

    def test_commit_state_concurrent(self, sender: "Model", receiver: "Model", transfer_balance_event1: Event, transfer_balance_event2: Event):
        ########## Begin 1st transaction ##########
        # INVOKE_SPLIT_FUN User(sender).transfer_balance_0(...)
        tr1_events = self.step(sender, transfer_balance_event1)
        # INVOKE_EXTERNAL User(receiver).update_balance(...)
        tr1_events = self.step(receiver, tr1_events[0])
        # INVOKE_SPLIT_FUN User(sender).transfer_balance_7(...)
        tr1_events = self.step(sender, tr1_events[0])

        ########## Begin 2nd transaction ##########
        def tr2():
            tr2_event_id = transfer_balance_event2.event_id
            # INVOKE_SPLIT_FUN User(sender).transfer_balance_0(...)
            # Gets committed version from 1st transaction.
            tr2_events = self.step(sender, transfer_balance_event2)
            sndr_version = sender.store.get_version_for_event_id(tr2_event_id)
            assert sender.store.last_committed_version_id == sndr_version.parent_id
            assert sndr_version.parent_id == 1
            assert sndr_version.id == 2
            assert tr2_events[0].payload["last_write_set"] == sender.store.get_last_committed_version(
            ).write_set

            # INVOKE_EXTERNAL User(receiver).update_balance(...)
            # Should get uncommitted (newer) state from 1st transaction, because
            # of detected new version in write_set of the state of item.
            tr2_events = self.step(receiver, tr2_events[0])
            rcvr_version = receiver.store.get_version_for_event_id(
                tr2_event_id)
            assert receiver.store.last_committed_version_id < rcvr_version.parent_id
            assert rcvr_version.parent_id == 1
            assert rcvr_version.id == 2

            # INVOKE_SPLIT_FUN User(sender).transfer_balance_7(...)
            tr2_events = self.step(sender, tr2_events[0])
            # COMMIT_STATE User(receiver)
            _ = self.step(receiver, tr2_events[0])

        tr2()  # indented to make it easier to see what happens
        ########## End 2nd transaction ##########

        # COMMIT_STATE User(receiver)
        _ = self.step(receiver, tr1_events[0])
        ########## End 1st transaction ##########

    def test_commit_state_concurrent_reverse(self, sender: "Model", receiver: "Model", transfer_balance_event1: Event, transfer_balance_event_reverse: Event):
        ########## Begin 1st transaction ##########
        # INVOKE_SPLIT_FUN User(sender).transfer_balance_0(...)
        tr1_events = self.step(sender, transfer_balance_event1)
        # INVOKE_EXTERNAL User(receiver).update_balance(...)
        tr1_events = self.step(receiver, tr1_events[0])
        # INVOKE_SPLIT_FUN User(sender).transfer_balance_7(...)
        tr1_events = self.step(sender, tr1_events[0])

        ########## Begin 2nd transaction ##########
        def tr2():
            tr2_event_id = transfer_balance_event_reverse.event_id
            # INVOKE_SPLIT_FUN User(receiver).transfer_balance_0(...)
            # Does not get committed version from 1st transaction.
            tr2_events = self.step(receiver, transfer_balance_event_reverse)
            rcvr_version = receiver.store.get_version_for_event_id(tr2_event_id)
            assert receiver.store.last_committed_version_id == rcvr_version.parent_id
            assert rcvr_version.parent_id == 0 # Old version
            assert rcvr_version.id == 2
            assert tr2_events[0].payload["last_write_set"] == {"global": {"User": {
                "receiver": 0,
            }}}

            # INVOKE_EXTERNAL User(sender).update_balance(...)
            # Should get committed (newer) state from 1st transaction, which
            # depends on the new version of User(receiver). This should result
            # in an updated last_write_set and a restart of the transaction.
            tr2_events = self.step(sender, tr2_events[0])
            assert sender.store.last_committed_version_id == 1
            assert tr2_events[0].payload["last_write_set"] == {"global": {"User": {
                "receiver": 1,
                "sender": 1,
            }}}

            # INVOKE_SPLIT_FUN User(receiver).transfer_balance_0(...) (AGAIN)
            # Gets uncommitted version from 1st transaction this time.
            tr2_events = self.step(receiver, transfer_balance_event_reverse)
            rcvr_version = receiver.store.get_version_for_event_id(tr2_event_id)
            assert receiver.store.last_committed_version_id < rcvr_version.parent_id
            assert rcvr_version.parent_id == 1 # New uncommitted version
            assert rcvr_version.id == 3

            # INVOKE_EXTERNAL User(sender).update_balance(...)
            tr2_events = self.step(sender, tr2_events[0])
            sndr_version = sender.store.get_version_for_event_id(tr2_event_id)
            assert sender.store.last_committed_version_id == sndr_version.parent_id
            assert sndr_version.parent_id == 1
            assert sndr_version.id == 2

            # INVOKE_SPLIT_FUN User(receiver).transfer_balance_7(...)
            tr2_events = self.step(receiver, tr2_events[0])
            # COMMIT_STATE User(sender)
            _ = self.step(sender, tr2_events[0])

        tr2()  # indented to make it easier to see what happens
        ########## End 2nd transaction ##########

        # COMMIT_STATE User(receiver)
        _ = self.step(receiver, tr1_events[0])
        ########## End 1st transaction ##########


@pytest.fixture(scope="class")
def flow() -> Dataflow:
    return stateflow.init()


@pytest.fixture(scope="function")
def sender(flow: Dataflow):
    initial_state = {"username": "sender", "balance": 20, "items": []}
    return Model(flow.operators[1], "sender", initial_state)


@pytest.fixture(scope="function")
def receiver(flow: Dataflow):
    initial_state = {"username": "receiver", "balance": 10, "items": []}
    return Model(flow.operators[1], "receiver", initial_state)


@pytest.fixture(scope="class")
def transfer_balance_flow(flow):
    cls_descriptor: ClassDescriptor = flow.operators[1].class_wrapper.class_desc
    for method in cls_descriptor.methods_dec:
        if method.method_name == "transfer_balance":
            break
    return method.flow_list


def transfer_balance_event(sender, receiver, transfer_balance_flow):
    args = {
        "receiver": InternalClassRef(receiver.fun_addr),
        "amount": 10,
    }
    return Event(
        str(uuid.uuid4()),
        sender.fun_addr,
        EventType.Request.EventFlow,
        {
            "flow": EventFlowGraph.construct_and_assign_arguments(
                copy.deepcopy(transfer_balance_flow),
                sender.fun_addr,
                Arguments(args),
            )
        },
    )


@pytest.fixture(scope="function")
def transfer_balance_event1(sender, receiver, transfer_balance_flow):
    return transfer_balance_event(sender, receiver, transfer_balance_flow)


@pytest.fixture(scope="function")
def transfer_balance_event2(sender, receiver, transfer_balance_flow):
    return transfer_balance_event(sender, receiver, transfer_balance_flow)


@pytest.fixture(scope="function")
def transfer_balance_event_reverse(sender, receiver, transfer_balance_flow):
    # Swap sender and receiver
    return transfer_balance_event(receiver, sender, transfer_balance_flow)


@pytest.fixture(scope="class")
def setup_ingress_egress(request):
    serializer = JsonSerializer()
    request.cls.ingress = IngressRouter(serializer)
    request.cls.egress = EgressRouter(serializer, serialize_on_return=False)


class Model:
    def __init__(self, operator: Operator, key: str, initial_state: dict):
        assert isinstance(operator, StatefulOperator)

        self.serializer = JsonSerializer()
        operator.serializer = self.serializer
        name = operator.class_wrapper.class_desc.class_name

        self.operator: StatefulOperator = operator
        self.store = Store(initial_state=initial_state)
        self.fun_addr = FunctionAddress(
            FunctionType("global", name, True), key)

    def update_store(self, serialized_store: bytes):
        self.store = self.serializer.deserialize_store(serialized_store)

    def get_serialized_store(self) -> bytes:
        return self.serializer.serialize_store(self.store)

    def get_state(self, event: Event) -> dict:
        event_id = event.event_id
        return self.store.get_version_for_event_id(event_id).state.get()