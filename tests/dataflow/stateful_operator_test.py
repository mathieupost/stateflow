import uuid
from turtle import up

import pytest
from stateflow.dataflow.address import FunctionAddress, FunctionType
from stateflow.dataflow.args import Arguments
from stateflow.dataflow.event import Event, EventType
from stateflow.dataflow.state import State
from stateflow.dataflow.stateful_operator import (StatefulGenerator,
                                                  StatefulOperator)
from stateflow.serialization.json_serde import JsonSerializer
from tests.common.common_classes import stateflow
from tests.context import stateflow
from tests.state.serialize_utils import (serialized_store_to_state,
                                         state_to_serialized_store)


@pytest.fixture(scope="session", autouse=True)
def setup():
    flow = stateflow.init()
    item_operator = flow.operators[0]
    item_operator.serializer = JsonSerializer()
    user_operator = flow.operators[1]
    user_operator.serializer = JsonSerializer()
    yield user_operator, item_operator


class TestStatefulOperator:
    def test_init_class_positive(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), None),
            EventType.Request.InitClass,
            {"args": Arguments({"username": "wouter"})},
        )

        return_event = operator.handle_create(event)

        assert return_event.event_id == event_id
        assert return_event.fun_address.key == "wouter"
        assert return_event.payload == {
            "init_class_state": {"username": "wouter", "balance": 0, "items": []}
        }

    def test_handle_init_class_positive(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), None),
            EventType.Request.InitClass,
            {"args": Arguments({"username": "wouter"})},
        )

        intermediate_event = operator.handle_create(event)
        handler = StatefulGenerator(operator.handle(intermediate_event, None))
        events = list(handler)
        
        assert len(events) == 1
        assert events[0].event_type == EventType.Reply.SuccessfulCreateClass
        assert events[0].payload["key"] == "wouter"
        assert handler.state is not None

    def test_handle_init_class_negative(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), None),
            EventType.Request.InitClass,
            {"args": Arguments({"username": "wouter"})},
        )

        intermediate_event = operator.handle_create(event)
        handler = StatefulGenerator(operator.handle(intermediate_event,  "non_empty_state"))
        events = list(handler)

        assert len(events) == 1
        assert events[0].event_type == EventType.Reply.FailedInvocation
        assert events[0].payload["error_message"]
        assert handler.state == "non_empty_state"

    def test_invoke_stateful_positive(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), "wouter"),
            EventType.Request.InvokeStateful,
            {"args": Arguments({"x": 5}), "method_name": "update_balance"},
        )

        state = State({"username": "wouter", "balance": 10, "items": []})
        handler = StatefulGenerator(operator.handle(
            event, state_to_serialized_store(state)
        ))
        events = list(handler)
        updated_state = serialized_store_to_state(handler.state)

        assert len(events) == 1
        assert events[0].event_type == EventType.Reply.SuccessfulInvocation
        assert events[0].payload["return_results"] is None
        assert updated_state["balance"] == 15

    def test_invoke_stateful_negative(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), "wouter"),
            EventType.Request.InvokeStateful,
            {"args": Arguments({"x": "100"}), "method_name": "update_balance"},
        )

        state = State({"username": "wouter", "balance": 10, "items": []})
        handler = StatefulGenerator(operator.handle(
            event, state_to_serialized_store(state)
        ))
        events = list(handler)
        updated_state = serialized_store_to_state(handler.state)

        assert len(events) == 1
        assert events[0].event_type == EventType.Reply.FailedInvocation
        assert updated_state["balance"] == 10

    def test_get_state_positive(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), "wouter"),
            EventType.Request.GetState,
            {"attribute": "balance"},
        )

        state = State({"username": "wouter", "balance": 11, "items": []})
        handler = StatefulGenerator(operator.handle(
            event, state_to_serialized_store(state)
        ))
        events = list(handler)
        updated_state = serialized_store_to_state(handler.state)

        assert len(events) == 1
        assert events[0].event_type == EventType.Reply.SuccessfulStateRequest
        assert events[0].payload["state"] == 11
        assert state.get() == updated_state.get()  # State is not updated.

    def test_update_state_positive(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), "wouter"),
            EventType.Request.UpdateState,
            {"attribute": "balance", "attribute_value": 8},
        )

        state = State({"username": "wouter", "balance": 11, "items": []})
        handler = StatefulGenerator(operator.handle(
            event, state_to_serialized_store(state)
        ))
        events = list(handler)
        updated_state = serialized_store_to_state(handler.state)

        assert len(events) == 1
        assert events[0].event_type == EventType.Reply.SuccessfulStateRequest
        assert events[0].payload == {}
        assert updated_state.get()["balance"] == 8
        assert state.get() != updated_state.get()  # State is updated.

    def test_find_class_positive(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), "wouter"),
            EventType.Request.FindClass,
            {},
        )

        state = State({"username": "wouter", "balance": 11, "items": []})
        handler = StatefulGenerator(operator.handle(
            event, state_to_serialized_store(state)
        ))
        events = list(handler)
        updated_state = serialized_store_to_state(handler.state)

        assert len(events) == 1
        assert events[0].event_type == EventType.Reply.FoundClass
        assert events[0].payload == {}
        assert state.get() == updated_state.get()  # State is updated.

    def test_state_does_not_exist_no_init_class(self, setup):
        operator: StatefulOperator = setup[0]

        event_id = str(uuid.uuid4())
        event = Event(
            event_id,
            FunctionAddress(FunctionType("global", "User", True), "wouter"),
            EventType.Request.InvokeStateful,
            {"args": Arguments({"x": "100"}), "method_name": "update_balance"},
        )

        handler = StatefulGenerator(operator.handle(event, None))
        events = list(handler)

        assert len(events) == 1
        assert events[0].event_type == EventType.Reply.KeyNotFound
        assert handler.state is None
