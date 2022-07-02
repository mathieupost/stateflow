from stateflow.dataflow.address import FunctionAddress, FunctionType
from stateflow.dataflow.event import Event, EventType
from stateflow.serialization.json_serde import JsonSerializer


class TestJsonSerializer:
    json_serde = JsonSerializer()

    def test_serialize_deserialize_event(self):
        event = Event(
            "id",
            FunctionAddress(FunctionType("namespace", "name", True), "key"),
            EventType.Request.EventFlow,
            {
                "args": {"arg1": "value1", "arg2": "value2"},
            },
        )
        serialized = self.json_serde.serialize_event(event)
