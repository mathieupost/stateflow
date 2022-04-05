import ujson
from stateflow.dataflow.args import Arguments
from stateflow.dataflow.event import EventType, FunctionAddress
from stateflow.dataflow.event_flow import EventFlowGraph
from stateflow.dataflow.state import Store
from stateflow.serialization.serde import Event, SerDe


class JsonSerializer(SerDe):
    def serialize_store(self, store: Store) -> bytes:
        encoded_versions = store.encoded_versions
        last_committed_version_id = store.last_committed_version_id
        event_version_map = store.event_version_map

        store_dict = {
            "encoded_versions": encoded_versions,
            "last_committed_version_id": last_committed_version_id,
            "event_version_map": event_version_map,
        }
        return self.serialize_dict(store_dict)

    def deserialize_store(self, store: bytes) -> Store:
        store_dict = self.deserialize_dict(store)
        if type(store_dict) is Store:
            return store_dict
        return Store(store_dict)

    def serialize_event(self, event: Event) -> bytes:
        event_id: str = event.event_id
        event_type: str = event.event_type.value
        fun_address: dict = event.fun_address.to_dict()
        payload: dict = event.payload

        for item in payload:
            if hasattr(payload[item], "to_dict"):
                payload[item] = payload[item].to_dict()

        event_dict = {
            "event_id": event_id,
            "event_type": event_type,
            "fun_address": fun_address,
            "payload": payload,
        }

        return self.serialize_dict(event_dict)

    def deserialize_event(self, event: bytes) -> Event:
        json = self.deserialize_dict(event)

        event_id: str = json["event_id"]
        event_type: str = EventType.from_str(json["event_type"])
        fun_address: dict = FunctionAddress.from_dict(json["fun_address"])
        payload: dict = json["payload"]

        if "args" in payload:
            payload["args"] = Arguments.from_dict(json["payload"]["args"])

        if "flow" in payload:
            payload["flow"] = EventFlowGraph.from_dict(payload["flow"])

        return Event(event_id, fun_address, event_type, payload)

    def serialize_dict(self, dictionary: dict) -> bytes:
        return ujson.encode(dictionary, ensure_ascii=False).encode("utf-8")

    def deserialize_dict(self, dictionary: bytes) -> dict:
        return ujson.decode(dictionary)
