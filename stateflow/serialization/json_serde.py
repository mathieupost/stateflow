from typing import List
import ujson
from stateflow.dataflow.args import Arguments
from stateflow.dataflow.event import EventType, FunctionAddress
from stateflow.dataflow.event_flow import EventFlowGraph
from stateflow.dataflow.state import AddressEventSet, AddressSet, EventAddressTuple, Store, WriteSet
from stateflow.serialization.serde import Event, SerDe


class JsonSerializer(SerDe):
    def serialize_store(self, store: Store) -> bytes:
        store_dict = {
            "encoded_versions": store.encoded_versions,
            "last_committed_version_id": store.last_committed_version_id,
            "event_version_map": store.event_version_map,
        }
        if len(store.queue) > 0:
            store_dict["queue"] = store.queue
        if len(store.waiting_for) > 0:
            store_dict["waiting_for"] = store.waiting_for
        return self.serialize_dict(store_dict)

    def deserialize_store(self, store: bytes) -> Store:
        store_dict = self.deserialize_dict(store)

        # Make sure the keys are integers (encoding/decoding makes them strings).
        encoded_versions = store_dict.get("encoded_versions", {})
        if len(encoded_versions) > 0 and not isinstance(
            next(iter(encoded_versions)), int
        ):
            store_dict["encoded_versions"] = {
                int(k): v for k, v in store_dict["encoded_versions"].items()
            }

        if "waiting_for" in store_dict:
            store_dict["waiting_for"] = AddressEventSet(store_dict["waiting_for"])

        if type(store_dict) is Store:
            return store_dict
        return Store(store_dict)

    def serialize_event(self, event: Event) -> bytes:
        event_id: str = event.event_id
        event_type: str = event.event_type.value
        fun_address: dict = event.fun_address.to_dict()
        payload: dict = event.payload

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
        event_type = EventType.from_str(json["event_type"])
        fun_address = FunctionAddress.from_dict(json["fun_address"])
        payload: dict = json["payload"]

        if "args" in payload:
            payload["args"] = Arguments.from_dict(payload["args"])

        if "flow" in payload:
            payload["flow"] = EventFlowGraph.from_dict(payload["flow"])

        if "path" in payload:
            path: List[EventAddressTuple] = []
            for path_item in payload["path"]:
                path.append(EventAddressTuple.from_dict(path_item))
            payload["path"] = path

        if "write_set" in payload:
            payload["write_set"] = WriteSet(payload["write_set"])

        if "last_write_set" in payload:
            payload["last_write_set"] = WriteSet(payload["last_write_set"])

        return Event(event_id, fun_address, event_type, payload)

    def serialize_dict(self, dictionary: dict) -> bytes:
        return ujson.encode(dictionary, ensure_ascii=False, reject_bytes=False).encode(
            "utf-8"
        )

    def deserialize_dict(self, dictionary: bytes) -> dict:
        return ujson.decode(dictionary)
