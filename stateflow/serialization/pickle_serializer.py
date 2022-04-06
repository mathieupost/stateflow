import pickle
from typing import Dict

from stateflow.dataflow.event import Event
from stateflow.dataflow.state import Store
from stateflow.serialization.serde import SerDe


class PickleSerializer(SerDe):
    def serialize_store(self, store: Store) -> bytes:
        return pickle.dumps(store)

    def deserialize_store(self, store: bytes) -> Store:
        return pickle.loads(store)

    def serialize_event(self, event: Event) -> bytes:
        return pickle.dumps(event)

    def deserialize_event(self, event: bytes) -> Event:
        return pickle.loads(event)

    def serialize_dict(self, dictionary: Dict) -> bytes:
        return pickle.dumps(dictionary)

    def deserialize_dict(self, dictionary: bytes) -> Dict:
        return pickle.loads(dictionary)
