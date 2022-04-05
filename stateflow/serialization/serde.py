import abc
from typing import Dict

from stateflow.dataflow.event import Event
from stateflow.dataflow.state import Store


class SerDe(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def serialize_store(self, store: Store) -> bytes:
        pass

    @abc.abstractmethod
    def deserialize_store(self, store: bytes) -> Store:
        pass

    @abc.abstractmethod
    def serialize_event(self, event: Event) -> bytes:
        pass

    @abc.abstractmethod
    def deserialize_event(self, event: bytes) -> Event:
        pass

    @abc.abstractmethod
    def serialize_dict(self, dict: Dict) -> bytes:
        pass

    @abc.abstractmethod
    def deserialize_dict(self, dict: bytes) -> Dict:
        pass
