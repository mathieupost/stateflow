from typing import Dict, Any, List


class State:
    __slots__ = ["_data", "_version_id"]

    def __init__(self, data: dict, version_id: int = 0):
        self._data = data
        self._version_id = version_id

    def __getitem__(self, item):
        return self._data[item]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __str__(self):
        return str(self._data)

    def get_keys(self):
        return self._data.keys()

    def get(self):
        return self._data

    def get_version_id(self):
        return self._version_id


class StateDescriptor:
    def __init__(self, state_desc: Dict[str, Any]):
        self._state_desc = state_desc

    def get_keys(self):
        return self._state_desc.keys()

    def match(self, state: State) -> bool:
        return self.get_keys() == state.get_keys()

    def __str__(self):
        return str(list(self._state_desc.keys()))

    def __contains__(self, item):
        return item in self._state_desc

    def __getitem__(self, item):
        return self._state_desc[item]

    def __setitem__(self, key, value):
        self._state_desc[key] = value
