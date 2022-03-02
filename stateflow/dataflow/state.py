from typing import Dict, Any, List

from stateflow.dataflow.address import FunctionAddress


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


class WriteSet(dict):
    def __init__(self, *args, **kwargs):
        super(WriteSet, self).__init__(*args, **kwargs)

    def add(self, address: FunctionAddress, version: int):
        """Adds the operator of the given FunctionAddress to the set.

        And sets the version of the operator
        """
        namespace = address.function_type.namespace
        if not namespace in self:
            self[namespace] = dict()

        operator = address.function_type.name
        if not operator in self[namespace]:
            self[namespace][operator] = dict()

        key = address.key
        self[namespace][operator][key] = version


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
