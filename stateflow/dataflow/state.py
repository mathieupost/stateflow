from typing import Dict, Any, Iterator, Optional
import jsonpickle

from stateflow.dataflow.address import FunctionAddress, FunctionType


class State:
    __slots__ = ["_data", "_version_id"]

    def __init__(self, data: dict, version_id: int = 0):
        if isinstance(data, State):
            data = data._data
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

    def copy_with_version_id(self, version_id) -> "State":
        """Copies the state, but overwrites the version_id attribute.
        """
        return State(self._data, version_id)


class WriteSet(dict):
    def __init__(self, *args, **kwargs):
        super(WriteSet, self).__init__(*args, **kwargs)

    def add(self, namespace: str, operator: str, key: str, version: int):
        """Adds the version for the given namespace, operator and key.
        
        If the version already exists, the maximum version is used.
        """
        if not namespace in self:
            self[namespace] = dict()

        if not operator in self[namespace]:
            self[namespace][operator] = dict()

        if not key in self[namespace][operator]:
            self[namespace][operator][key] = version
        else:
            self[namespace][operator][key] = max(self[namespace][operator][key], version)

    def add_address(self, address: FunctionAddress, version: int):
        """Adds the version of the given FunctionAddress to the set.
        
        If the version already exists, the maximum version is used.
        """
        namespace = address.function_type.namespace
        operator = address.function_type.name
        key = address.key or ""
        self.add(namespace, operator, key, version)

    def iterate(self) -> Iterator[Tuple[str, str, str, int]]:
        """Iterates over all (namespace, operator, key, version) tuples in the WriteSet."""
        for namespace in self:
            for operator in self[namespace]:
                for key in self[namespace][operator]:
                    yield (namespace, operator, key, self[namespace][operator][key])

    def iterate_addresses(self) -> Iterator[FunctionAddress]:
        """Iterates over all FunctionAddresses in the WriteSet."""
        for namespace, operator, key, _ in self.iterate():
            yield FunctionAddress(namespace, operator, key)

class Version:
    def __init__(self, id: int, parent_id: int, state: State) -> None:
        self.id = id
        self.parent_id = parent_id
        self.state = state
        self.write_set = None

    def create_child(self, new_id: int) -> "Version":
        """Creates a new version based on this version.

        Sets the version id to the given id and the parent id to the id of this
        version and copies the state.
        """
        parent_id = self.id
        state = self.state.copy_with_version_id(new_id)
        return Version(new_id, parent_id, state)

    def set_state(self, state: State):
        """Sets the state of this version to the given state.
        
        And updates the version id of the state.
        """
        state._version_id = self.id
        self.state = state

    def set_write_set(self, write_set: WriteSet):
        self.write_set = write_set
    
    def update(self, state: State, write_set: WriteSet):
        self.set_state(state)
        self.set_write_set(write_set)


class Store:
    def __init__(self, data=None, initial_state=None) -> None:
        """Initialized the store object from the given dict.
        
        If an initial state is provided, a new Store will be created with the
        given initial state as version 0."""
        if initial_state:
            self.encoded_versions: Dict[int, bytes] = dict()
            self.last_committed_version_id: int = 0
            self.event_version_map: Dict[str, int] = dict()
            initial_version = Version(0, -1, State(initial_state))
            self.set_version(0, initial_version)
            return

        self.encoded_versions: Dict[int, bytes] = data["encoded_versions"]
        self.last_committed_version_id: int = data["last_committed_version_id"]
        self.event_version_map: Dict[str, int] = data["event_version_map"]

    def create_new_version(self) -> Version:
        """Create a new version based on the last committed version.

        Increments the highest available version id to use as id for the new
        version.

        :return: the new version.
        """
        # Add 1 to the currently highest version id
        new_id = max(self.encoded_versions.keys()) + 1

        # Copy state from last committed state
        last_version = self.get_version(self.last_committed_version_id)
        version = last_version.create_child(new_id)

        return version

    def set_version(self, id: int, version: Version):
        """Encodes and sets the given version.

        :param id: the id of the version to set.
        :param version: the version to set.
        """
        encoded_version: bytes = jsonpickle.encode(version)
        self.encoded_versions[id] = encoded_version

    def get_version(self, id: int) -> Version:
        """Gets the encoded version with the given id and decodes it into a
        Version object.

        :param id: the id of the version to retrieve.
        :return: the version with the given id.
        """
        encoded_version = self.encoded_versions[id]
        return jsonpickle.decode(encoded_version)

    def get_last_committed_version(self) -> Version:
        """Gets the last committed version.

        :return: the last committed version.
        """
        return self.get_version(self.last_committed_version_id)

    def get_version_for_event(self, event_id: str) -> Optional[Version]:
        """Gets the version for the given event id, if it exists.
        
        :param event_id: the id of the event to get the corresponding version for.
        :return: the version for the given event id, or None if no version exists.
        """
        if event_id in self.event_version_map:
            id = self.event_version_map[event_id]
            version = self.get_version(id)
            return version
        return None

    def get_or_create_version_for_event(self, event_id: str) -> Version:
        """Returns the version for the given event id.

        If no version exists yet for the given event id, a new version will be
        created.

        :param event_id: the id of the event to get the corresponding version for.
        :return: the version for the given event id.
        """
        version = self.get_version_for_event(event_id)
        if not version:
            version = self.create_new_version()
            self.event_version_map[event_id] = version.id
        return version

    def commit_version(self, version_id):
        # TODO: check if last committed version is not changed (not different from base id)
        self.last_committed_version_id = version_id

    def commit_version_for_event_id(self, event_id):
        """Commits the version for the given event id."""
        version_id = self.event_version_map[event_id]
        self.commit_version(version_id)

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
