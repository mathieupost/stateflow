from stateflow.dataflow.state import State, Store
from stateflow.serialization.json_serde import JsonSerializer


def serialized_store_to_state(serialized_store: bytes) -> State:
    store = JsonSerializer().deserialize_store(serialized_store)
    if len(store.event_version_map) == 0:
        version_id = store.last_committed_version_id
    else:
        version_id = list(store.event_version_map.items())[0][1]
    version = store.get_version(version_id)
    return version.state

def state_to_serialized_store(state: State) -> bytes:
    store = Store(initial_state=state)
    return JsonSerializer().serialize_store(store)
