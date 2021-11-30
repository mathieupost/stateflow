from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from demo_common import stateflow
from stateflow.runtime.beam_runtime import BeamRuntime
from stateflow.serialization.pickle_serializer import PickleSerializer

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)
topic_list = [
    NewTopic(name="client_request", num_partitions=1, replication_factor=1),
    NewTopic(name="internal", num_partitions=1, replication_factor=1),
]
try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
except TopicAlreadyExistsError:
    pass

# Initialize stateflow
flow = stateflow.init()

runtime: BeamRuntime = BeamRuntime(flow, serializer=PickleSerializer())
runtime.run()
