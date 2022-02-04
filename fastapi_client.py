import uuid

from stateflow.client.fastapi.kafka import KafkaFastAPIClient
from demo_common import stateflow

client = KafkaFastAPIClient(stateflow.init())
app = client.get_app()
