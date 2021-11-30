from demo_common import stateflow
from stateflow.client.graphql.kafka import KafkaGraphQLClient

client = KafkaGraphQLClient(stateflow.init())
app = client.get_app()
