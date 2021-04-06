from apache_beam import DoFn
from apache_beam.coders import StrUtf8Coder
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
import apache_beam as beam
from src.dataflow import StatefulOperator
from typing import List
import apache_beam.io.kafka as kafka

from runtime.runtime import Runtime
from src.dataflow import Dataflow
from src.dataflow import State, Event


class BeamRouter(DoFn):
    def process(self, element: str) -> List[Event]:
        event: Event = Event.deserialize(element)

        return [event]


class BeamInitOperator(DoFn):
    def __init__(self, operator: StatefulOperator):
        self.operator = operator

    def process(self, element: Event) -> List[Event]:
        return_event = self.operator.handle_create(element)
        return [return_event]


class BeamOperator(DoFn):

    STATE_SPEC = ReadModifyWriteStateSpec("state", StrUtf8Coder())

    def __init__(self, operator: StatefulOperator):
        self.operator = operator

    def process(
        self, element, operator_state=DoFn.StateParam(STATE_SPEC)
    ) -> List[Event]:
        if operator_state.read() is not None:
            state_decoded = State.deserialize(operator_state.read())
        else:
            state_decoded = None

        # Execute event.
        operator_return, updated_state = self.operator.handle(element[1], state_decoded)

        # Update state.
        if updated_state is not None:
            state_encoded = State.serialize(updated_state)
            operator_state.write(state_encoded)

        return [operator_return]


class BeamRuntime(Runtime):
    def __init__(self):
        self.init_operators: List[BeamInitOperator] = []
        self.operators: List[BeamOperator] = []
        self.router = BeamRouter()

    def transform(self, dataflow: Dataflow):
        for operator in dataflow.operators:
            self.init_operators.append(BeamInitOperator(operator))
            self.operators.append(BeamOperator(operator))

    def run(self, events):
        print("Running Beam pipeline!")

        with beam.Pipeline() as pipeline:
            kafka_client = kafka.ReadFromKafka(
                {"bootstrap.servers": "localhost:9092"},
                ["client_request"],
                key_deserializer="org.apache.kafka.common. serialization.StringDeserializer",
                value_deserializer="org.apache.kafka.common. serialization.StringDeserializer",
            )

            # Read from Kafka
            input_kafka = pipeline | kafka_client

            # Forward to init stateful operator
            init_stateful = (
                input_kafka
                | beam.ParDo(self.router)
                | beam.ParDo(self.init_operators[0])
                | "Print" >> beam.Map(print)
            )
