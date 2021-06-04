from src.runtime.runtime import Runtime
from pyflink.datastream import (
    RuntimeContext,
    MapFunction,
    StreamExecutionEnvironment,
    DataStream,
)

from pyflink.datastream.connectors import (
    FlinkKafkaConsumer,
    FlinkKafkaProducer,
)
from pyflink.common import Configuration
from pyflink.util.java_utils import get_j_env_configuration, get_gateway

from pyflink.common.serialization import (
    SimpleStringSchema,
    SerializationSchema,
    DeserializationSchema,
)
from pyflink.datastream.state import ValueStateDescriptor, ValueState
from pyflink.datastream.data_stream import KeyedProcessFunction, ProcessFunction
from pyflink.common.typeinfo import Types
from src.dataflow.stateful_operator import StatefulOperator, Event
from src.dataflow.dataflow import (
    IngressRouter,
    Route,
    EgressRouter,
    Dataflow,
    RouteDirection,
)
import pyflink.lib
from src.serialization.json_serde import SerDe, JsonSerializer
from typing import Tuple, List
import uuid


class ByteSerializer(SerializationSchema, DeserializationSchema):
    def __init__(self, execution_environment):
        gate_way = get_gateway()

        j_byte_string_schema = gate_way.jvm.org.apache.flink.api.common.serialization.TypeInformationSerializationSchema(
            Types.BYTE().get_java_type_info(),
            get_j_env_configuration(execution_environment),
        )
        SerializationSchema.__init__(self, j_serialization_schema=j_byte_string_schema)
        DeserializationSchema.__init__(
            self, j_deserialization_schema=j_byte_string_schema
        )


class FlinkIngressRouter(MapFunction):
    def __init__(self, router: IngressRouter):
        self.router = router

    def map(self, value) -> Route:
        route = self.router.parse_and_route(value)
        import logging

        logging.info(f"Ingress operator, return route {route}")
        return route


class FlinkEgressRouter(MapFunction):
    def __init__(self, router: EgressRouter):
        self.router = router

    def map(self, value: Event) -> Route:
        route: Route = self.router.route_and_serialize(value)
        import logging

        logging.info(f"Egress operator, return route {route}")
        return route


class FlinkInitOperator(ProcessFunction):
    def __init__(self, operator: StatefulOperator):
        self.operator: StatefulOperator = operator

    def process_element(self, value, ctx: "ProcessFunction.Context"):
        import logging

        logging.info(f"Init operator, value {value}")
        return_event = self.operator.handle_create(value[1])
        logging.info(f"Init operator, return event {return_event}")
        yield return_event.fun_address.key, return_event


class FlinkOperator(KeyedProcessFunction):
    def __init__(self, operator: StatefulOperator):
        self.state: ValueState = None
        self.operator: StatefulOperator = operator

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("state", Types.BYTE())
        self.state: ValueState = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx: KeyedProcessFunction.Context) -> Event:
        import logging

        logging.info(
            f"Stateful operator for key {ctx.get_current_key()} with state {self.state.value()}"
        )
        return_event, updated_state = self.operator.handle(value[1], self.state.value())

        logging.info(
            f"Stateful operator, return event {return_event}, updated state {updated_state}"
        )

        if updated_state:
            self.state.update(updated_state)

        yield return_event


class FlinkRuntime(Runtime):
    def __init__(self, dataflow: Dataflow, serializer: SerDe = JsonSerializer()):
        super().__init__()
        self.dataflow = dataflow
        self.serializer = serializer
        self.env: StreamExecutionEnvironment = (
            StreamExecutionEnvironment.get_execution_environment()
        )

        self.operators = self.dataflow.operators
        self.pipeline_initialized: bool = False

        config = Configuration(
            j_configuration=get_j_env_configuration(
                self.env._j_stream_execution_environment
            )
        )
        config.set_integer("python.fn-execution.bundle.time", 1)
        config.set_integer("python.fn-execution.bundle.size", 5)

        import os

        jar_path = os.path.abspath("bin/flink-sql-connector-kafka_2.11-1.13.0.jar")
        print(f"Found jar path {jar_path}")
        self.env.add_jars(f"file://{jar_path}")

    def _setup_kafka_client(self) -> FlinkKafkaConsumer:
        return FlinkKafkaConsumer(
            ["client_request", "internal"],
            ByteSerializer(self.env._j_stream_execution_environment),
            {
                "bootstrap.servers": "localhost:9092",
                "auto.offset.reset": "latest",
                "group.id": str(uuid.uuid4()),
            },
        )

    def _setup_kafka_producer(self, topic: str) -> FlinkKafkaProducer:
        kafka_props = {"bootstrap.servers": "localhost:9092"}
        return FlinkKafkaProducer(
            topic,
            ByteSerializer(self.env._j_stream_execution_environment),
            kafka_props,
        )

    def _setup_pipeline(self):
        # Setup all Kafka communication.
        kafka_consumer: FlinkKafkaConsumer = self._setup_kafka_client()
        kafka_producer_reply: FlinkKafkaProducer = self._setup_kafka_producer(
            "client_reply"
        )
        kafka_producer_internal: FlinkKafkaProducer = self._setup_kafka_producer(
            "internal"
        )

        # Routers
        ingress_router: FlinkIngressRouter = FlinkIngressRouter(
            IngressRouter(self.serializer)
        )
        egress_router: FlinkEgressRouter = FlinkEgressRouter(
            EgressRouter(self.serializer)
        )

        # Reading all Kafka messages here
        routed_kafka_consumption: DataStream[Route] = (
            self.env.add_source(kafka_consumer)
            .map(ingress_router)
            .name("Route-Incoming-Events")
        )

        final_operator_streams: List[DataStream] = []

        for operator in self.operators:
            op_name: str = operator.function_type.get_full_name()
            stateful_operator: FlinkOperator = FlinkOperator(operator)
            init_operator: FlinkInitOperator = FlinkInitOperator(operator)

            operator_stream = (
                routed_kafka_consumption.filter(
                    lambda r: r.direction is RouteDirection.INTERNAL
                    and r.route_name == op_name
                )
                .name(f"Filter-On-{operator.function_type.name}")
                .map(lambda r: (r.key, r.value))
            )

            init_stream = (
                operator_stream.filter(lambda r: r[0] is None)
                .name(f"Filter-Init-{operator.function_type.name}")
                .process(init_operator)
                .name(f"Init-{operator.function_type.name}")
            )

            stateful_operator_stream = operator_stream.filter(
                lambda r: r[0] is not None,
            ).name(f"Filter-Stateful-{operator.function_type.name}")

            final_operator_stream = (
                stateful_operator_stream.union(init_stream)
                .key_by(lambda x: x[0])
                .process(stateful_operator)
                .name(f"Process-Stateful-{operator.function_type.name}")
            )

            final_operator_streams.append(final_operator_stream)

        egress_stream: DataStream[Route] = (
            (
                routed_kafka_consumption.filter(
                    lambda r: r.direction is RouteDirection.EGRESS
                )
                .map(lambda r: r.value)
                .union(*final_operator_streams)
            )
            .map(egress_router)
            .name(f"Map-Egress")
        )

        # Reply output
        egress_stream.filter(lambda r: r.direction is RouteDirection.CLIENT).map(
            lambda r: r.value, output_type=Types.STRING()
        ).name(f"Kafka-To-Client").add_sink(kafka_producer_reply)

        # Internal output
        egress_stream.filter(lambda r: r.direction is RouteDirection.INTERNAL).map(
            lambda r: r.value, output_type=Types.STRING()
        ).name(f"Kafka-To-Internal").add_sink(kafka_producer_internal)

        self.pipeline_initialized = True

    def run(self, async_execution=False):
        print("Now running Flink runtime!", flush=True)
        if not self.pipeline_initialized:
            self._setup_pipeline()

        if async_execution:
            self.env.execute_async("Stateflow Runtime")
        else:
            self.env.execute("Stateflow Runtime")
