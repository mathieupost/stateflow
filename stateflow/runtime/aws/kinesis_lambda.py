from stateflow.runtime.aws.abstract_lambda import (
    AWSLambdaRuntime,
    Route,
    RouteDirection,
    Event,
    SerDe,
    PickleSerializer,
    Dataflow,
    Config,
)
import base64
import boto3


class AWSKinesisLambdaRuntime(AWSLambdaRuntime):
    def __init__(
        self,
        flow: Dataflow,
        table_name="stateflow",
        request_stream="stateflow-request",
        reply_stream="stateflow-reply",
        serializer: SerDe = PickleSerializer(),
        config: Config = Config(region_name="eu-west-1"),
    ):
        super().__init__(flow, table_name, serializer, config)

        self.kinesis = self._setup_kinesis(config)
        self.request_stream: str = request_stream
        self.reply_stream: str = reply_stream

    def _setup_kinesis(self, config: Config):
        return boto3.client("kinesis", config=config)

    def handle(self, event, context):
        for record in event["Records"]:
            event_body = record["kinesis"]["data"]
            serialized_event = self._handle(event_body)

            self.kinesis.put_record(
                StreamName=self.reply_stream,
                Data=serialized_event,
                PartitionKey=return_route.value.event_id,
            )
