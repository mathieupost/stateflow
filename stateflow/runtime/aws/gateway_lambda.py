import json

from stateflow.runtime.aws.abstract_lambda import (
    AWSLambdaRuntime,
    Dataflow,
    SerDe,
    Config,
    PickleSerializer,
    Event,
    RouteDirection,
    Route,
)
import base64


class AWSGatewayLambdaRuntime(AWSLambdaRuntime):
    def __init__(
        self,
        flow: Dataflow,
        table_name="stateflow",
        gateway: bool = True,
        serializer: SerDe = PickleSerializer(),
        config: Config = Config(region_name="eu-west-2"),
    ):
        super().__init__(flow, table_name, serializer, config)
        self.gateway = gateway

    def handle(self, event, context):
        if self.gateway:
            event = json.loads(event["body"])

        event_body = event["event"]
        serialized_event = self._handle(event_body)
        encoded_event = base64.b64encode(serialized_event)

        return {
            "statusCode": 200,
            "body": json.dumps({"event": encoded_event.decode()}),
        }
