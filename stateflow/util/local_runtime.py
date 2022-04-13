import time
from queue import Queue
from typing import ByteString, Dict, Iterator

from stateflow.client.stateflow_client import (StateflowClient,
                                               StateflowFuture, T)
from stateflow.dataflow.dataflow import (Dataflow, EgressRouter, EventType,
                                         IngressRouter, Route, RouteDirection)
from stateflow.dataflow.event import Event
from stateflow.dataflow.stateful_operator import (StatefulGenerator,
                                                  StatefulOperator)
from stateflow.serialization.pickle_serializer import PickleSerializer, SerDe


class LocalRuntime(StateflowClient):
    def __init__(
        self,
        flow: Dataflow,
        serializer: SerDe = PickleSerializer(),
        return_future: bool = False,
    ):
        super().__init__(flow, serializer)

        self.flow: Dataflow = flow
        self.serializer: SerDe = serializer

        self.ingress_router = IngressRouter(self.serializer)
        self.egress_router = EgressRouter(self.serializer, serialize_on_return=False)

        self.operators = {
            operator.function_type.get_full_name(): operator
            for operator in self.flow.operators
        }

        # Set the wrapper.
        [op.meta_wrapper.set_client(self) for op in flow.operators]

        self.state: Dict[str, ByteString] = {}
        self.return_future: bool = return_future

    def invoke_operator(self, route: Route) -> Iterator[Event]:
        event: Event = route.value

        operator_name: str = route.route_name
        operator: StatefulOperator = self.operators[operator_name]

        if event.event_type == EventType.Request.InitClass and route.key is None:
            new_event = operator.handle_create(event)
            yield from self.invoke_operator(
                Route(
                    RouteDirection.INTERNAL,
                    operator_name,
                    new_event.fun_address.key,
                    new_event,
                )
            )
        else:
            full_key: str = f"{operator_name}_{route.key}"
            operator_state = self.state.get(full_key)

            handler = StatefulGenerator(operator.handle(event, operator_state))
            yield from handler

            self.state[full_key] = handler.state

    def handle_invocation(self, event: Event) -> Iterator[Route]:
        route: Route = self.ingress_router.route(event)

        if route.direction == RouteDirection.INTERNAL:
            for event in self.invoke_operator(route):
                yield self.egress_router.route_and_serialize(event)
        elif route.direction == RouteDirection.EGRESS:
            yield self.egress_router.route_and_serialize(route.value)
        else:
            return route

    def execute_event(self, event: Event) -> Event:
        parsed_event: Event = self.ingress_router.parse(event)
        
        event_queue: Queue = Queue()
        event_queue.put(parsed_event)

        while not event_queue.empty():
            event = event_queue.get()
            for route in self.handle_invocation(event):
                if route.direction == RouteDirection.CLIENT:
                    return_event = route.value
                else:
                    event_queue.put(route.value)

        return return_event

    def send(self, event: Event, return_type: T = None) -> T:
        return_event = self.execute_event(self.serializer.serialize_event(event))
        future = StateflowFuture(
            event.event_id, time.time(), event.fun_address, return_type
        )

        future.complete(return_event)
        if self.return_future:
            return future
        else:
            return future.get()
