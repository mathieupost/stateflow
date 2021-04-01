import stateflow
from src.example.shop import User
from src.dataflow.state import State
from src.dataflow.args import Arguments
from src.wrappers.class_wrapper import FailedInvocation, ClassWrapper
from src.runtime.beam_runtime import BeamRuntime
from src.dataflow.event import Event, EventType, FunctionType
from client.class_ref import ClassRef
from src.client.kafka_client import StateflowKafkaClient, StateflowClient
import time

start = time.perf_counter()


@stateflow.stateflow
class Fun:
    def __init__(self, username: str):
        self.x = 3
        self.username = username

    def update_x(self, delta_x: int) -> int:
        self.x -= delta_x
        return self.x

    def __key__(self):
        return self.username


print(Fun)
flow = stateflow.init()

client: StateflowClient = StateflowKafkaClient(flow)


fun = Fun("wouter")
print(fun.username)
# fun.username
# fun_type = FunctionType("global", "Fun", True)
# class_descriptor = flow.get_descriptor_by_type(FunctionType("global", "Fun", True))
# class_ref = ClassRef(fun_type, class_descriptor, Fun)
#
# class_ref.update_x("hoi", 123)
#
# end = time.perf_counter()
# print((end - start) * 1000)
# print(class_descriptor)
# operator = BeamRuntime()
# operator.transform(flow)
# operator.run(
#     [
#         (
#             "wouter",
#             Event(
#                 None,
#                 EventType.Request.value.InvokeStateful,
#                 Arguments({"username": "wouter"}),
#             ),
#         )
#     ]
# )


# wrapper = stateflow.registered_classes[0]
#
# # Create the class
# result = wrapper.init_class(Arguments({"username": "wouter"}))
#
# print(
#     f"Created class with key: {result.return_results} and state {result.updated_state}."
# )
# print()
#
# # Returns failed invocation
# print(wrapper.invoke("LOL", None, None))
# print()
#
# # Invoke delta x
# state = result.updated_state
# args = Arguments({"delta_x": 5})
# result = wrapper.invoke("update_x", state, args)
#
# print(
#     f"Invoked method update_x.\nUpdated state {result.updated_state}.\nResult: {result.return_results}"
# )
