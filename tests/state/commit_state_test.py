import copy
import uuid
from typing import List

import pytest
from stateflow.dataflow import stateful_operator
from stateflow.dataflow.address import FunctionAddress, FunctionType
from stateflow.dataflow.args import Arguments
from stateflow.dataflow.dataflow import Dataflow, EgressRouter, IngressRouter, Operator
from stateflow.dataflow.event import Event, EventType
from stateflow.dataflow.event_flow import (
    EventFlowGraph,
    EventFlowNode,
    InternalClassRef,
    InvokeSplitFun,
)
from stateflow.dataflow.state import EventAddressTuple, Store, WriteSet
from stateflow.dataflow.stateful_operator import (
    IsolationType,
    StatefulOperator,
)
from stateflow.descriptors.class_descriptor import ClassDescriptor
from stateflow.serialization.json_serde import JsonSerializer
from tests.common.common_classes import stateflow


class TestCommitState:
    serializer = JsonSerializer()
    ingress = IngressRouter(serializer)
    egress = EgressRouter(serializer, serialize_on_return=False)

    def step(self, model: "Model", event: Event) -> List[Event]:
        operator = model.operator
        ingress: IngressRouter = self.ingress
        egress: EgressRouter = self.egress

        route = ingress.route(event)
        serialized_store = model.get_serialized_store()
        handler = operator.handle(route.value, serialized_store)

        events = list(handler)
        if handler.return_value is not None:
            model.update_store(handler.return_value)

        if len(events) == 0:
            return events

        for i, event in enumerate(events):
            route = egress.route_and_serialize(event)
            events[i] = route.value

        return events

    @pytest.mark.parametrize("isolation_mode", IsolationType)
    def test__all_isolation__single_transaction(
        self,
        isolation_mode: IsolationType,
        user1: "Model",
        user2: "Model",
        transfer_balance_event1,
    ):
        stateful_operator.IsolationMode = isolation_mode

        initial_state_u1 = user1.store.get_last_committed_version().state.get()
        initial_state_u2 = user2.store.get_last_committed_version().state.get()

        # INVOKE_SPLIT_FUN User(user1).transfer_balance_0(...)
        events = self.step(user1, transfer_balance_event1)
        assert len(events) == 1
        assert events[0].event_type == EventType.Request.EventFlow
        expected_write_set_1 = {
            "global": {
                "User": {
                    "user1": 1,
                }
            }
        }
        assert events[0].payload["write_set"] == expected_write_set_1
        expected_last_write_set_1 = {
            "global": {
                "User": {
                    "user1": 0,
                }
            }
        }
        assert events[0].payload["last_write_set"] == expected_last_write_set_1
        assert user1.store.last_committed_version_id == 0

        # INVOKE_EXTERNAL User(user2).update_balance(...)
        events = self.step(user2, events[0])
        assert len(events) == 1
        assert events[0].event_type == EventType.Request.EventFlow
        expected_write_set_2 = {
            "global": {
                "User": {
                    "user1": 1,
                    "user2": 1,
                }
            }
        }
        assert events[0].payload["write_set"] == expected_write_set_2
        expected_last_write_set_2 = {
            "global": {
                "User": {
                    "user1": 0,
                    "user2": 0,
                }
            }
        }
        assert events[0].payload["last_write_set"] == expected_last_write_set_2
        assert user2.store.last_committed_version_id == 0
        updated_state = user2.get_state(events[0])
        assert updated_state["balance"] - initial_state_u2["balance"] == 10

        # INVOKE_SPLIT_FUN User(user1).transfer_balance_7(...)
        events = self.step(user1, events[0])
        assert len(events) == 2
        # COMMIT_STATE User(user2)
        assert events[0].event_type == EventType.Request.CommitState
        assert events[0].payload["write_set"] == expected_write_set_2
        assert events[1].event_type == EventType.Reply.SuccessfulInvocation
        assert events[1].payload["return_results"] == [True]
        # Version was committed.
        assert user1.store.last_committed_version_id == 1
        updated_state = user1.get_state(events[0])
        assert updated_state["balance"] - initial_state_u1["balance"] == -10

        # COMMIT_STATE User(user2)
        _ = self.step(user2, events[0])
        # Version was committed.
        assert user2.store.last_committed_version_id == 1

        item_write_set = user1.store.get_last_committed_version().write_set
        user_write_set = user2.store.get_last_committed_version().write_set
        assert item_write_set == user_write_set

    # Tests a scenario where a user1 transfers money to a user2, but before
    # the user2's state is committed, the user1 starts another transfer.
    # Since the user1's state is committed before the 2nd transfer starts, the
    # 2nd transfer can detect and use the new user2 state.
    @pytest.mark.parametrize("isolation_mode", IsolationType)
    def test__all_isolation__read_skew__direct_detect(
        self,
        isolation_mode: IsolationType,
        user1: "Model",
        user2: "Model",
        transfer_balance_event1: Event,
        transfer_balance_event2: Event,
    ):
        stateful_operator.IsolationMode = isolation_mode

        ########## Begin 1st transaction ##########
        # INVOKE_SPLIT_FUN User(user1).transfer_balance_0(...)
        tr1_events = self.step(user1, transfer_balance_event1)

        # INVOKE_EXTERNAL User(user2).update_balance(...)
        tr1_events = self.step(user2, tr1_events[0])

        # INVOKE_SPLIT_FUN User(user1).transfer_balance_7(...)
        tr1_events = self.step(user1, tr1_events[0])
        # User(user1) is committed now.
        assert user1.store.last_committed_version_id == 1

        if True:  # indented for readability
            ########## Begin 2nd transaction ##########
            tr2_event_id = transfer_balance_event2.event_id

            # INVOKE_SPLIT_FUN User(user1).transfer_balance_0(...)
            # Gets committed version from 1st transaction.
            tr2_events = self.step(user1, transfer_balance_event2)
            u1_version = user1.store.get_version_for_event_id(tr2_event_id)
            assert user1.store.last_committed_version_id == u1_version.parent_id
            assert u1_version.parent_id == 1
            assert u1_version.id == 2
            # The event's last_write_set should have been set to the write_set
            # of the last committed version.
            last_write_set = tr2_events[0].payload["last_write_set"]
            assert last_write_set == user1.store.get_last_committed_version().write_set
            assert last_write_set == {
                "global": {
                    "User": {
                        "user2": 1,
                        "user1": 1,
                    }
                }
            }

            # INVOKE_EXTERNAL User(user2).update_balance(...)
            # Should get uncommitted (newer) state from 1st transaction, because
            # of detected new version (user2: 1) in the event's last_write_set.
            tr2_events = self.step(user2, tr2_events[0])
            u2_version = user2.store.get_version_for_event_id(tr2_event_id)
            assert user2.store.last_committed_version_id < u2_version.parent_id
            assert u2_version.parent_id == 1
            assert u2_version.id == 2

            # INVOKE_SPLIT_FUN User(user1).transfer_balance_7(...)
            tr2_events = self.step(user1, tr2_events[0])

            # COMMIT_STATE User(user2)
            _ = self.step(user2, tr2_events[0])
            assert user2.store.last_committed_version_id == 2
            ########## End 2nd transaction ##########

        # COMMIT_STATE User(user2)
        _ = self.step(user2, tr1_events[0])
        # Check if the CommitState event did NOT overwrite the committed version.
        assert user2.store.last_committed_version_id == 2
        ########## End 1st transaction ##########

    def test__all_isolation__read_skew__late_detect(
        self,
        user1: "Model",
        user2: "Model",
        transfer_balance_event1: Event,
        transfer_balance_event_reverse: Event,
    ):
        stateful_operator.IsolationMode = IsolationType.TwoPhaseCommit

        ########## Begin 1st transaction ##########
        # INVOKE_SPLIT_FUN User(user1).transfer_balance_0(...)
        tr1_events = self.step(user1, transfer_balance_event1)

        # INVOKE_EXTERNAL User(user2).update_balance(...)
        tr1_events = self.step(user2, tr1_events[0])

        # INVOKE_SPLIT_FUN User(user1).transfer_balance_7(...)
        tr1_events = self.step(user1, tr1_events[0])
        # User(user1) is committed now.
        assert user1.store.last_committed_version_id == 1

        if True:  # indented for readability
            ########## Begin 2nd transaction ##########
            tr2_event_id = transfer_balance_event_reverse.event_id

            # INVOKE_SPLIT_FUN User(user2).transfer_balance_0(...)
            # Doesn't get uncommitted version from 1st transaction.
            tr2_events = self.step(user2, transfer_balance_event_reverse)
            u2_version = user2.store.get_version_for_event_id(tr2_event_id)
            assert user2.store.last_committed_version_id == 0
            # Created from last committed version.
            assert u2_version.parent_id == 0
            assert u2_version.id == 2

            # INVOKE_EXTERNAL User(user1).update_balance(...) Should detect Read
            # Skew and retry the transaction, because of detected new version
            # (user2: 1) in the user1's last_write_set.
            tr2_events = self.step(user1, tr2_events[0])
            assert tr2_events[0].payload["retries"] == 1
            assert tr2_events[0].payload["last_write_set"] == {
                "global": {
                    "User": {
                        "user2": 1,
                        "user1": 1,
                    }
                }
            }

            # INVOKE_SPLIT_FUN User(user2).transfer_balance_0(...)
            # Gets uncommitted version from 1st transaction.
            tr2_events = self.step(user2, tr2_events[0])
            u2_version = user2.store.get_version_for_event_id(tr2_event_id)
            assert user2.store.last_committed_version_id == 0
            # Created from uncomitted version.
            assert u2_version.parent_id == 1
            assert u2_version.id == 2

            # INVOKE_EXTERNAL User(user1).update_balance(...)
            tr2_events = self.step(user1, tr2_events[0])

            # INVOKE_SPLIT_FUN User(user2).transfer_balance_7(...)
            tr2_events = self.step(user2, tr2_events[0])
            # User(user2) is committed now.
            assert user2.store.last_committed_version_id == 2

            # COMMIT_STATE User(user1)
            _ = self.step(user1, tr2_events[0])
            assert user1.store.last_committed_version_id == 2
            ########## End 2nd transaction ##########

        # COMMIT_STATE User(user2)
        _ = self.step(user2, tr1_events[0])
        # Check if the CommitState event did NOT overwrite the committed version.
        assert user2.store.last_committed_version_id == 2
        ########## End 1st transaction ##########

    def test__abort_isolation__transfer_same(
        self,
        user1: "Model",
        user2: "Model",
        transfer_balance_event1: Event,
        transfer_balance_event2: Event,
    ):
        stateful_operator.IsolationMode = IsolationType.Abort

        ########## Begin 1st transaction ##########
        # INVOKE_SPLIT_FUN User(user1).transfer_balance_0(...)
        tr1_events = self.step(user1, transfer_balance_event1)

        # INVOKE_EXTERNAL User(user2).update_balance(...)
        tr1_events = self.step(user2, tr1_events[0])

        if True:  # indented for readability
            ########## Begin 2nd transaction ##########
            tr2_event_id = transfer_balance_event2.event_id

            # INVOKE_SPLIT_FUN User(user1).transfer_balance_0(...)
            # Detects uncomitted version of User(user2) and returns the same
            # event to retry.
            tr2_events = self.step(user1, transfer_balance_event2)
            assert tr2_events[0].payload["retries"] == 1
            current_node: EventFlowNode = tr2_events[0].payload["flow"].current_node
            assert isinstance(current_node, InvokeSplitFun)
            assert current_node.fun_name == "transfer_balance_0"

        ########## Resume 1st transaction ##########
        # INVOKE_SPLIT_FUN User(user1).transfer_balance_7(...)
        tr1_events = self.step(user1, tr1_events[0])
        # User(user1) is committed now.
        assert user1.store.last_committed_version_id == 1

        # COMMIT_STATE User(user2)
        _ = self.step(user2, tr1_events[0])
        assert user2.store.last_committed_version_id == 1
        ########## End 1st transaction ##########

        if True:
            ########## Resume 2nd transaction ##########
            tr2_events = self.step(user1, tr2_events[0])
            u1_version = user1.store.get_version_for_event_id(tr2_event_id)
            assert user1.store.last_committed_version_id == u1_version.parent_id
            assert u1_version.parent_id == 1
            assert u1_version.id == 2

            # INVOKE_EXTERNAL User(user2).update_balance(...)
            tr2_events = self.step(user2, tr2_events[0])
            u2_version = user2.store.get_version_for_event_id(tr2_event_id)
            assert user2.store.last_committed_version_id == u2_version.parent_id
            assert u2_version.parent_id == 1
            assert u2_version.id == 2

            # INVOKE_SPLIT_FUN User(user1).transfer_balance_7(...)
            tr2_events = self.step(user1, tr2_events[0])
            # User(user1) is committed now.
            assert user1.store.last_committed_version_id == 2

            # COMMIT_STATE User(user2)
            _ = self.step(user2, tr2_events[0])
            assert user2.store.last_committed_version_id == 2
            ########## End 2nd transaction ##########

    def test__abort_isolation__transfer_reverse(
        self,
        user1: "Model",
        user2: "Model",
        transfer_balance_event1: Event,
        transfer_balance_event_reverse: Event,
    ):
        stateful_operator.IsolationMode = IsolationType.Abort

        ########## Begin 1st transaction ##########
        # INVOKE_SPLIT_FUN User(user1).transfer_balance_0(...)
        tr1_events = self.step(user1, transfer_balance_event1)
        # INVOKE_EXTERNAL User(user2).update_balance(...)
        tr1_events = self.step(user2, tr1_events[0])
        # INVOKE_SPLIT_FUN User(user1).transfer_balance_7(...)
        tr1_events = self.step(user1, tr1_events[0])
        # User(user1) is committed now.
        assert user1.store.last_committed_version_id == 1

        if True:  # indented for readability
            ########## Begin 2nd transaction ##########
            tr2_event_id = transfer_balance_event_reverse.event_id
            # INVOKE_SPLIT_FUN User(user2).transfer_balance_0(...)
            # Detects uncomitted version of User(user2) and returns the same
            # event to retry with a "retries" counter defined.
            tr2_events = self.step(user2, transfer_balance_event_reverse)
            assert tr2_events[0].payload["retries"] == 1
            current_node: EventFlowNode = tr2_events[0].payload["flow"].current_node
            assert isinstance(current_node, InvokeSplitFun)
            assert current_node.fun_name == "transfer_balance_0"

        ########## Resume 1st transaction ##########
        # COMMIT_STATE User(user2)
        _ = self.step(user2, tr1_events[0])
        # User(user2) is committed now.
        assert user2.store.last_committed_version_id == 1
        ########## End 1st transaction ##########

        if True:
            ########## Resume 2nd transaction ##########
            # Transaction 2 can continue. Retry the
            # transfer_balance_event_reverse event from above.
            tr2_events = self.step(user2, tr2_events[0])
            u2_version = user2.store.get_version_for_event_id(tr2_event_id)
            assert u2_version.parent_id == user2.store.last_committed_version_id
            assert u2_version.parent_id == 1
            assert u2_version.id == 2

            # INVOKE_EXTERNAL User(user1).update_balance(...)
            tr2_events = self.step(user1, tr2_events[0])

            # INVOKE_SPLIT_FUN User(user2).transfer_balance_7(...)
            tr2_events = self.step(user2, tr2_events[0])
            # User(user2) is committed now.
            assert user2.store.last_committed_version_id == 2

            # COMMIT_STATE User(user1)
            _ = self.step(user1, tr2_events[0])
            # User(user1) is committed now.
            assert user1.store.last_committed_version_id == 2
            ########## End 2nd transaction ##########

    def test__isolation_queue__deadlock(
        self,
        user1: "Model",
        user2: "Model",
        transfer_balance_event1: Event,
        transfer_balance_event_reverse: Event,
    ):
        stateful_operator.IsolationMode = IsolationType.Queue

        # Set the event id to a value that will always be higher than a
        # generated uuid, because the event with the lowest id will be aborted.
        tr1_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        transfer_balance_event1.event_id = tr1_id

        ########## Begin 1st transaction ##########
        # INVOKE_SPLIT_FUN User(user1).transfer_balance_0(...)
        tr1_events = self.step(user1, transfer_balance_event1)
        # User1 waits for event with tr1_id to return from user2
        assert user1.store.waiting_for.get_one() == (user2.fun_addr, tr1_id)

        if True:  # indented for readability
            ########## Begin 2nd transaction ##########
            tr2_id = transfer_balance_event_reverse.event_id
            # INVOKE_SPLIT_FUN User(user2).transfer_balance_0(...)
            tr2_events = self.step(user2, transfer_balance_event_reverse)
            # User2 waits for event with tr2_id to return from user1
            assert user2.store.waiting_for.get_one() == (user1.fun_addr, tr2_id)

        ########## Resume 1st transaction ##########
        # INVOKE_EXTERNAL User(user2).update_balance(...)
        tr1_events = self.step(user2, tr1_events[0])
        # Should've detected and resolved the deadlock check locally and
        # continue with the transaction.
        assert tr1_events[0].event_type == EventType.Request.EventFlow
        assert tr1_events[0].payload["flow"].current_node.previous > 0
        assert tr1_events[0].payload.get("retries", 0) == 0  # Not retried

        if True:
            ########## Resume 2nd transaction ##########
            # INVOKE_EXTERNAL User(user1).update_balance(...)
            tr2_events = self.step(user1, tr2_events[0])
            # Should've detected and resolved the deadlock check locally and
            # resetted the transaction to try again.
            assert tr2_events[0].event_type == EventType.Request.EventFlow
            assert tr2_events[0].payload.get("retries", 0) == 1
            assert (
                tr2_events[0].payload["flow"].current_node.previous == 0
            )  # Back at the start
            assert tr2_events[0].payload.get("last_write_set", WriteSet()) == {
                "global": {"User": {"user2": 0}}
            }

        ########## DEADLOCK ##########

    def test__isolation_queue__deadlock_triple(
        self,
        user1: "Model",
        user2: "Model",
        user3: "Model",
        transfer_balance_flow: List[EventFlowNode],
    ):
        stateful_operator.IsolationMode = IsolationType.Queue

        # Create events that will cause deadlock.
        # Transfer balance from user1 -> user2
        event1 = transfer_balance_event(user1, user2, transfer_balance_flow)
        # Transfer balance from user2 -> user3
        event2 = transfer_balance_event(user2, user3, transfer_balance_flow)
        # Transfer balance from user3 -> user1
        event3 = transfer_balance_event(user3, user1, transfer_balance_flow)

        # Set transaction/event id's. Lowest id should be aborted
        t1_id = event1.event_id = "11111111-1111-1111-1111-111111111111"
        t2_id = event2.event_id = "22222222-2222-2222-2222-222222222222"
        t3_id = event3.event_id = "33333333-3333-3333-3333-333333333333"

        # Shorthands for asserting paths.
        # "user1_tr1" => "user1 is waiting for t1", etc.
        user1_t1 = EventAddressTuple(t1_id, user1.fun_addr)
        user2_t2 = EventAddressTuple(t2_id, user2.fun_addr)
        user3_t3 = EventAddressTuple(t3_id, user3.fun_addr)

        # Begin transaction in all 3 users
        t1_events = self.step(user1, event1)
        t2_events = self.step(user2, event2)
        t3_events = self.step(user3, event3)

        # Assert updated path
        assert len(t1_events[0].payload["path"]) == 1
        assert len(t2_events[0].payload["path"]) == 1
        assert len(t3_events[0].payload["path"]) == 1
        assert t1_events[0].payload["path"][-1] == user1_t1
        assert t2_events[0].payload["path"][-1] == user2_t2
        assert t3_events[0].payload["path"][-1] == user3_t3

        # Continue tansaction in receiving users
        t1_events = self.step(user2, t1_events[0])
        t2_events = self.step(user3, t2_events[0])
        t3_events = self.step(user1, t3_events[0])

        # Confirm all tansactions detected a deadlock
        assert t1_events[0].event_type == EventType.Request.DeadlockCheck
        assert t2_events[0].event_type == EventType.Request.DeadlockCheck
        assert t3_events[0].event_type == EventType.Request.DeadlockCheck

        # Assert updated path
        assert len(t1_events[0].payload["path"]) == 2
        assert len(t2_events[0].payload["path"]) == 2
        assert len(t3_events[0].payload["path"]) == 2
        assert t1_events[0].payload["path"][-1] == user2_t2
        assert t2_events[0].payload["path"][-1] == user3_t3
        assert t3_events[0].payload["path"][-1] == user1_t1

        # Continue deadlock check in the operator the previous operator was
        # waiting for.
        t1_events = self.step(user3, t1_events[0])
        t2_events = self.step(user1, t2_events[0])
        t3_events = self.step(user2, t3_events[0])

        # t1 should be aborted.
        assert len(t1_events) == 0
        # t2 should abort t1 in user1 and pop t3 from the queue and continue
        # with that.
        assert len(t2_events) == 1
        assert t2_events[0].event_id == t3_id
        event3 = t2_events[0]
        assert event3.payload["flow"].current_node.fun_addr == user3.fun_addr
        # t3 should pop t1 from the queue and reset it to retry.
        assert len(t3_events) == 1
        assert t3_events[0].event_id == t1_id
        event1 = t3_events[0]
        assert event1.payload["flow"].current_node.fun_addr == user1.fun_addr
        assert event1.payload["retries"] == 1

        # The transactions will now divert in their behaviour, so we'll use an
        # EventBus comment to keep track of the events.

        ########## EventBus = [event3, event1] ##########

        # Continue t3 (which was popped from the queue by t2) in user3.
        t3_events = self.step(user3, event3)

        # Should finish t3 and continue with t2 from the queue.
        assert len(t3_events) == 3
        # Commit t3 in user1.
        assert t3_events[0].event_id == t3_id
        t3_commit_user1 = t3_events[0]
        assert t3_commit_user1.event_type == EventType.Request.CommitState
        assert t3_commit_user1.fun_address == user1.fun_addr
        # Return t3 result to client.
        assert t3_events[1].event_id == t3_id
        assert t3_events[1].event_type == EventType.Reply.SuccessfulInvocation
        # Continue t2 in user2.
        assert t3_events[2].event_id == t2_id
        event2 = t3_events[2]
        assert event2.event_type == EventType.Request.EventFlow
        assert event2.payload["flow"].current_node.fun_addr == user2.fun_addr

        ########## EventBus = [event1, t3_commit_user1, event2] ##########

        # Retry t1 (which was popped from the queue by t3) in user1.
        t1_events = self.step(user1, event1)

        # t1 is queued again because user1 still has to commit. But no
        # DeadlockCheck should be sent, since the path is still empty. I.e. no
        # other operators are waiting for t1.
        assert len(t1_events) == 0

        ########## EventBus = [t3_commit_user1, event2] ##########

        # Commit t3 in user1.
        t3_events = self.step(user1, t3_commit_user1)

        # Committed t3 and should pop t1 from the queue.
        assert len(t3_events) == 1
        assert t3_events[0].event_id == t1_id
        event1 = t3_events[0]
        assert event1.event_type == EventType.Request.EventFlow
        assert event1.payload["flow"].current_node.fun_addr == user2.fun_addr

        ########## EventBus = [event2, event1] ##########

        # Continue t2 in user2.
        t2_events = self.step(user2, event2)

        # Should finish t2.
        assert len(t2_events) == 2
        # Commit t2 in user3.
        assert t2_events[0].event_id == t2_id
        t2_commit_user3 = t2_events[0]
        assert t2_commit_user3.event_type == EventType.Request.CommitState
        assert t2_commit_user3.fun_address == user3.fun_addr
        # Return t2 result to client.
        assert t2_events[1].event_id == t2_id
        assert t2_events[1].event_type == EventType.Reply.SuccessfulInvocation

        ########## EventBus = [event1, t2_commit_user3] ##########

        # Continue t1 in user2.
        t1_events = self.step(user2, event1)

        # Should continue t1 in user1.
        assert len(t1_events) == 1
        assert t1_events[0].event_id == t1_id
        event1 = t1_events[0]
        assert event1.event_type == EventType.Request.EventFlow
        assert event1.payload["flow"].current_node.fun_addr == user1.fun_addr

        ########## EventBus = [t2_commit_user3, event1] ##########

        # Commit t2 in user3.
        t2_events = self.step(user3, t2_commit_user3)

        # Committed t2
        assert len(t2_events) == 0

        ########## EventBus = [event1] ##########

        # Continue t1 in user1.
        t1_events = self.step(user1, event1)

        # Should finish t1.
        assert len(t1_events) == 2
        # Commit t1 in user2.
        assert t1_events[0].event_id == t1_id
        t1_commit_user2 = t1_events[0]
        assert t1_commit_user2.event_type == EventType.Request.CommitState
        assert t1_commit_user2.fun_address == user2.fun_addr
        # Return t1 result to client.
        assert t1_events[1].event_id == t1_id
        assert t1_events[1].event_type == EventType.Reply.SuccessfulInvocation

        ########## EventBus = [t1_commit_user2] ##########

        # Commit t1 in user2.
        t1_events = self.step(user2, t1_commit_user2)

        # Committed t1
        assert len(t1_events) == 0

        ########## EventBus = [] ##########


@pytest.fixture(scope="class")
def flow() -> Dataflow:
    return stateflow.init()


@pytest.fixture(scope="function")
def user1(flow: Dataflow):
    initial_state = {"username": "user1", "balance": 20, "items": []}
    return Model(flow.operators[1], "user1", initial_state)


@pytest.fixture(scope="function")
def user2(flow: Dataflow):
    initial_state = {"username": "user2", "balance": 10, "items": []}
    return Model(flow.operators[1], "user2", initial_state)


@pytest.fixture(scope="function")
def user3(flow: Dataflow):
    initial_state = {"username": "user3", "balance": 10, "items": []}
    return Model(flow.operators[1], "user3", initial_state)


@pytest.fixture(scope="class")
def transfer_balance_flow(flow):
    cls_descriptor: ClassDescriptor = flow.operators[1].class_wrapper.class_desc
    for method in cls_descriptor.methods_dec:
        if method.method_name == "transfer_balance":
            break
    return method.flow_list


def transfer_balance_event(user1, user2, transfer_balance_flow):
    args = {
        "receiver": InternalClassRef(user2.fun_addr),
        "amount": 10,
    }
    return Event(
        str(uuid.uuid4()),
        user1.fun_addr,
        EventType.Request.EventFlow,
        {
            "flow": EventFlowGraph.construct_and_assign_arguments(
                copy.deepcopy(transfer_balance_flow),
                user1.fun_addr,
                Arguments(args),
            )
        },
    )


@pytest.fixture(scope="function")
def transfer_balance_event1(user1, user2, transfer_balance_flow):
    return transfer_balance_event(user1, user2, transfer_balance_flow)


@pytest.fixture(scope="function")
def transfer_balance_event2(user1, user2, transfer_balance_flow):
    return transfer_balance_event(user1, user2, transfer_balance_flow)


@pytest.fixture(scope="function")
def transfer_balance_event_reverse(user1, user2, transfer_balance_flow):
    # Swap user1 and user2
    return transfer_balance_event(user2, user1, transfer_balance_flow)


class Model:
    def __init__(self, operator: Operator, key: str, initial_state: dict):
        assert isinstance(operator, StatefulOperator)

        self.serializer = JsonSerializer()
        operator.serializer = self.serializer
        name = operator.class_wrapper.class_desc.class_name

        self.operator: StatefulOperator = operator
        self.store = Store(initial_state=initial_state)
        self.fun_addr = FunctionAddress(
            FunctionType("global", name, True),
            key,
        )

    def update_store(self, serialized_store: bytes):
        self.store = self.serializer.deserialize_store(serialized_store)

    def get_serialized_store(self) -> bytes:
        return self.serializer.serialize_store(self.store)

    def get_state(self, event: Event) -> dict:
        event_id = event.event_id
        return self.store.get_version_for_event_id(event_id).state.get()
