import uuid
from typing import List
from tests.context import stateflow


@stateflow.stateflow
class Item:
    def __init__(self, item_name: str, price: int):
        self.item_name: str = item_name
        self.stock: int = 0
        self.price: int = price

    def update_stock(self, amount: int) -> bool:
        if (self.stock + amount) < 0:  # We can't get a stock < 0.
            return False

        self.stock += amount
        return True

    def __key__(self):
        return self.item_name


@stateflow.stateflow
class User:
    def __init__(self, username: str):
        self.username: str = username
        self.balance: int = 0
        self.items: List[Item] = []

    def update_balance(self, x: int):
        self.balance += x

    def transfer_balance(self, receiver: "User", amount: int) -> bool:
        if amount < 0:
            return False

        if self.balance < amount:
            return False

        self.update_balance(-amount)
        receiver.update_balance(amount)
        return True

    def buy_item(self, amount: int, item: Item) -> bool:
        total_price = amount * item.price

        if self.balance < total_price:
            return False

        if not item.update_stock(-amount):
            return False  # For some reason, stock couldn't be decreased.

        self.balance -= total_price
        return True

    def simple_for_loops(self, users: List["User"]):
        i = 0
        for user in users:
            if i > 0:
                user.update_balance(9)
            else:
                user.update_balance(4)
            i += 1

        return i

    def __key__(self):
        return self.username


@stateflow.stateflow
class ExperimentalB:
    def __init__(self, name: str):
        self.name = name
        self.balance = 0

    def add_balance(self, balance: int):
        self.balance += balance

    def set_balance(self, balance: int):
        self.balance = balance

    def balance_equal_to(self, equal_balance: int) -> bool:
        return self.balance == equal_balance

    def __key__(self):
        return self.name


@stateflow.stateflow
class ExperimentalA:
    def __init__(self, name: str):
        self.name = name
        self.balance = 0

    def complex_method(self, balance: int, other: ExperimentalB) -> bool:
        self.balance += balance * 2
        other.add_balance(balance * 2)
        self.balance -= balance
        other.add_balance(-balance)
        self.balance -= balance
        is_equal = other.balance_equal_to(balance)
        return is_equal

    def complex_if(self, balance: int, b_ins: ExperimentalB):
        self.balance = balance

        if self.balance > 10:
            b_ins.add_balance(balance)
            self.balance = 0
        elif b_ins.balance_equal_to(5):
            self.balance = 1
        else:
            self.balance = 2

        return self.balance

    def more_complex_if(self, balance: int, b_ins: ExperimentalB) -> int:
        self.balance = balance
        if balance >= 0:
            self.balance = balance
            if b_ins.balance_equal_to(balance * 2):
                self.balance = 1
            else:
                return -1

        return self.balance

    def test_no_return(self, balance: int, b_ins: ExperimentalB):
        if balance >= self.balance:
            self.balance = 0
            b_ins.add_balance(balance)
        else:
            self.balance = 1

    def work_with_list(self, x: int, others: List[ExperimentalB]):
        other_one: ExperimentalB = others[0]
        other_one.add_balance(10)

        if x > 0:
            others[-1].add_balance(10)
        else:
            other_one.add_balance(-10)

    def for_loops(self, x: int, others: List[ExperimentalB]):
        for y in others:
            y.add_balance(5)

            if x > 0:
                z = x
            else:
                z = -1

        return z

    def state_requests(self, items: List[ExperimentalB]):
        total: int = 0
        first_item: ExperimentalB = items[0]
        print(f"Total is now {total}.")
        total += first_item.balance  # Total = 0
        first_item.set_balance(10)
        total += first_item.balance  # total = 10
        first_item.set_balance(0)
        for x in items:
            total += x.balance  # total = 10
            x.set_balance(5)
            total += x.balance  # total = 10 + 5 + 5 = 20

        print(f"Total is now {total}.")
        total += first_item.balance  # total = 25
        if total > 0:
            first_item.set_balance(1)

        print(f"Total is now {total}.")

        total += first_item.balance  # total = 26
        return total

    def __key__(self):
        return self.name


@stateflow.stateflow
class OtherNestClass:
    def __init__(self, x: int):
        self.id = str(uuid.uuid4())
        self.x = x

        print(f"Im {type(self)} with id {self.id}")

    def is_really_true(self):
        return True

    def is_true(self, other: "OtherNestClass"):
        is_really_true: bool = other.is_really_true()
        return is_really_true

    def nest_calll(self, other: "OtherNestClass") -> bool:
        z = 0
        is_true = other.is_true(other)
        return is_true

    def __key__(self):
        return self.id


@stateflow.stateflow
class NestClass:
    def __init__(self, x: int):
        self.id = str(uuid.uuid4())
        self.x = x

        print(f"Im {type(self)} with id {self.id}")

    def nest_call(self, other: OtherNestClass):
        y = other.x
        z = 3

        if other.nest_calll(other):
            p = 3

        other.nest_calll(other)

        return y, z, p

    def __key__(self):
        return self.id
