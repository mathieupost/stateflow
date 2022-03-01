from typing import List
import stateflow


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

    def set_stock(self, amount: int):
        self.stock = amount

    def ping(self, user: "User") -> bool:
        return user.pong()

    def __key__(self) -> str:
        return self.item_name


@stateflow.stateflow
class User:
    def __init__(self, username: str):
        self.username: str = username
        self.balance: int = 1
        self.items: List[Item] = []

    def update_balance(self, x: int):
        self.balance += x

    def get_balance(self) -> int:
        return self.balance

    def buy_item(self, amount: int, item: Item, user: "User") -> bool:
        print("buy_item")
        # user and item should be consistent versions
        total_price = amount * item.price
        print(f"total_price = {total_price}")

        if self.balance < total_price:
            print("insufficient balance")
            return False

        # Decrease the stock.
        decrease_stock = item.update_stock(-amount)
        print(f"decrease_stock = {decrease_stock}")

        # TODO Calling back to same class doesn't work.
        # user: "User" = self
        item.ping(user)

        if not decrease_stock:
            return False  # For some reason, stock couldn't be decreased.

        self.balance -= total_price
        print(f"balance = {self.balance}")
        return True

    def simple_for_loop(self, users: List["User"]) -> int:
        i = 0
        for user in users:
            if i > 0:
                user.update_balance(9)
            else:
                user.update_balance(4)
            i += 1

        return i

    def state_requests(self, items: List[Item]) -> int:
        print(f"state_requests")
        total: int = 0
        print(f"total = {total}")
        first_item: Item = items[0]
        print(f"got first_item")
        total += first_item.stock  # Total = 0
        print(f"total = {total}")
        first_item.set_stock(10)
        print(f"set first_item stock to 10")
        total += first_item.stock  # total = 10
        print(f"total = {total}")
        first_item.set_stock(0)
        print(f"set first_item stock to 0")
        for x in items:
            total += x.stock  # total = 10
            print(f"total = {total}")
            x.set_stock(5)
            print(f"set x stock to 5")
            total += x.stock  # total = 10 + 5 + 5 = 20
            print(f"total = {total}")

        total += first_item.stock  # total = 25
        print(f"total = {total}")
        if total > 0:
            first_item.set_stock(1)
            print(f"set first_item stock to 1")

        first_item: Item = first_item
        print(f"set first_item to first_item")
        total += first_item.stock  # total = 26
        print(f"end total = {total}")
        return total

    def pong(self) -> bool:
        return True

    def __key__(self) -> str:
        return self.username
