from functools import wraps
from typing import Generator, TypeVar


YT = TypeVar("YT") # YieldType
ST = TypeVar("ST") # SendType
RT = TypeVar("RT") # ReturnType

class WrappedGenerator(Generator[YT, ST, RT]):
    """Wraps a generator function to retain its return value.

    This makes it possible to yield values in a generator function, while also
    storing the final return value for later use."""
    def __init__(self, gen: Generator[YT, ST, RT]):
        self.gen = gen

    def __iter__(self) -> Generator[YT, ST, RT]:
        self.return_value = yield from self.gen
        return self.return_value

    def send(self, value: ST) -> YT:
        return self.gen.send(value)

    def throw(self, typ, val=None, tb=None) -> YT:
        return self.gen.throw(typ, val, tb)

def keep_return_value(f):
    @wraps(f)
    def g(*args, **kwargs):
        return WrappedGenerator(f(*args, **kwargs))
    return g
