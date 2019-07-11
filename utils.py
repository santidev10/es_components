from itertools import count
from itertools import groupby


def chunks(iterable, size):
    c = count()
    for _, g in groupby(iterable, lambda _: next(c) // size):
        yield g