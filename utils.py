from itertools import count
from itertools import groupby


def chunks(iterable, size):
    chunk = count()
    for _, group in groupby(iterable, lambda _: next(chunk) // size):
        yield group


def safe_div(numerator, denominator):
    try:
        return numerator / denominator
    except ZeroDivisionError:
        return None
