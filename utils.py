from itertools import count
from itertools import groupby

import brand_safety.constants as constants

def chunks(iterable, size):
    chunk = count()
    for _, group in groupby(iterable, lambda _: next(chunk) // size):
        yield group


def safe_div(numerator, denominator):
    try:
        return numerator / denominator
    except ZeroDivisionError:
        return None


def add_brand_safety_labels(aggregations):
    if "brand_safety" in aggregations:
        aggregations["brand_safety"]["buckets"][0]["key"] = constants.HIGH_RISK
        aggregations["brand_safety"]["buckets"][1]["key"] = constants.RISKY
        aggregations["brand_safety"]["buckets"][2]["key"] = constants.LOW_RISK
        aggregations["brand_safety"]["buckets"][3]["key"] = constants.SAFE
    return aggregations
