from itertools import count
from itertools import groupby
import time

from elasticsearch.exceptions import ConflictError


def chunks(iterable, size):
    chunk = count()
    for _, group in groupby(iterable, lambda _: next(chunk) // size):
        yield group


def safe_div(numerator, denominator):
    try:
        return numerator / denominator
    except ZeroDivisionError:
        return None


def add_brand_safety_labels(aggregations, use_admin_brand_safety_labels):
    if "brand_safety" in aggregations:
        if use_admin_brand_safety_labels:
            aggregations["brand_safety"]["buckets"][0]["key"] = "7 and below"
            aggregations["brand_safety"]["buckets"][1]["key"] = "Low Suitability"
            aggregations["brand_safety"]["buckets"][2]["key"] = "Medium Suitability"
            aggregations["brand_safety"]["buckets"][3]["key"] = "Suitable"
        else:
            aggregations["brand_safety"]["buckets"][0]["key"] = "Low Suitability"
            aggregations["brand_safety"]["buckets"][1]["key"] = "Medium Suitability"
            aggregations["brand_safety"]["buckets"][2]["key"] = "Suitable"

    return aggregations


def add_sentiment_labels(aggregations):
    if "stats.sentiment" in aggregations:
        aggregations["stats.sentiment"]["buckets"][0]["key"] = "All"
        aggregations["stats.sentiment"]["buckets"][1]["key"] = "At least 79% liked"
        aggregations["stats.sentiment"]["buckets"][2]["key"] = "At least 90% liked"
        bottom_bucket = aggregations["stats.sentiment"]["buckets"].pop(1)
        aggregations["stats.sentiment"]["buckets"].append(bottom_bucket)
    return aggregations


def retry_on_conflict(method, *args, retry_amount=5, sleep_coeff=2, **kwargs):
    """
    Retry on Document Conflicts
    """
    tries_count = 0
    while tries_count <= retry_amount:
        try:
            result = method(*args, **kwargs)
        except ConflictError:
            tries_count += 1
            if tries_count <= retry_amount:
                sleep_seconds_count = tries_count ** sleep_coeff
                time.sleep(sleep_seconds_count)
        else:
            return result
