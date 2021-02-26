from elasticsearch_dsl import Q

from es_components.query_builder import QueryBuilder
from es_components.constants import LAST_VETTED_AT_MIN_DATE
from es_components.constants import Sections


def get_last_vetted_at_exists_filter() -> Q:
    """
    when we check if something was vetted, the task_us_data.last_vetted_at field must exist AND be greater than the
    LAST_VETTED_AT_MIN_DATE
    :return: elasticsearch_dsl.Q instance
    """
    return QueryBuilder().build().must().range().field(f"{Sections.TASK_US_DATA}.last_vetted_at") \
        .gte(LAST_VETTED_AT_MIN_DATE).get()


def get_ias_verified_exists_filter(last_ingested_timestamp: str = None) -> Q:
    """
    IAS verified date should be passed in. it's typically the time when an IAS csv was last ingested.
    Default to no less than one week old if no date is passed
    :return: Q
    """
    return QueryBuilder().build().must().range().field(f"{Sections.IAS_DATA}.ias_verified") \
        .gte(last_ingested_timestamp or "now-7d/d").get()
