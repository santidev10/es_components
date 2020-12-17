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


def get_ias_verified_exists_filter() -> Q:
    """
    IAS verified date should be no less than one week old in order to be considered to "exist"
    :return: Q
    """
    return QueryBuilder().build().must().range().field(f"{Sections.IAS_DATA}.ias_verified").gte("now-7d/d").get()
