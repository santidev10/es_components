from elasticsearch_dsl import Q

from es_components.connections import connections
from es_components.constants import TimestampFields
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder


class BaseWarning:
    def __init__(self, *params):
        self.params = params

class Warnings:
    class MainSectionNotFilled(BaseWarning):
        name = "MainSectionNotFilled"
        message = "The total count is bigger than the count of records with the filled `main` section"


    class NoNewSections(BaseWarning):
        name = "NoNewSections"

        @property
        def message(self):
            sections = ','.join(self.params)
            return f"No new {sections} sections in the last 3 days"


    class FewRecordsUpdated(BaseWarning):
        name = "FewRecordsUpdated"
        message = "Less than 50% of records has been updated in the last 3 days"


class BaseMonitor:
    name = None

    def __init__(self, index_name):
        self.index_name = index_name
        self.connection = connections.get_connection()

    def get_info(self, *args, **kwargs):
        raise NotImplementedError

    def get_warnings(self, *args, **kwargs):
        raise NotImplementedError


class MonitoringIndex(BaseMonitor):
    name = "index"
    INFO_TABLE_FIELDS = ("index", "health", "pri", "rep", "docs.count", "docs.deleted", "store.size", "pri.store.size")

    # pylint: disable=arguments-differ
    def get_info(self, *args):
        info = self.connection.cat.indices(format="json",
                                           index=self.index_name,
                                           h=",".join(self.INFO_TABLE_FIELDS))
        info_data = info.pop() if info else {}

        return dict(
            index=info_data.get("index"),
            docs_count=info_data.get("docs.count"),
            docs_deleted=info_data.get("docs.deleted"),
            store_size=info_data.get("store.size"),
            pri_store_size=info_data.get("pri.store.size"),
            health=info_data.get("health"),
            pri=info_data.get("pri"),
            rep=info_data.get("rep"),

        )
    # pylint: enable=arguments-differ

    def get_warnings(self, *args, **kwargs):
        return []


class MonitoringPerformance(BaseMonitor):
    name = "performance"
    DAYS_LIST = (
        ("last_day", 1),
        ("last_3_days", 3),
        ("last_7_days", 7),
        ("last_30_days", 30),
        ("last_365_days", 365),
    )
    WARNINGS_CHECK_DAYS = 3

    def __init__(self, *args, **kwargs):
        super(MonitoringPerformance, self).__init__(*args, **kwargs)
        self._warnings_check_func = {
            Warnings.MainSectionNotFilled.name: self.__check_main_section_not_filled,
            Warnings.FewRecordsUpdated.name: self.__check_few_records_updated,
            Warnings.NoNewSections.name: self.__check_no_new_section
        }


    def __get_count(self, query=None):
        body = {}
        if query:
            body.update(query=query)
        count = self.connection.count(index=self.index_name, body=body).get("count")
        return count

    def __timestamp_query_generator(self, section=Sections.MAIN, timestamp_field=TimestampFields.CREATED_AT):
        for key, days in self.DAYS_LIST:
            yield key, QueryBuilder().build().must().range()\
                .field(f"{section}.{timestamp_field}")\
                .gt(f"now-{86400 * days}s/s")\
                .get()

    def get_section_info(self, section):
        filled = self.__get_count(query=QueryBuilder().build().must().exists().field(section).get())
        missed = self.__get_count(query=QueryBuilder().build().must_not().exists().field(section).get())

        missed_by_days = {
            key: self.__get_count(query=query & QueryBuilder().build().must_not().exists().field(section).get())
            for key, query in self.__timestamp_query_generator()
        }

        updated_by_days = {
            key: self.__get_count(query=query)
            for key, query in self.__timestamp_query_generator(section, timestamp_field=TimestampFields.UPDATED_AT)
        }

        created_by_days = {
            key: self.__get_count(query=query)
            for key, query in self.__timestamp_query_generator(section, timestamp_field=TimestampFields.CREATED_AT)
        }

        return dict(
            filled=filled,
            missed=missed,
            missed_by_days=missed_by_days,
            updated_by_days=updated_by_days,
            created_by_days=created_by_days
        )

    # pylint: disable=arguments-differ
    def get_info(self, sections, *args):
        results = {}
        for section in sections:
            results.update({
                section: self.get_section_info(section)
            })
        return results
    # pylint: enable=arguments-differ

    def get_warnings(self, warnings, *args):
        warning_messages = []
        for warning in warnings:
            check_func = self._warnings_check_func.get(warning.name)

            if check_func and check_func(*warning.params):
                warning_messages.append(warning.message)

        return warning_messages


    def __check_main_section_not_filled(self, *args):
        count = self.__get_count(query=QueryBuilder().build().must().exists().field("main").get())
        total_count = self.__get_count()
        return count < total_count


    def __check_no_new_section(self, sections):
        queries = []
        for section in sections:
            queries.append(
                QueryBuilder().build().must().range()\
                .field(f"{section}.{TimestampFields.CREATED_AT}")\
                .gt(f"now-{86400 * self.WARNINGS_CHECK_DAYS}s/s")\
                .get()
            )

        count = self.__get_count(query=Q("bool", filter=queries))
        return count == 0

    def __check_few_records_updated(self, sections):
        queries = []
        for section in sections:
            queries.append(
                QueryBuilder().build().must().range() \
                    .field(f"{section}.{TimestampFields.UPDATED_AT}") \
                    .gt(f"now-{86400 * self.WARNINGS_CHECK_DAYS}s/s") \
                    .get()
            )

        count = self.__get_count(query=Q("bool", filter=queries))
        control_count = self.__get_count() / 2
        return count > control_count




class Monitor(BaseMonitor):
    monitors = [MonitoringIndex, MonitoringPerformance]

    def __init__(self, *args, **kwargs):
        super(Monitor, self).__init__(*args, **kwargs)
        self.__monitors = [monitor(self.index_name) for monitor in self.monitors]

    def get_cluster_name(self):
        name = self.connection.cluster.stats(format="json").get("cluster_name")
        name = name.split(":")[-1]
        return name

    # pylint: disable=arguments-differ
    def get_info(self, *args):
        results = {}
        for monitor in self.__monitors:
            results[monitor.name] = monitor.get_info(*args)
        return results
    # pylint: enable=arguments-differ


    def get_warnings(self, *args):
        results = []
        for monitor in self.__monitors:
            results += monitor.get_warnings(*args)
        return results


