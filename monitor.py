from es_components.connections import connections
from es_components.constants import TimestampFields
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder
from es_components.utils import safe_div


PERCENTAGE_DECIMALS_ROUND = 1


class BaseMonitor:
    name = None

    def __init__(self, index_name):
        self.index_name = index_name
        self.connection = connections.get_connection()

    def get_info(self, *args, **kwargs):
        raise NotImplementedError



class MonitoringIndex(BaseMonitor):
    name = "index"
    INFO_TABLE_FIELDS = ("index", "health", "pri", "rep", "docs.count", "docs.deleted", "store.size", "pri.store.size")

    # pylint: disable=arguments-differ
    def get_info(self, *args):
        info = self.connection.cat.indices(format="json",
                                           index=self.index_name,
                                           h=",".join(self.INFO_TABLE_FIELDS))
        return info.pop() if info else {}
    # pylint: enable=arguments-differ


class MonitoringPerformance(BaseMonitor):
    name = "performance"
    DAYS_LIST = (1, 3, 7, 30, 365)

    class Result:

        def __init__(self, total, filled, missed, missed_by_days, updated_by_days, created_by_days):
            self.total = total
            self.filled = (filled, self.get_representing_in_percentage(total, filled))
            self.missed = (missed, self.get_representing_in_percentage(total, missed))
            self.missed_by_days = [
                (_missed, self.get_representing_in_percentage(total, _missed))
                for _missed in missed_by_days
            ]

            self.updated_by_days = [
                (_updated, self.get_representing_in_percentage(total, _updated))
                for _updated in updated_by_days
            ]
            self.created_by_days = [
                (_created, self.get_representing_in_percentage(total, _created))
                for _created in created_by_days
            ]

        def get_representing_in_percentage(self, total, part):
            return round(safe_div(part * 100, total), PERCENTAGE_DECIMALS_ROUND)

    def __get_count(self, query=None):
        body = {}
        if query:
            body.update(query=query)
        count = self.connection.count(index=self.index_name, body=body).get("count")
        return count

    def __timestamp_query_generator(self, section=Sections.MAIN, timestamp_field=TimestampFields.CREATED_AT):
        for days in self.DAYS_LIST:
            yield QueryBuilder().build().must().range()\
                .field(f"{section}.{timestamp_field}")\
                .gt(f"now-{86400 * days}s/s")\
                .get()

    def get_section_info(self, section, total):
        filled = self.__get_count(query=QueryBuilder().build().must().exists().field(section).get())
        missed = self.__get_count(query=QueryBuilder().build().must_not().exists().field(section).get())

        missed_by_days = [
            self.__get_count(query=query & QueryBuilder().build().must_not().exists().field(section).get())
            for query in self.__timestamp_query_generator()
        ]

        updated_by_days = [
            self.__get_count(query=query)
            for query in self.__timestamp_query_generator(section, timestamp_field=TimestampFields.UPDATED_AT)
        ]

        created_by_days = [
            self.__get_count(query=query)
            for query in self.__timestamp_query_generator(section, timestamp_field=TimestampFields.CREATED_AT)
        ]

        return self.Result(total=total,
                           filled=filled,
                           missed=missed,
                           missed_by_days=missed_by_days,
                           updated_by_days=updated_by_days,
                           created_by_days=created_by_days)

    # pylint: disable=arguments-differ
    def get_info(self, sections, *args):
        results = {}
        total_count = self.__get_count()
        for section in sections:
            results.update({
                section: self.get_section_info(section, total_count)
            })
        return results
    # pylint: enable=arguments-differ


class Monitor(BaseMonitor):
    monitors = [MonitoringIndex, MonitoringPerformance]

    def get_cluster_name(self):
        name = self.connection.cluster.stats(format="json").get("cluster_name")
        name = name.split(":")[-1]
        return name

    # pylint: disable=arguments-differ
    def get_info(self, *args):
        results = {}
        for monitor in self.monitors:
            results[monitor.name] = monitor(self.index_name).get_info(*args)
        return results
    # pylint: enable=arguments-differ

