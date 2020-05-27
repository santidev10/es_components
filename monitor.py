from collections import defaultdict

from es_components.connections import connections
from es_components.constants import TimestampFields
from es_components.constants import Sections
from es_components.query_builder import QueryBuilder


class BaseWarning:
    def __init__(self, *params):
        self.params = params


class Emergency:
    class NoneRecordsUpdated(BaseWarning):
        name = "NoneRecordsUpdated"

        def __init__(self, sections, control_percentage, ignore_deleted=False):
            super(Emergency.NoneRecordsUpdated, self).__init__(sections, control_percentage, ignore_deleted)
            self.sections = sections
            self.control_percentage = control_percentage
            self.ignore_deleted = ignore_deleted

        @property
        def message(self):
            sections = ", ".join(self.sections)
            return f"Less than {self.control_percentage}% of {sections} data has been updated during the last day"

class Warnings:
    class MainSectionNotFilled(BaseWarning):
        name = "MainSectionNotFilled"
        message = "The total count is bigger than the count of records with the filled `main` section"


    class NoNewSections(BaseWarning):
        name = "NoNewSections"

        @property
        def message(self):
            sections = ",".join(self.params)
            return f"No new {sections} sections in the last 3 days"


    class FewRecordsUpdated(BaseWarning):
        name = "FewRecordsUpdated"

        def __init__(self, section, control_percentage, ignore_deleted=False):
            super(Warnings.FewRecordsUpdated, self).__init__(section, control_percentage, ignore_deleted)
            self.section = section
            self.control_percentage = control_percentage
            self.ignore_deleted = ignore_deleted

        @property
        def message(self):
            return f"Less than {self.control_percentage}% of {self.section} data has been updated during the last day"


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

    # pylint: disable=unused-argument
    def get_warnings(self, *args, **kwargs):
        return []

    def get_alerts(self, *args, **kwargs):
        return []
    # pylint: enable=unused-argument


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
    WARNINGS_FEW_UPDATES_CHECK_DAYS = 1
    EMERGENCY_NONE_RECORDS_UPDATED_CHECK_DAYS = 1
    SKIPPED_SECTION_CHECK_DAYS = 7
    UPDATER_TASK_EXPIRE_DAYS = 2


    def __init__(self, *args, **kwargs):
        super(MonitoringPerformance, self).__init__(*args, **kwargs)
        self._warnings_check_func = {
            Warnings.MainSectionNotFilled.name: self.__check_main_section_not_filled,
            Warnings.FewRecordsUpdated.name: self.__check_few_records_updated,
            Warnings.NoNewSections.name: self.__check_no_new_section
        }
        self._warnings_prepare_message = {
            Warnings.MainSectionNotFilled.name: self.__prepare_messages,
            Warnings.FewRecordsUpdated.name: self.__prepare_messages,
            Warnings.NoNewSections.name: self.__prepare_messages_no_new_sections
        }

        self._emergency_check_func = {
            Emergency.NoneRecordsUpdated.name: self.__check_none_records_updated
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

    def get_section_info(self, section, ignore_deleted=False):
        # pylint: disable=no-member
        filled_query = QueryBuilder().build().must().exists().field(section).get()
        missed_query = QueryBuilder().build().must_not().exists().field(section).get()

        if ignore_deleted is True:
            ignore_deleted_query = QueryBuilder().build().must_not().exists().field(Sections.DELETED).get()
            filled_query = filled_query & ignore_deleted_query
            missed_query = missed_query & ignore_deleted_query

        filled = self.__get_count(query=filled_query)
        missed = self.__get_count(query=missed_query)

        updated_by_days = {
            key: self.__get_count(query=query)
            for key, query in self.__timestamp_query_generator(section, timestamp_field=TimestampFields.UPDATED_AT)
        }

        created_by_days = {
            key: self.__get_count(query=query)
            for key, query in self.__timestamp_query_generator(section, timestamp_field=TimestampFields.CREATED_AT)
        }
        # pylint: enable=no-member

        return dict(
            filled=filled,
            missed=missed,
            updated_by_days=updated_by_days,
            created_by_days=created_by_days
        )

    def __get_skipped(self, skipped_sections):
        queries = None

        for section in skipped_sections:
            __queries = \
                QueryBuilder().build().must_not().range().field(f"{section}.{TimestampFields.UPDATED_AT}")\
                    .gt(f"now-{86400 * self.SKIPPED_SECTION_CHECK_DAYS}s/s").get() & \
                QueryBuilder().build().must().range().field(f"{section}_schedule.{TimestampFields.UPDATED_AT}")\
                    .gt(f"now-{86400 * self.SKIPPED_SECTION_CHECK_DAYS}s/s").get() & \
                QueryBuilder().build().must().range().field(f"{section}_schedule.{TimestampFields.CREATED_AT}")\
                    .lt(f"now-{86400}s/s").get()

            queries = queries | __queries if queries is not None else __queries

        return self.__get_count(query=queries) if queries else None

    # pylint: disable=no-member
    def __get_deleted(self):
        return self.__get_count(query=QueryBuilder().build().must().exists().field(Sections.DELETED).get())
    # pylint: enable=no-member

    # pylint: disable=arguments-differ
    def get_info(self, sections, skipped_sections, *args):
        info_by_sections = {}
        for section in sections:
            info_by_sections.update({
                section: self.get_section_info(section, *args)
            })
        general = {
            "deleted": self.__get_deleted(),
            "skipped": self.__get_skipped(skipped_sections)
        }
        return dict(info_by_sections=info_by_sections, general=general)
    # pylint: enable=arguments-differ

    # pylint: disable=arguments-differ
    def get_warnings(self, warnings, *args):
        warning_messages = []
        available_warning = defaultdict(list)
        # pylint: disable=redefined-argument-from-local
        for warning in warnings:
            check_func = self._warnings_check_func.get(warning.name)

            if check_func and check_func(*warning.params):
                available_warning[warning.name].append(warning)

        for name, warnings in available_warning.items():
            prepare_messages = self._warnings_prepare_message.get(name)
            warning_messages += prepare_messages(warnings)
        # pylint: enable=redefined-argument-from-local
        return warning_messages
    # pylint: enable=arguments-differ

    # pylint: disable=unused-argument
    def get_alerts(self, emergencies, *args):
        alert_messages = []
        for emergency in emergencies:
            check_func = self._emergency_check_func.get(emergency.name)

            if check_func and check_func(*emergency.params):
                alert_messages.append(emergency.message)


        return alert_messages
    # pylint: enable=unused-argument

    # pylint: disable=unused-argument
    def __check_main_section_not_filled(self, *args):
        # pylint: disable=no-member
        count = self.__get_count(query=QueryBuilder().build().must().exists().field("main").get())
        # pylint: enable=no-member
        total_count = self.__get_count()
        return count < total_count
    # pylint: enable=unused-argument

    def __check_no_new_section(self, section):
        count = self.__get_count(
            query=QueryBuilder().build().must().range()\
                .field(f"{section}.{TimestampFields.CREATED_AT}")\
                .gt(f"now-{86400 * self.WARNINGS_CHECK_DAYS}s/s")\
                .get()
        )
        return count == 0

    def __check_few_records_updated(self, section, control_percentage=0, ignore_deleted=False, check_days=None):
        # pylint: disable=no-member
        check_days = check_days or self.WARNINGS_FEW_UPDATES_CHECK_DAYS

        count = self.__get_count(query=QueryBuilder().build().must().range() \
                                 .field(f"{section}.{TimestampFields.UPDATED_AT}") \
                                 .gt(f"now-{86400 * check_days}s/s") \
                                 .get())

        total_query = QueryBuilder().build().must().exists().field(section).get()

        if ignore_deleted is True:
            total_query = total_query & QueryBuilder().build().must_not().exists().field(Sections.DELETED).get()
        total = self.__get_count(query=total_query)
        # pylint: enable=no-member
        try:
            updated_percentage = count / total * 100
        except ZeroDivisionError:
            updated_percentage = 0

        return updated_percentage < control_percentage

    def __prepare_messages(self, warnings):
        messages = []
        for warning in warnings:
            messages.append(warning.message)
        return messages

    def __prepare_messages_no_new_sections(self, warnings):
        if warnings:
            sections = [warning.params[0] for warning in warnings]
            merge_warning = warnings[0].__class__(*sections)
            return [merge_warning.message]
        return []

    def __check_none_records_updated(self, sections, control_percentage, ignore_deleted=False):
        # pylint: disable=no-member
        checks = [
            self.__check_few_records_updated(
                section, control_percentage, ignore_deleted, self.EMERGENCY_NONE_RECORDS_UPDATED_CHECK_DAYS
            )
            for section in sections
        ]
        # pylint: enable=no-member
        return all(checks)


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

    # pylint: disable=arguments-differ
    def get_warnings(self, *args):
        results = []
        for monitor in self.__monitors:
            results += monitor.get_warnings(*args)
        return results
    # pylint: enable=arguments-differ

    # pylint: disable=arguments-differ
    def get_alerts(self, *args):
        results = []
        for monitor in self.__monitors:
            results += monitor.get_alerts(*args)
        return results
    # pylint: enable=arguments-differ

