from datetime import timedelta

from es_components.datetime_service import datetime_service

from .formula import get_linear_value


class HistoryValueError(Exception):
    pass


class BaseHistory:
    DAYS_LIMIT = None

    ONE_DAY = timedelta(days=1)

    def __init__(self, section, field_names):
        self.section = section
        self.field_names = field_names

        self.prev_fetched_at = None
        self.prev_historydate = None
        self.prev_values = {}
        self.save_prev_values()

    def save_prev_values(self):
        try:
            self.prev_fetched_at = self.section.fetched_at
        except AttributeError:
            return

        if self.prev_fetched_at:
            self.prev_fetched_at = datetime_service.localize(self.prev_fetched_at)

        if self.section.historydate:
            self.prev_historydate = datetime_service.localize(self.section.historydate)

        for field_name in self.field_names:
            if not hasattr(self.section, field_name):
                continue
            value = getattr(self.section, field_name)
            self.prev_values[field_name] = value

    def update(self):
        if self.section is None:
            return

        if self.section.fetched_at is None:
            return
        self.section.fetched_at = datetime_service.localize(self.section.fetched_at)

        if self.prev_fetched_at is not None and self.prev_fetched_at > self.section.fetched_at:
            raise HistoryValueError("Can't update a history with older values.")

        self.section.historydate = self.prev_day_last_second(self.section.fetched_at)

    def prev_day_last_second(self, datetime):
        value = datetime.replace(hour=23, minute=59, second=59)
        value -= self.ONE_DAY
        return value



class History(BaseHistory):
    """
        The History context manager is needed to build *_history fields
        from field with some stats metric.


        Usage scenatio:
            history = History(stats_section, ['subscribers', 'views'])

            stats_section.videos = 12
            stats_section.subscribers = 123
            stats_section.views = 1234

            history.update()

        It will update appropriate history fields:
            stats_section.subscribers_history = [...some previous values...]
            stats_section.views_history = [...some previous values...]
    """


    def update(self):
        super(History, self).update()

        if self.prev_fetched_at is None or self.prev_fetched_at.date() == self.section.fetched_at.date():
            return

        for field_name in self.prev_values.keys():
            self._update_field_history(field_name)

    def _update_field_history(self, field_name):
        prev_value = self.prev_values[field_name]
        value = getattr(self.section, field_name)
        value_type = type(value)

        history_field_name = f"{field_name}_history"
        values_history = getattr(self.section, history_field_name)

        if prev_value is None and value is not None and self.prev_historydate is not None:
            valuable_history = ((index, value) for index, value in enumerate(values_history) if value is not None)
            last_index, prev_value = next(valuable_history, (0, None))
            values_history = values_history[last_index+1:]
            prev_fetched_at = self.prev_historydate - last_index * self.ONE_DAY
        else:
            prev_fetched_at = self.prev_fetched_at

        new_values_history = []
        date = self.section.historydate
        while date >= prev_fetched_at:
            try:
                val = get_linear_value(date.timestamp(),
                                       prev_fetched_at.timestamp(), prev_value,
                                       self.section.fetched_at.timestamp(), value)
                val = value_type(val)
            except TypeError:
                val = None

            new_values_history.append(val)
            date -= self.ONE_DAY

        values_history = new_values_history + list(values_history or [])
        if self.DAYS_LIMIT is not None:
            values_history = values_history[:self.DAYS_LIMIT]

        if all([value is None for value in values_history]):
            values_history = []

        setattr(self.section, history_field_name, values_history)
