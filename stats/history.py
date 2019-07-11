from datetime import timedelta

from .formula import get_linear_value



class HistoryValueError(Exception):
    pass


class History:
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

    DAYS_LIMIT = 31

    ONE_DAY = timedelta(days=1)

    def __init__(self, section, field_names):
        self.section = section
        self.field_names = field_names

        self.prev_values = {}
        self.prev_fetched_at = None
        self.prev_historydate = None
        self.save_prev_values()

    def save_prev_values(self):
        try:
            self.prev_fetched_at = self.section.fetched_at
        except AttributeError:
            return

        self.prev_historydate = self.section.historydate

        for field_name in self.field_names:
            if not hasattr(self.section, field_name):
                continue
            value = getattr(self.section, field_name)
            if value:
                self.prev_values[field_name] = value

    def update(self):
        if self.section is None:
            return

        if self.section.fetched_at is None:
            return

        if self.prev_fetched_at is None:
            return

        if self.prev_fetched_at > self.section.fetched_at:
            raise HistoryValueError("Can't update a history with older values.")

        if self.prev_fetched_at.date() == self.section.fetched_at.date():
            return

        self.section.historydate = self.prev_day_last_second(self.section.fetched_at)

        for field_name in self.prev_values.keys():
            self._update_field_history(field_name)


    def _update_field_history(self, field_name):
        prev_value = self.prev_values[field_name]
        value = getattr(self.section, field_name)
        value_type = type(value)

        history_field_name = f"{field_name}_history"
        values_history = getattr(self.section, history_field_name)

        new_values_history = []
        date = self.section.historydate
        while date >= self.prev_fetched_at:
            val = get_linear_value(date.timestamp(),
                                   self.prev_fetched_at.timestamp(), prev_value,
                                   self.section.fetched_at.timestamp(), value)
            val = value_type(val)
            new_values_history.append(val)
            date -= self.ONE_DAY

        values_history = new_values_history + list(values_history or [])
        values_history = values_history[:self.DAYS_LIMIT]

        setattr(self.section, history_field_name,  values_history)

    def prev_day_last_second(self, datetime):
        value = datetime.replace(hour=23, minute=59, second=59)
        value -= self.ONE_DAY
        return value
