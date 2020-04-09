from es_components.stats.history import BaseHistory


class RawHistory(BaseHistory):

    def update(self):
        super(RawHistory, self).update()

        for field_name in self.field_names:
            self._update_field_history(field_name)

    def _update_field_history(self, field_name):
        prev_value = self.prev_values.get(field_name)
        value = getattr(self.section, field_name)
        date = self.section.historydate.date()

        history_field_name = f"{field_name}_raw_history"
        values_history = getattr(self.section, history_field_name).to_dict()

        if self.prev_historydate and prev_value is not None and \
                str(self.prev_historydate.date()) not in values_history.keys():
            values_history[str(self.prev_historydate.date())] = prev_value

        values_history[str(date)] = value

        if self.DAYS_LIMIT is not None:
            values_history = self._clear_history(values_history)

        setattr(self.section, history_field_name, values_history)


    def _clear_history(self, values_history):
        dates = sorted(values_history.keys(), reverse=True)
        dates = dates[:self.DAYS_LIMIT]

        return {date: value for date, value in values_history if date in dates}
