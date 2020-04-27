from datetime import datetime

import pytz


class DateTimeService:
    def now(self, timezone=None, tz_str=None):
        timezone = timezone or pytz.timezone(tz_str or "UTC")
        return datetime.now(tz=timezone)

    def localize(self, value, timezone=None, tz_str=None):
        timezone = timezone or pytz.timezone(tz_str or "UTC")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone)
        return value

    def datetime(self, **kwargs):
        if "tzinfo" in kwargs:
            value = datetime(**kwargs)
        else:
            tz_str = kwargs.get("tz_str", "UTC")
            timezone = pytz.timezone(tz_str)
            value = datetime(**kwargs, tzinfo=timezone)
        return value

    def fromisoformat(self, date_string):
        return datetime.fromisoformat(date_string)


datetime_service = DateTimeService()
