from datetime import datetime

from elasticsearch_dsl import AttrDict
import pandas


def get_sentiment(likes, dislikes):
    likes = likes or 0
    dislikes = dislikes or 0
    value = (likes / max(sum((likes, dislikes)), 1)) * 100
    return value


def get_engage_rate(likes, dislikes, comments, views):
    likes = likes or 0
    dislikes = dislikes or 0
    comments = comments or 0
    views = views or 1

    value = 0.
    if likes + dislikes < views:
        plain = (sum((likes, dislikes, comments)) / views) * 100
        value = plain if plain <= 100 else 100. if plain <= 1000 else 0.

    return value


def get_linear_value(x, x1, y1, x2, y2):
    y = (x - x1) / (x2 - x1) * (y2 - y1) + y1
    return y


def get_counter_dataframe(history, max_sigmas=None, std_period=None, constantly_growing=None):
    history = pandas.Series(reversed(history))
    dataframe = pandas.DataFrame(dict(
        history=history,
        diffs=history - history.shift(1),
        is_normal=True,
    ))

    if constantly_growing:
        # pylint: disable=no-member
        # For all diffs <= 0, set is_normal = False
        dataframe.is_normal &= (dataframe.diffs.fillna(0) >= 0)
        # pylint: enable=no-member

    if max_sigmas:
        # if std deviation for history date is > max_sigmas, set is_normal=False
        std_deviation = history.rolling(std_period or 14, min_periods=2).std()
        sigmas = abs(dataframe.diffs / std_deviation).fillna(0)
        # pylint: disable=no-member
        dataframe.is_normal &= (sigmas <= max_sigmas)
        # pylint: enable=no-member

    # pylint: disable=no-member
    # set diffs as NaN if is normal is False
    dataframe.diffs = dataframe.diffs.mask(~dataframe.is_normal)
    # pylint: enable=no-member

    return dataframe


def get_counter_dataframe_tailing_sum(dataframe, count, offset=0, max_errors=None, cast_type=None):
    count = count or dataframe.diffs.count()
    start, end = -count - offset, -offset or None
    if max_errors is not None:
        total_count = dataframe.is_normal[start:end].count()
        normal_count = dataframe[start:end].is_normal[dataframe.is_normal].count()
        errors_count = total_count - normal_count
        if errors_count > max_errors:
            return None

    value = dataframe.diffs[start:end].sum()

    if cast_type and cast_type is not None.__class__:
        value = cast_type(value)

    return value


def get_counter_dataframe_tailing_diffs_mean(dataframe, count=None, offset=0, max_errors=None, cast_type=None):
    count = count or dataframe.diffs.count()
    start, end = -count - offset, -offset or None
    if max_errors is not None:
        total_count = dataframe.is_normal[start:end].count()
        normal_count = dataframe[start:end].is_normal[dataframe.is_normal].count()
        errors_count = total_count - normal_count
        if errors_count > max_errors:
            return None

    value = dataframe.diffs[start:end].mean()

    if cast_type and cast_type is not None.__class__:
        value = cast_type(value)

    return value


def get_counter_sum_days(raw_history: AttrDict, days: int):
    """
    Calculate sum values for days range by finding the simple average of the last two valid values and multiplying
        by days
    """
    value_for_days = None
    try:
        dates = list(sorted(raw_history.to_dict().keys(), key=lambda x: datetime.strptime(x, "%Y-%m-%d")))
        if len(dates) >= 2:
            start = dates[-2]
            end = dates[-1]
            days_between = (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")).days
            value_for_days = (raw_history[end] - raw_history[start]) / days_between * days
    except (ZeroDivisionError, AttributeError, KeyError):
        pass
    return value_for_days
