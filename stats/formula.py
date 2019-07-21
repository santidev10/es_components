from statistics import mean

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
        dataframe.is_normal &= (dataframe.diffs.fillna(0) >= 0)
        # pylint: enable=no-member

    if max_sigmas:
        std_deviation = history.rolling(std_period or 14, min_periods=2).std()
        sigmas = abs(dataframe.diffs / std_deviation).fillna(0)
        # pylint: disable=no-member
        dataframe.is_normal &= (sigmas <= max_sigmas)
        # pylint: enable=no-member

    # pylint: disable=no-member
    dataframe.diffs = dataframe.diffs.mask(~dataframe.is_normal)
    # pylint: enable=no-member

    return dataframe


def get_counter_dataframe_tailing_sum(dataframe, count, max_errors=None, cast_type=None):
    if max_errors is not None:
        total_count = dataframe.is_normal[-count:].count()
        normal_count = dataframe[-count:].is_normal[dataframe.is_normal].count()
        errors_count = total_count - normal_count
        if errors_count > max_errors:
            return None

    value = dataframe.diffs[-count:].sum()

    if cast_type and cast_type is not None.__class__:
        value = cast_type(value)

    return value


def get_is_strange_views(views_history):
    deltas = [views_history[i - 1] - views_history[i] for i in range(1, len(views_history))]
    return mean(deltas[:2]) > mean(deltas[2:]) * 1.5
