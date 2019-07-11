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
