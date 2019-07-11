from datetime import datetime
from unittest import TestCase

from es_components.stats.formula import get_linear_value
from es_components.stats.formula import get_engage_rate
from es_components.stats.formula import get_sentiment


class TestFormulaLinearValue(TestCase):
    def test_success(self):
        timestamp_1 = datetime(year=2020, month=1, day=1, hour=12).timestamp()
        value_1 = 15000

        timestamp_2 = datetime(year=2020, month=1, day=5, hour=12).timestamp()
        value_2 = 55000

        desired_timestamp = datetime(year=2020, month=1, day=2).timestamp()
        expected_value = 20000

        actual_value = get_linear_value(
            x=desired_timestamp,
            x1=timestamp_1,
            y1=value_1,
            x2=timestamp_2,
            y2=value_2
        )

        self.assertEqual(expected_value, actual_value)


class TestFormulaSentiment(TestCase):
    def test_all_defined(self):
        likes = 200
        dislikes = 50
        sentiment = get_sentiment(likes, dislikes)
        expected_sentiment = 80

        self.assertEqual(expected_sentiment, sentiment)
        self.assertTrue(type(sentiment) is float)

    def test_likes_undefined(self):
        likes = None
        dislikes = 50
        sentiment = get_sentiment(likes, dislikes)
        expected_sentiment = 0

        self.assertEqual(expected_sentiment, sentiment)
        self.assertTrue(type(sentiment) is float)

    def test_dislikes_undefined(self):
        likes = 200
        dislikes = None
        sentiment = get_sentiment(likes, dislikes)
        expected_sentiment = 100

        self.assertEqual(expected_sentiment, sentiment)
        self.assertTrue(type(sentiment) is float)

    def test_all_undefined(self):
        likes = None
        dislikes = None
        sentiment = get_sentiment(likes, dislikes)
        expected_sentiment = 0

        self.assertEqual(expected_sentiment, sentiment)
        self.assertTrue(type(sentiment) is float)


class TestFormulaEngageRate(TestCase):
    def test_all_defineds(self):
        likes = 10
        dislikes = 20
        comments = 30
        views = 400

        engage_rate = get_engage_rate(likes, dislikes, comments, views)
        expected_engage_rate = 15

        self.assertEqual(expected_engage_rate, engage_rate)
        self.assertTrue(type(engage_rate) is float)

    def test_no_views(self):
        likes = None
        dislikes = None
        comments = None

        expected_engage_rate = 0

        engage_rate = get_engage_rate(likes, dislikes, comments, 0)
        self.assertEqual(expected_engage_rate, engage_rate)

        engage_rate = get_engage_rate(likes, dislikes, comments, None)
        self.assertEqual(expected_engage_rate, engage_rate)

    def test_between_100_and_1000(self):
        likes = 100
        dislikes = 200
        comments = 300
        views = 400

        engage_rate = get_engage_rate(likes, dislikes, comments, views)
        expected_engage_rate = 100

        self.assertEqual(expected_engage_rate, engage_rate)
        self.assertTrue(type(engage_rate) is float)

    def test_greater_then_1000(self):
        likes = 1000
        dislikes = 2000
        comments = 3000
        views = 400

        engage_rate = get_engage_rate(likes, dislikes, comments, views)
        expected_engage_rate = 0

        self.assertEqual(expected_engage_rate, engage_rate)
        self.assertTrue(type(engage_rate) is float)
