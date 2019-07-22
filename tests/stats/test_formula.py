import math
from datetime import datetime
from unittest import TestCase

from es_components.stats.formula import get_counter_dataframe
from es_components.stats.formula import get_counter_dataframe_tailing_diffs_mean
from es_components.stats.formula import get_counter_dataframe_tailing_sum
from es_components.stats.formula import get_engage_rate
from es_components.stats.formula import get_linear_value
from es_components.stats.formula import get_sentiment
from .base import MathTestCase


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
        self.assertTrue(isinstance(sentiment, float))

    def test_likes_undefined(self):
        likes = None
        dislikes = 50
        sentiment = get_sentiment(likes, dislikes)
        expected_sentiment = 0

        self.assertEqual(expected_sentiment, sentiment)
        self.assertTrue(isinstance(sentiment, float))

    def test_dislikes_undefined(self):
        likes = 200
        dislikes = None
        sentiment = get_sentiment(likes, dislikes)
        expected_sentiment = 100

        self.assertEqual(expected_sentiment, sentiment)
        self.assertTrue(isinstance(sentiment, float))

    def test_all_undefined(self):
        likes = None
        dislikes = None
        sentiment = get_sentiment(likes, dislikes)
        expected_sentiment = 0

        self.assertEqual(expected_sentiment, sentiment)
        self.assertTrue(isinstance(sentiment, float))


class TestFormulaEngageRate(TestCase):
    def test_all_defineds(self):
        likes = 10
        dislikes = 20
        comments = 30
        views = 400

        engage_rate = get_engage_rate(likes, dislikes, comments, views)
        expected_engage_rate = 15

        self.assertEqual(expected_engage_rate, engage_rate)
        self.assertTrue(isinstance(engage_rate, float))

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
        self.assertTrue(isinstance(engage_rate, float))

    def test_greater_then_1000(self):
        likes = 1000
        dislikes = 2000
        comments = 3000
        views = 400

        engage_rate = get_engage_rate(likes, dislikes, comments, views)
        expected_engage_rate = 0

        self.assertEqual(expected_engage_rate, engage_rate)
        self.assertTrue(isinstance(engage_rate, float))


class TestCumulativeCounter(MathTestCase):
    def get_diffs(self, history):
        diffs = [math.nan] + [
            history[-i - 2] - history[-i - 1]
            for i in range(len(history) - 1)
        ]

        return diffs

    def test_dataframe_without_normalization(self):
        history = [750, 650, 550, 100000, 90000, 81000, 0, 0, 66000, 55000,
                   51000, 48000, 46000, 45000, 44500, 44250, 44125]
        expected_diffs = self.get_diffs(history)

        dataframe = get_counter_dataframe(history, constantly_growing=None, max_sigmas=None)

        # has expected fields and length
        self.assertListEqual(list(dataframe), ["history", "diffs", "is_normal"])
        self.assertEqual(dataframe.history.count(), len(history))

        # reversed values order
        self.assertEqual(dataframe.history[0], history[-1])
        self.assertEqual(dataframe.history[len(history) - 1], history[0])

        # all diffs are normal
        expected_is_normal = [True for _ in expected_diffs]

        # pylint: disable=no-member
        self.assertMathListEqual(expected_is_normal, list(dataframe.is_normal))
        # pylint: enable=no-member

        # all diffs are expected
        self.assertMathListEqual(expected_diffs, list(dataframe.diffs))

    def test_dataframe_normalization_by_constantly_growing(self):
        history = [750, 650, 550, 100000, 90000, 81000, 0, 0, 66000, 55000,
                   51000, 48000, 46000, 45000, 44500, 44250, 44125]
        expected_diffs = self.get_diffs(history)

        dataframe = get_counter_dataframe(history, constantly_growing=True, max_sigmas=None)

        # every diff is normal except negatives
        expected_is_normal = [math.isnan(diff) or diff >= 0 for diff in expected_diffs]
        # pylint: disable=no-member
        self.assertListEqual(expected_is_normal, list(dataframe.is_normal))
        # pylint: enable=no-member

        # all diffs are expected
        expected_diffs = [math.nan if diff < 0 else diff for diff in expected_diffs]
        self.assertMathListEqual(expected_diffs, list(dataframe.diffs))

    def test_dataframe_normalization_by_sigmas(self):
        history = [750, 650, 550, 100000, 90000, 81000, 0, 0, 66000, 55000,
                   51000, 48000, 46000, 45000, 44500, 44250, 44125]
        expected_diffs = self.get_diffs(history)

        dataframe = get_counter_dataframe(history, constantly_growing=False, max_sigmas=2)

        # every diff is normal except failed by the 3-sigma rule
        expected_is_normal = [True for _ in expected_diffs]
        expected_is_normal[9] = False  # 66000 -> 0
        expected_is_normal[11] = False  # 0 -> 81000
        expected_is_normal[14] = False  # 100000 -> 550
        # pylint: disable=no-member
        self.assertMathListEqual(expected_is_normal, list(dataframe.is_normal))
        # pylint: enable=no-member

    def test_tailing_sum_1(self):
        history = [750, 650, 550, 100000, 90000, 81000, 0, 0, 66000, 55000,
                   51000, 48000, 46000, 45000, 44500, 44250, 44125]

        dataframe = get_counter_dataframe(history, constantly_growing=True, max_sigmas=2)

        expected_value = 100  # (750 - 650)
        actual_value = get_counter_dataframe_tailing_sum(dataframe, count=1, max_errors=None)
        self.assertEqual(expected_value, actual_value)

    def test_tailing_sum_2(self):
        history = [750, 650, 550, 100000, 90000, 81000, 0, 0, 66000, 55000,
                   51000, 48000, 46000, 45000, 44500, 44250, 44125]

        dataframe = get_counter_dataframe(history, constantly_growing=True, max_sigmas=2)

        # (750 - 650) + (650 - 550)
        expected_value = 200

        actual_value = get_counter_dataframe_tailing_sum(dataframe, count=2, max_errors=None)
        self.assertEqual(expected_value, actual_value)

    def test_tailing_sum_10(self):
        history = [750, 650, 550, 100000, 90000, 81000, 0, 0, 66000, 55000,
                   51000, 48000, 46000, 45000, 44500, 44250, 44125]

        # = (750 - 650) + (650 - 550) + (#invalid: 550-100k) + (100k - 90k) + (90k - 81k) +
        # + (#invalid: 81k - 0) + (0 - 0) + (#invalid: 0 - 66k) + (66k - 55k) + (55k - 51k)
        expected_value = 200 + 10000 + 9000 + 0 + 11000 + 4000

        dataframe = get_counter_dataframe(history, constantly_growing=True, max_sigmas=2)

        actual_value = get_counter_dataframe_tailing_sum(dataframe, count=10, max_errors=None)
        self.assertEqual(expected_value, actual_value)

    def test_tailing_sum_10_errors_filter(self):
        history = [750, 650, 550, 100000, 90000, 81000, 0, 0, 66000, 55000,
                   51000, 48000, 46000, 45000, 44500, 44250, 44125]

        # = (750 - 650) + (650 - 550) + (#invalid: 550-100k) + (100k - 90k) + (90k - 81k) +
        # + (#invalid: 81k - 0) + (0 - 0) + (#invalid: 0 - 66k) + (66k - 55k) + (55k - 51k)
        expected_value = 200 + 10000 + 9000 + 0 + 11000 + 4000

        dataframe = get_counter_dataframe(history, constantly_growing=True, max_sigmas=2)

        actual_value = get_counter_dataframe_tailing_sum(dataframe, count=10, max_errors=2)
        self.assertIsNone(actual_value)

        actual_value = get_counter_dataframe_tailing_sum(dataframe, count=10, max_errors=3)
        self.assertEqual(expected_value, actual_value)

    def test_tailing_sum_too_wide(self):
        history = [750, 650, 550, 100000, 90000, 81000, 0, 0, 66000, 55000,
                   51000, 48000, 46000, 45000, 44500, 44250, 44125]

        # = (750 - 650) + (650 - 550) + (#invalid: 550-100k) + (100k - 90k) + (90k - 81k) +
        # + (#invalid: 81k - 0) + (0 - 0) + (#invalid: 0 - 66k) + (66k - 55k) + (55k - 51k) +
        # + (51k - 48k) + (48k - 46k) + (46k - 45k) + (45k - 44.5k) + (44.5k - 44.25k) + (44.25k - 44.125k)
        expected_value = 200 + 10000 + 9000 + 0 + 11000 + 4000 + 3000 + 2000 + 1000 + 500 + 250 + 125

        dataframe = get_counter_dataframe(history, constantly_growing=True, max_sigmas=2)

        actual_value = get_counter_dataframe_tailing_sum(dataframe, count=len(history) + 10, max_errors=None)
        self.assertEqual(expected_value, actual_value)

    def test_tailing_sum_cast_type(self):
        history = [750, 650, 550]
        expected_type = int

        dataframe = get_counter_dataframe(history)
        value = get_counter_dataframe_tailing_sum(dataframe, count=1, cast_type=expected_type)

        actual_type = type(value)
        self.assertEqual(expected_type, actual_type)

    def test_tail_mean(self):
        history = [60, 40, 30]

        dataframe = get_counter_dataframe(history)
        value = get_counter_dataframe_tailing_diffs_mean(dataframe)

        self.assertEqual(15, value)

    def test_tail_mean_count(self):
        history = [60, 40, 30]

        dataframe = get_counter_dataframe(history)
        value = get_counter_dataframe_tailing_diffs_mean(dataframe, count=1)

        self.assertEqual(20, value)

    def test_tail_mean_offset(self):
        history = [60, 40, 30]

        dataframe = get_counter_dataframe(history)
        value = get_counter_dataframe_tailing_diffs_mean(dataframe, offset=1)

        self.assertEqual(10, value)

    def test_tail_mean_count_offset(self):
        history = [110, 70, 40, 20, 10]

        dataframe = get_counter_dataframe(history)
        value = get_counter_dataframe_tailing_diffs_mean(dataframe, offset=2, count=2)

        self.assertEqual(15, value)
