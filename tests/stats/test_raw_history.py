from unittest import TestCase

from es_components.models.channel import ChannelSectionStats
from es_components.stats import RawHistory

from es_components.datetime_service import datetime_service


class TestRawHistory(TestCase):
    def test_success(self):
        section = ChannelSectionStats()
        section.subscribers = 100

        with self.subTest("check initial"):

            self.assertEqual(None, section.fetched_at)
            self.assertEqual(None, section.historydate)
            self.assertEqual([], section.subscribers_history)

        with self.subTest("check first update - no history values yet"):

            history = RawHistory(section, ["subscribers", "views"])
            section.fetched_at = datetime_service.datetime(year=2020, month=1, day=3, hour=12)
            section.subscribers = 301
            history.update()

            self.assertEqual(
                datetime_service.datetime(year=2020, month=1, day=2, hour=23, minute=59, second=59),
                section.historydate
            )
            self.assertEqual({"2020-01-02": 301}, section.subscribers_raw_history.to_dict())

        with self.subTest("check second update - history contains values"):

            history = RawHistory(section, ["subscribers", "views"])
            section.fetched_at = datetime_service.datetime(year=2020, month=1, day=13, hour=12)
            section.subscribers = 1301
            history.update()

            expected_historydate = datetime_service.datetime(year=2020, month=1, day=12, hour=23, minute=59, second=59,
                                                             microsecond=0)
            expected_raw_history = {"2020-01-02": 301, "2020-01-12": 1301}
            self.assertEqual(expected_historydate, section.historydate)
            self.assertEqual(expected_raw_history, section.subscribers_raw_history.to_dict())
