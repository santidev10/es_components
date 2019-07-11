from datetime import datetime
from unittest import TestCase

from components.channel import ChannelSectionStats
from stats import History
from stats import HistoryValueError


class TestHistory(TestCase):
    def test_success(self):
        section = ChannelSectionStats()
        section.subscribers = 100
        section.views = 1000

        # check initial
        self.assertEqual(None, section.fetched_at)
        self.assertEqual(None, section.historydate)
        self.assertEqual([], section.subscribers_history)
        self.assertEqual([], section.views_history)

        # check first update - no history values yet
        history = History(section, ["subscribers", "views"])
        section.fetched_at = datetime(year=2020, month=1, day=3, hour=12)
        section.subscribers = 301
        section.views = 3001
        history.update()

        self.assertEqual(None, section.historydate)
        self.assertEqual([], section.subscribers_history)
        self.assertEqual([], section.views_history)

        # check second update

        history = History(section, ["subscribers", "views"])
        section.fetched_at = datetime(year=2020, month=1, day=13, hour=12)
        section.subscribers = 1301
        section.views = 13001
        history.update()

        expected_historydate = datetime(year=2020, month=1, day=12, hour=23, minute=59, second=59, microsecond=0)
        self.assertEqual(expected_historydate, section.historydate)

        self.assertEqual([1250, 1150, 1050, 950, 850, 750, 650, 550, 450, 350], section.subscribers_history)
        self.assertEqual([12500, 11500, 10500, 9500, 8500, 7500, 6500, 5500, 4500, 3500], section.views_history)


    def test_exception(self):
        section = ChannelSectionStats()
        section.fetched_at = datetime(year=2020, month=1, day=1, hour=12)

        history = History(section, ["subscribers"])
        section.fetched_at = datetime(year=2020, month=1, day=1, hour=0)
        self.assertRaises(HistoryValueError, history.update)

    def test_retention_period(self):
        max_retention_perod = 31

        section = ChannelSectionStats()
        section.subscribers = 100
        section.views = 1000

        history = History(section, ["subscribers"])
        section.subscribers = 301
        section.views = 3001
        section.fetched_at = datetime(year=2020, month=1, day=3)
        history.update()
        self.assertEqual(0, len(section.subscribers_history))

        history = History(section, ["subscribers"])
        section.fetched_at = datetime(year=2020, month=1, day=13)
        history.update()
        self.assertEqual(10, len(section.subscribers_history))

        history = History(section, ["subscribers"])
        section.fetched_at = datetime(year=2021, month=1, day=13)
        history.update()
        self.assertEqual(max_retention_perod, len(section.subscribers_history))
