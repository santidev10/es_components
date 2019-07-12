from unittest import TestCase

from es_components.models.channel import ChannelSectionStats
from es_components.stats import History
from es_components.stats import HistoryValueError

from es_components.datetime_service import datetime_service


class TestHistory(TestCase):
    def test_success(self):
        section = ChannelSectionStats()
        section.subscribers = 100

        # check initial
        self.assertEqual(None, section.fetched_at)
        self.assertEqual(None, section.historydate)
        self.assertEqual([], section.subscribers_history)

        # check first update - no history values yet
        history = History(section, ["subscribers", "views"])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=3, hour=12)
        section.subscribers = 301
        history.update()

        self.assertEqual(
            datetime_service.datetime(year=2020, month=1, day=2, hour=23, minute=59, second=59),
            section.historydate
        )
        self.assertEqual([], section.subscribers_history)

        # check second update - history contains values
        history = History(section, ["subscribers", "views"])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=13, hour=12)
        section.subscribers = 1301
        history.update()

        expected_historydate = datetime_service.datetime(year=2020, month=1, day=12, hour=23, minute=59, second=59,
                                                         microsecond=0)
        self.assertEqual(expected_historydate, section.historydate)
        self.assertEqual([1250, 1150, 1050, 950, 850, 750, 650, 550, 450, 350], section.subscribers_history)

    def test_same_date(self):
        section = ChannelSectionStats()
        section.subscribers = 100

        # check initial
        self.assertEqual(None, section.fetched_at)
        self.assertEqual(None, section.historydate)
        self.assertEqual([], section.subscribers_history)

        # check first update - no history values yet
        history = History(section, ["subscribers", "views"])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=3, hour=12)
        section.subscribers = 301
        history.update()

        self.assertEqual(
            datetime_service.datetime(year=2020, month=1, day=2, hour=23, minute=59, second=59),
            section.historydate
        )
        self.assertEqual([], section.subscribers_history)

        # check second update - no history values because of the same date
        history = History(section, ["subscribers", "views"])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=3, hour=20)
        section.subscribers = 1301
        history.update()

        expected_historydate = datetime_service.datetime(year=2020, month=1, day=2, hour=23, minute=59, second=59,
                                                         microsecond=0)
        self.assertEqual(expected_historydate, section.historydate)
        self.assertEqual([], section.subscribers_history)

    def test_exception_current_time_less_then_previous(self):
        section = ChannelSectionStats()
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=1, hour=12)

        history = History(section, ["subscribers"])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=1, hour=0)
        self.assertRaises(HistoryValueError, history.update)

    def test_retention_period(self):
        max_retention_perod = 31
        section = ChannelSectionStats()
        section.subscribers = 100
        section.views = 1000

        history = History(section, ["subscribers"])
        section.subscribers = 301
        section.views = 3001
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=3)
        history.update()
        self.assertEqual(0, len(section.subscribers_history))

        history = History(section, ["subscribers"])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=13)
        history.update()
        self.assertEqual(10, len(section.subscribers_history))

        history = History(section, ["subscribers"])
        section.fetched_at = datetime_service.datetime(year=2021, month=1, day=13)
        history.update()
        self.assertEqual(max_retention_perod, len(section.subscribers_history))

    def test_missing_values(self):
        section = ChannelSectionStats()
        # check 2-nd complete update
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=13, hour=12)
        section.subscribers = 1301
        section.historydate = datetime_service.datetime(year=2020, month=1, day=12, hour=23, minute=59, second=59,
                                                        microsecond=0)
        section.subscribers_history = [1250, 1150, 1050, 950, 850, 750, 650, 550, 450, 350]

        # check 1-st missed value
        history = History(section, ["subscribers",])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=15, hour=12)
        section.subscribers = None
        section.views = 15001
        history.update()

        expected_historydate = datetime_service.datetime(year=2020, month=1, day=14, hour=23, minute=59, second=59,
                                                         microsecond=0)
        self.assertEqual(expected_historydate, section.historydate)
        self.assertEqual([None, None, 1250, 1150, 1050, 950, 850, 750, 650, 550, 450, 350],
                         section.subscribers_history)

        # check 2-nd missed value
        history = History(section, ["subscribers"])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=16, hour=12)
        section.subscribers = None
        history.update()

        expected_historydate = datetime_service.datetime(year=2020, month=1, day=15, hour=23, minute=59, second=59,
                                                         microsecond=0)
        self.assertEqual(expected_historydate, section.historydate)
        self.assertEqual([None, None, None, 1250, 1150, 1050, 950, 850, 750, 650, 550, 450, 350],
                         section.subscribers_history)

        # check 3-nd complete update
        history = History(section, ["subscribers"])
        section.fetched_at = datetime_service.datetime(year=2020, month=1, day=18, hour=12)
        section.subscribers = 1801
        history.update()

        expected_historydate = datetime_service.datetime(year=2020, month=1, day=17, hour=23, minute=59, second=59,
                                                         microsecond=0)
        self.assertEqual(expected_historydate, section.historydate)
        self.assertEqual([1750, 1650, 1550, 1450, 1350, 1250, 1150, 1050, 950, 850, 750, 650, 550, 450, 350],
                         section.subscribers_history)

        # check missed all values
        history = History(section, ["subscribers"])
        section.fetched_at = datetime_service.datetime(year=2020, month=2, day=18, hour=12)
        section.subscribers = None
        section.views = None
        history.update()

        expected_historydate = datetime_service.datetime(year=2020, month=2, day=17, hour=23, minute=59, second=59,
                                                         microsecond=0)
        self.assertEqual(expected_historydate, section.historydate)
        self.assertEqual([], section.subscribers_history)
