import math
from unittest import TestCase


class MathTestCase(TestCase):
    # pylint: disable=invalid-name
    def assertMathListEqual(self, list1, list2, msg=None):
        nan = type("nan", (object,), {})
        list1 = [nan if math.isnan(val) else val for val in list1]
        list2 = [nan if math.isnan(val) else val for val in list2]
        self.assertListEqual(list1, list2, msg)
