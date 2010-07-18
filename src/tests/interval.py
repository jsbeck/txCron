import os
import sys
sys.path.append(os.path.dirname(os.getcwd()))

from twisted.trial.unittest import TestCase

from txcron.scheduler import Scheduler
from txcron.jobs import IntervalJob

class IntervalJobTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass
