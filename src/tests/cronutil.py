import os
import sys
sys.path.append(os.path.dirname(os.getcwd()))

from twisted.trial.unittest import TestCase

from txcron.cronutil import CronParser

class CronParserTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parseSimpleField(self):
        """Parse a simple cron field, i.e. '*', 12"""
        pass
    
    def test_parseStepField(self):
        """Parsea step cron field, i.e. */5"""
        pass

    def test_parseRangeField(self):
        """Parse a range cron field, i.e. 1-6"""
        pass

    def test_parseComplexField(self):
        """Parse a field that contains the a combination 
           of simple, step & range entries.
        """
        pass
