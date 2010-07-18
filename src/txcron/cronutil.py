import re
import time
from datetime import datetime

MIN_MINUTE = 0
MIN_HOUR = 0
MIN_DOM = 1
MIN_MONTH = 1
MIN_DOW = 0 # Sunday can count as 0 or 7

MAX_MINUTE = 59
MAX_HOUR = 23
MAX_DOM = 31
MAX_MONTH = 12
MAX_DOW = 7

MONTHS = {
1:'january',
2:'february',
3:'march',
4:'april',
5:'may',
6:'june',
7:'july',
8:'august',
9:'september',
10:'october',
11:'november',
12:'december'
}

WEEKDAYS = {
0:'sunday',
1:'monday',
2:'tuesday',
3:'wednesday',
4:'thursday',
5:'friday',
6:'saturday',
7:'sunday'
}

SHORTCUTS = {
'yearly':[0, 0, 1, 1, '*'],
'annually':[0, 0, 1, 1, '*'],
'monthly':[0, 0, 1, '*', '*'],
'weekly':[0, 0, '*', '*', 0],
'daily':[0, 0, '*', '*', '*'],
'midnight':[0, 0, '*', '*', '*']
'hourly':[0, '*', '*', '*', '*']
}

SHORTCUT_RE = re.compile('^\@(?P<keyword>[a-z])$')
RANGE_RE = re.compile('^(?P<begin>[\d{1,2}|[a-z]])-(?P<end>[\d{1,2}|[a-z])$')
STEP_RE = re.compile('^\*/(?P<step>\d{1,2})$')

class CronScheduler(object):

    minutes = None
    hours = None
    doms = None
    months = None
    dows = None

    _mins = [
        MIN_MINUTE,
        MIN_HOUR,
        MIN_DOM,
        MIN_MONTH,
        MIN_DOW
    ]

    _maxes = [
        MAX_MINUTE,
        MAX_HOUR,
        MAX_DOM,
        MAX_MONTH,
        MAX_DOW
    ]

    def __init__(self, cron_string):
        """
           Shortcuts:

           Entry      Description         Equivalent To
           =========  =================   =============
           @yearly    Run once a year     0 0 1 1 *
           @annually  (same as @yearly)   0 0 1 1 *
           @monthly   Run once a month    0 0 1 * *
           @weekly    Run once a week     0 0 * * 0
           @daily     Run once a day      0 0 * * *
           @midnight  (same as @daily)    0 0 * * *
           @hourly    Run once an hour    0 * * * *

            Otherwise, cron_string is expected to be a string
            of 0-5 fields seperated by whitespace.  Cron takes
            5 fields, but if less are supplied, the missing
            fields will be filled in from left to right with
            asterisks ('*').
        """

        if not isinstance(basestring, cron_string):
            raise TypeError('Expected string type')

        m = SHORTCUT_RE.match(cron_string):
        if not m is None:
            # Found a shortcut
            try:
                fields = SHORTCUTS.get(m.group('keyword'))
            except KeyError:
                raise ValueError('Unknown shortcut value: %s' % (cron_string,))
        else:
            fields = re.split('\s+', cron_string.strip())
            if len(fields) > 5:
                raise ValueError('Too many fields in cron string')
            while len(fields) < 5:
                fields.append('*')

            self.minutes,
            self.hours,
            self.doms,
            self.months,
            self.dows = map(self.parseField, fields, self._mins, self._maxes)

    def parseField(self, field, low, high):
        # XXX
        pass

    def getNextTimestamp(self, f, start_time):
        dt = self.getNextDateTime(f, start_time)
        return time.mktime(dt.timetuple())

    def getNextDateTime(self, f, start_time):
        if not isinstance(datetime, start_time):
            raise TypeError("Expecting datetime.datetime object")
        # XXX
        pass        
