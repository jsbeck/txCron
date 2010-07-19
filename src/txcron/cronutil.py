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
'midnight':[0, 0, '*', '*', '*'],
'hourly':[0, '*', '*', '*', '*']
}

SHORTCUT_RE = re.compile('^\@(?P<keyword>[a-z])$')
CRON_FIELD_RE = re.compile('^((?P<star>\*)|(?P<begin>(\d{1,2}|[a-zA-Z]+))(?:-(?P<end>(\d{1,2}|[a-zA-Z]+)))?)(?:/(?P<step>\d{1,2}))?$')

class CronOutOfBoundsError(Exception): pass
class CronParseError(Exception): pass

class CronParser(object):

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

        if not isinstance(cron_string, basestring):
            raise TypeError('Expected string type')

        m = SHORTCUT_RE.match(cron_string)
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

    def _convertAlpha(self, alpha, high):
        """Convert an alpha field to it's integer counterpart.
           Only the dow and month fields are allowed to contain words.
        """
        intfield = None
        if high == MAX_DOW:
            for k, v in WEEKDAYS.iteritems():
                if v.tolower().startswith(alpha.tolower(), 0, 2):
                    intfield = int(k)
                    break
        elif high == MAX_MONTH:
            for k, v in MONTHS.iteritems():
                if v.tolower().startswith(alpha.tolower(), 0, 2):
                    intfield = int(k)
                    break
        else:
            raise ValueError('Second argument must be %d or %d' % (MAX_DOW, MAX_MONTH,))

        if intfield is None:
            raise ValueError('Could not find an integer value for %s' % (alpha,))

        return intfield

    def _fieldParser(self, field, low, high):
        """Parser for a range field"""

        if low > high:
            raise ValueError('Low value must be lower than high value')
        if low < 0:
            raise ValueError('Low value must not be a negative integer')

        m = CRON_FIELD_RE.match(field)
        if not m is None:
            star = m.group('star')
            begin = m.group('begin')
            end = m.group('end')
            step = m.group('step')

            if not step is None:
                try:
                    step = int(step)
                except ValueError:
                    raise
            else:
                step = 1

            if not star is None:
                return [val for val in (low, high+1, step)]
            if not begin is None:
                try:
                    begin = int(begin)
                except ValueError:
                    begin = self._convertAlpha(begin, high)

            if end is None:
                # Ensure there is no step supplied with a single 
                # specifier, i.e. 12/3 or Oct/4.  This is invalid
                # syntax.
                if not m.group('step') is None:
                    raise CronParseError('Cannot supply a step with a single specifier: %s' % (field,))

                return [begin]
            else:
                try:
                    end = int(end)
                except ValueError:
                    end = self._convertAlpha(end)

                if begin < high and begin >= low and end <= high and end > low:
                    return [val for val in xrange(begin, end+1, step)]
                raise CronOutOfBoundsError
        else:
            raise CronParseError('Failed to parse cron entry: %s' % (field,))
        
    def parseField(self, field, low, high):
        """Parse a cron field.
           Returns a list of integers correlating to the field in which
           this schedule should run.

           Examples:
           >>> parseField('0,30,60', MIN_MINUTE, MAX_MINUTE)
           ... [0, 30]
           >>> parseField('*/5', MIN_MINUTE, MAX_MINUTE)
           ... [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
           >>> parseField('1-march,Oct-12/2', MIN_MONTH, MAX_MONTH)
           ... [1, 2, 3, 10, 12]
        """
        try:
            low = int(low)
            high = int(high)
        except ValueError:
            raise

        result = []
        for e in field.strip().split(','):
            rs = self._fieldParser(e, low, high)
            result.extend(rs)

        return result

    def getNextTimestamp(self, start_time):
        dt = self.getNextDateTime(start_time)
        return time.mktime(dt.timetuple())

    def getNextDateTime(self, start_time):
        if not isinstance(start_time, datetime):
            raise TypeError("Expecting datetime.datetime object")
        # XXX: Find the next time to run, after start_time

        return datetime.now()        
