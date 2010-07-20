import re
import time
from calendar import monthrange
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

MINUTE_COUNT = (MAX_MINUTE - MIN_MINUTE + 1)
HOUR_COUNT = (MAX_HOUR - MIN_HOUR + 1)
DOM_COUNT = (MAX_DOM - MIN_DOM + 1)
MONTH_COUNT = (MAX_MONTH - MIN_MONTH + 1)
DOW_COUNT = (MAX_DOW - MIN_DOW + 1)

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
CRON_FIELD_RE = re.compile('^((?P<star>\*)|(?P<begin>(\d{1,2}|[a-zA-Z]+))'\
                             '(?:-(?P<end>(\d{1,2}|[a-zA-Z]+)))?)'\
                             '(?:/(?P<step>\d{1,2}))?$')

class CronOutOfBoundsError(Exception): pass
class CronParseError(Exception): pass

class CronParser(object):

    _minutes = None
    _hours = None
    _doms = None
    _months = None
    _dows = None

    _all_minutes = False
    _all_hours = False
    _all_doms = False
    _all_months = False
    _all_dows = False

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

        (self._minutes,
         self._hours,
         self._doms,
         self._months,
         self._dows) = map(self.parseField, fields, self._mins, self._maxes)

        # Some short circuiting for faster processing getNextDateTime()
        if len(self._minutes) == MINUTE_COUNT:
            self._all_minutes = True
        if len(self._hours) == HOUR_COUNT:
            self._all_hours = True
        if len(self._doms) == DOM_COUNT:
            self._all_doms = True
        if len(self._months) == MONTH_COUNT:
            self._all_months = True
        if len(self._dows) == DOW_COUNT:
            self._all_dows = True

        # Even though the cron syntax allows Sunday to be specified by 
        # either 0 or 7, datetime.datetime is going to be operating on
        # the DOW with a range of 1-7.  Ensure that if 0 is in the 
        # self.dows list that it be replaced with 7.
        if 0 in self._dows:
            self._dows.remove(0)
            if 7 not in self._dows:
                self._dows.append(7)

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
                    raise CronParseError('Cannot supply a step with a \
                                          single specifier: %s' % (field,))

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

    # Public API

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

        # New/Start datetime fields
        nminute = sminute = start_time.minute
        nhour = shour = start_time.hour
        ndom = sdom = start_time.day
        nmonth = smonth = start_time.month
        nyear = syear = start_time.year

        # indicate whether a particular field is rolling over
        incr_minute = 1
        incr_dom = 1
        incr_dow = 1
        incr_hour = 1
        incr_month = 1
        incr_year = 1

        # Next minute in schedule
        for minute in xrange(sminute+incr_minute, MINUTE_COUNT):
            if minute in self._minutes:
                nminute = minute
                incr_hour = 0
                break

        # Try short circuit
        if self._all_hours \
        and self._all_dows \
        and self._all_months \
        and self._all_doms \
        and incr_hour == 0:
            return start_time.replace(minute=nminute, 
                                      second=0, 
                                      microsecond=0)

        # New hour, start at he 1st minute in the list
        if incr_hour == 1:
            nminute = self._minutes[0]

        # Next hour in schedule
        for hour in xrange(shour+incr_hour, HOUR_COUNT):
            if hour in self._hours:
                nhour = hour
                incr_dom = 0
                break

        # Try short circuit
        if self._all_dows \
        and self._all_months \
        and self._all_doms \
        and incr_dom == 0:
            return start_time.replace(hour=nhour, 
                                      minute=nminute, 
                                      second=0,
                                      microsecond=0)

        # New day, start at the 1st hour in the list.
        if incr_dom == 1:
            nhour = self._hours[0]

        # See if the month has to roll over
        dom_count = monthrange(syear, smonth)[1]
        for dom in xrange(sdom+incr_dom, dom_count+1):
            if dom in self._doms:
                ndom = dom
                incr_month = 0
                break

        if incr_month == 1:
            ndom = self._doms[0]

        # Short circuit
        if self._all_dows \
        and self._all_months \
        and incr_month == 0:
            return start_time.replace(day=ndom,
                                      hour=nhour,
                                      minute=nminute,
                                      second=0,
                                      microsecond=0)

        # Next month in schedule
        for month in xrange(smonth+incr_month, MONTH_COUNT+1):
            if month in self._months:
                nmonth = month
                incr_year = 0
                break

        if incr_year == 1:
            nmonth = self._months[0]
            nyear = syear + incr_year

        new_time = start_time.replace(year=nyear,
                                      month=nmonth,
                                      day=ndom,
                                      hour=nhour,
                                      minute=nminute,
                                      second=0,
                                      microsecond=0) 
        if self._all_dows:
            return new_time
        else:
            # Days of the week is a bit of a special case
            ndow = new_time.isoweekday()
            multiplier = 0
            if ndow in self._dows:
                return new_time

            for dow in xrange(ndow+incr_dow, DOW_COUNT):
                if dow in self._dows:
                    multiplier = dow - ndow
                    break
            
            if multiplier == 0:
                # the new day of the week is greater than the 
                # next dow entry in self.dows.
                multiplier = (MAX_DOW - ndow) + self._dows[0]

            # Add the number of days to new_time until the next DOW
            newtimestamp = time.mktime(new_time.timetuple()) \
                                       + (multiplier * 24 * 3600)
            return datetime.fromtimestamp(newtimestamp)
