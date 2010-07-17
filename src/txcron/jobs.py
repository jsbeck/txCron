import time
from datetime import datetime

from twisted.internet import reactor, defer
from twisted.internet.error import AlreadyCalled, AlreadyCancelled
from zope.interface import implements

from txcron.interfaces import IJob

class AbstractBaseJob(object):
    _paused = False
    _cancelled = False
    _timer = None
    deferred = None
    func = None
    job_id = 0
    next_exec_time = 0
    last_exec_time = 0
    args = []
    kwargs = {}

    def __init__(self):
        raise NotImplementedError

    def _post_exec_hook(self, result):
        return result

    def getNextExecutionDelay(self):
        raise NotImplementedError

    def execute(self):
        self.last_exec_time = datetime.now()
        df = defer.maybeDeferred(self.func, *self.args, **self.kwargs)
        df.addBoth(self._post_exec_hook)
        return df

    def resume(self):
        self._paused = False
        self._cancelled = False
        delay = self.getNextExecutionDelay()
        self._timer.reset(delay)

    def cancel(self):
        self._cancelled = True
        try:
            self._timer.cancel()
        except (AlreadyCalled, AlreadyCancelled):
            pass

    def pause(self):
        self._paused = True
        try:
            self._timer.cancel()
        except (AlreadyCalled, AlreadyCancelled):
            pass

    def reschedule(self, schedule):
        raise NotImplementedError

class CronJob(AbstractBaseJob):

    implements(IJob)

    minutes = None
    hours = None
    doms = None
    months = None
    dows = None
    cron_string = None

    def __init__(self, job_id, manager, cron_string, func, *args, **kwargs):
        self.job_id = job_id
        self.manager = manager
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.cron_string = cron_string

        self.parseSchedule(cron_string)

    def getNextExecutionDelay(self):
        delay = self.next_exec_time - reactor.seconds()
        if delay < 0:
            delay = 0.1
        return delay

    def _find_next_exec_time(self):
        #XXX: figure out the next exec time
        self.next_exec_time = reactor.seconds() + 10

    def _post_exec_hook(self, result):
        self._find_next_exec_time()
        return result

    def parseSchedule(self, cron_string):
        # XXX
        pass

    def reschedule(self, cron_string):
        self.parseSchedule(cron_string)
        self.cron_string = cron_string
        self._find_next_exec_time()
        self._timer.reset(self.getNextExecutionDelay())

class DateJob(AbstractBaseJob):

    implements(IJob)

    date_time = None

    def __init__(self, job_id, manager, date_time, func, *args, **kwargs):
        self.job_id = job_id
        self.manager = manager
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.date_time = self.parseDateTime(date_time)

    def _post_exec_hook(self, result):
        self.manager.removeJob(self.job_id)
        return result

    def getNextExecutionDelay(self):
        delay = time.mktime(self.date_time.timetuple()) - reactor.seconds()
        if delay < 0:
            delay = 0.1

        return delay

    def parseDateTime(self, date_time):
        if isinstance(date_time, datetime):
            return date_time
        else:
            raise ValueError("Expected a datetime.datetime object")

    def reschedule(self, date_time):
        self.date_time = self.parseDateTime(date_time)
        self._timer.reset(self.getNextExecutionDelay())

class IntervalJob(AbstractBaseJob):

    implements(IJob)

    interval = None

    def __init__(self, job_id, manager, interval, func, *args, **kwargs):
        self.job_id = job_id
        self.manager = manager
        self.func = func
        self.args = args
        self.kwargs = kwargs

        if isinstance(interval, (float, int, long)):
            self.interval = interval
        else:
            raise ValueError("Expected an int, float or long")

    def _post_exec_hook(self, result):
        if not self.last_exec_time:
            self.last_exec_time = reactor.seconds()

        self.next_exec_time = self.last_exec_time + self.interval
        
    def getNextExecutionDelay(self):
        delay = self.next_exec_time - reactor.seconds()
        if delay < 0.1:
            delay = 0.1

        return delay

    def reschedule(self, interval):
        if isinstance(interval, (float, int, long)):
            self.interval = interval
        else:
            raise ValueError("Expected an int, float or long")

        self._timer.reset(self.getNextExecutionDelay())
