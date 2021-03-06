import time
import copy
from datetime import datetime

from twisted.internet import reactor, defer
from twisted.internet.error import AlreadyCalled, AlreadyCancelled
from zope.interface import implements

from txcron.interfaces import IJob
from txcron.cronutil import CronParser

class AbstractBaseJob(object):
    _paused = False
    _cancelled = False
    _timer = None
    _user_callbacks = []
    _user_errbacks = []
    func = None
    job_id = 0
    next_exec_time = 0
    last_exec_time = 0
    times_executed = 0
    args = []
    kwargs = {}

    def __init__(self):
        raise NotImplementedError

    def _post_exec_hook(self, result):
        return result

    def getNextExecutionDelay(self):
        raise NotImplementedError

    def addCallback(self, func, *args, **kwargs):
        """ Convenience method to add additional callbacks to the function
            to be executed by this scheduled job.
        """
        if not callable(func):
            raise TypeError('%s must be callable' % (func,))

        self._user_callbacks.append((func, *args, **kwargs,))

    def addErrback(self, func, *args, **kwargs):
        """ Convenience method to add additional errbacks to the function
            to be executed by this scheduled job.
        """
        if not callable(func):
            raise TypeError('%s must be callable' % (func,))

        self._user_errbacks.append((func, *args, **kwargs,))

    def execute(self):
        self.last_exec_time = reactor.seconds()
        self.times_executed = self.times_executed + 1

        # Here 3 Deferred() object callback chains are going to be chained 
        # together.  The first Deferred, main_df is triggered at the specified
        # time based on the schedule for this job.  The second Deferred, 
        # user_df is built from the callbacks & errbacks added via the job 
        # addCallback and addErrback interface.  The user callbacks/errbacks 
        # are called on every iteration of the jobs execution.  The third and 
        # final callback chain, post_df is a Deferred that contains the 
        # callbacks necessary to reschedule this job.
        #
        # The chain will go:
        # main_df
        #  user_df
        #   post_df
        main_df = defer.maybeDeferred(self.func, *self.args, **self.kwargs)
        user_df = defer.Deferred()
        for f in self._user_callbacks: 
            user_df.addCallback(f[0], *f[1], **f[2])

        for f in self._user_errbacks:
            user_df.addErrback(f[0], *f[1], **f[2])

        post_df = defer.Deferred()
        post_df.addBoth(self._post_exec_hook)

        main_df.chainDeferred(user_df)
        user_df.chainDeferred(post_df)

        return main_df

    def resume(self):
        self._paused = False
        self._cancelled = False
        self.manager.scheduleJob(self.job_id)

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

    cron_string = None
    schedule = None

    def __init__(self, job_id, manager, cron_string, func, *args, **kwargs):
        self.df = defer.Deferred()
        self.job_id = job_id
        self.manager = manager
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.cron_string = cron_string
        self.schedule = CronParser(cron_string)

    def getNextExecutionDelay(self):
        if not self.next_exec_time:
            self.next_exec_time = self.schedule.getNextTimestamp(datetime.now())

        delay = self.next_exec_time - reactor.seconds()
        if delay < 0:
            delay = 0.1
        return delay

    def _post_exec_hook(self, result):
        self.next_exec_time = self.schedule.getNextTimestamp(datetime.now())
        self.manager.scheduleJob(self.job_id)
        return result

    def reschedule(self, cron_string):
        self.cron_string = cron_string
        self.schedule = CronScheduler(cron_string)
        self.next_exec_time = self.schedule.getNextTimestamp(datetime.now())
        self._timer.reset(self.getNextExecutionDelay())

class DateJob(AbstractBaseJob):

    implements(IJob)

    date_time = None

    def __init__(self, job_id, manager, date_time, func, *args, **kwargs):
        self.df = defer.Deferred()
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
    iterations = 0
    times_executed = 0

    # XXX: how to pass/set iterations?
    def __init__(self, job_id, manager, interval, func, *args, **kwargs):
        self.df = defer.Deferred()
        self.job_id = job_id
        self.manager = manager
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.iterations = kwargs.get('iterations', 0)

        if isinstance(interval, (float, int, long)):
            self.interval = interval
        else:
            raise ValueError("Expected an int, float or long")

    def _post_exec_hook(self, result):
        if not self.last_exec_time:
            self.last_exec_time = reactor.seconds()

        self.times_executed += 1
        self.next_exec_time = self.last_exec_time + self.interval
        if self.iterations and self.iterations >= self.times_executed:
            self.manager.removeJob(self.job_id)
        else:
            self.manager.scheduleJob(self.job_id)

        return result

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
