from datetime import datetime

from zope.interface import implements
from twisted.internet import reactor, defer

from txcron.interfaces import IScheduler
from txcron.jobs import CronJob, DateJob, IntervalJob

class SchedulerError(Exception): pass

class Scheduler(object):

    implements(IScheduler) 

    __jobIdIter = 0
    __tasklist = {}

    def __init__(self):
        pass

    def _getNextJobId(self):
        self.__jobIdIter = self.__jobIdIter + 1
        return self.__jobIdIter

    # Public API

    def addJob(self, schedule, func, *args, **kwargs):
        """Create a new CronJob, IntervalJob or DateJob and add
           it to the schedule.

           Returns an object implementing the txcron.interfaces.IJob 
           interface
        """

        if not callable(func):
            raise ValueError("'func' must be callable")

        job_id = self._getNextJobId()

        if isinstance(schedule, (int, long, float)):
            job = IntervalJob(job_id, self, schedule, func, *args, **kwargs)
        elif isinstance(schedule, datetime):
            job = DateJob(job_id, self, schedule, func, *args, **kwargs)
        #elif XXX: schedule matches cron string regex
        #   job = CronJob(job_id, self, schedule, func, *args, **kwargs)
        else:
            raise ValueError("Could not evaluate which job type \
                              to create based on the schedule")

        self.__tasklist[job_id] = job
        self.scheduleJob(job_id)
        return job

    def removeJob(self, job_id):
        try:
            job = self.__tasklist[job_id]
        except KeyError:
            raise SchedulerError("Job %d not found" % (job_id,))

        job.cancel()
        del self.__tasklist[job_id]

    def cancelJob(self, job_id):
        try:
            job = self.__tasklist[job_id]
        except KeyError:
            raise SchedulerError("Job %d not found" % (job_id,))

        job.cancel()

    def pauseJob(self, job_id):
        try:
            job = self.__tasklist[job_id]
        except KeyError:
            raise SchedulerError("Job %d not found" % (job_id,))

        job.pause()

    def resumeJob(self, job_id):
        try:
            job = self.__tasklist[job_id]
        except KeyError:
            raise SchedulerError("Job %d not found" % (job_id,))

        job.resume()

    def scheduleJob(self, job_id):
        try:
            job = self.__tasklist[job_id]
        except KeyError:
            raise SchedulerError("Job %d not found" % (job_id,))

        delay = job.getNextExecutionDelay()
        if delay < 0.0:
            delay = 0.1

        if job._timer and job._timer.active():
            # XXX: should throw an error here?
            job._timer.reset(delay)
        else:
            job._timer = reactor.callLater(delay, job.execute)

    def getJob(self, job_id):
        try:
            return self.__tasklist[job_id]
        except KeyError:
            return None

    def getJobs(self):
        return self.__tasklist.values()

    def getPausedJobs(self):
        return [job for job in self.__tasklist.values() if job._paused is True]
