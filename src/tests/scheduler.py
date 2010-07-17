import sys
import time
sys.path.append('/home/jhensley/projects/txCron/src')
from datetime import datetime

from twisted.trial.unittest import TestCase
from twisted.internet import defer
from twisted.internet.base import DelayedCall
from twisted.internet.error import AlreadyCalled, AlreadyCancelled

from txcron.scheduler import Scheduler, SchedulerError
from txcron.jobs import CronJob, IntervalJob, DateJob

def t_func(*args, **kwargs):
    print("Test Function:\n%s\n%s" % (args, kwargs))

class SchedulerTestCase(TestCase):

    def setUp(self):
        self.sched = Scheduler()
        DelayedCall.debug = True

    def tearDown(self):
        for j in self.sched.getJobs():
            j.cancel()

    def test_add_date_job(self):
        date_time = datetime.fromtimestamp(time.time() + 300)
        j = self.sched.addJob(date_time, t_func)
        self.assertTrue(isinstance(j, DateJob))
        self.assertEqual(self.sched, j.manager)
        self.assertEqual(j.job_id, 1)

    def test_add_interval_job(self):
        j = self.sched.addJob(300, t_func)
        self.assertTrue(isinstance(j, IntervalJob))
        self.assertEqual(self.sched, j.manager)
        self.assertEqual(j.job_id, 1)

    def test_add_cron_job(self):
        # XXX
        pass

    def test_remove_job(self):
        j = self.sched.addJob(300, t_func)
        self.assertTrue(isinstance(j, IntervalJob))
        self.assertEqual(self.sched, j.manager)
        self.assertEqual(j.job_id, 1)
        self.sched.removeJob(1)
        self.assertRaises(SchedulerError, self.sched.removeJob, 1)
        self.assertRaises(AlreadyCancelled, j._timer.cancel)

    def test_pause_job(self):
        j = self.sched.addJob(300, t_func)
        self.assertTrue(isinstance(j, IntervalJob))
        self.assertEqual(self.sched, j.manager)
        self.assertEqual(j.job_id, 1)
        self.sched.pauseJob(1)
        paused = self.sched.getPausedJobs()
        self.assertEquals(paused, [j])
        self.assertEquals(j._timer.active(), False)

    def test_reschedule_job(self):
        pass

    def test_get_job(self):
        j1 = self.sched.addJob(300, t_func)
        j2 = self.sched.addJob(500, t_func)
        j3 = self.sched.addJob(100, t_func)
        self.assertEquals(j1, self.sched.getJob(j1.job_id))
        self.assertEquals(j2, self.sched.getJob(j2.job_id))
        self.assertEquals(j3, self.sched.getJob(j3.job_id))
