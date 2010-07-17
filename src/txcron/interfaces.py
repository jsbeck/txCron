from zope.interface import Interface

class IJob(Interface):

    def cancel():
        """Cancels the next execution of the job.  The job will 
           no longer be rescheduled until resume() is called. 
        """

    def resume():
        """Resumes the scheduling of this job."""

    def execute():
        """Executes the function associated with this job."""

    def pause():
        """After the next execution of this job, it will no 
           longer be rescheduled.
        """

    def reschedule(schedule):
        """Remove this job from the schedule and add it back 
           based on the new schedule.
        """

    def getNextExecutionDelay():
        """This method will deliver the delay in seconds the
           reactor should wait before the next execution of
           this job.
        """

class IScheduler(Interface):

    def addJob(schedule, func, *args, **kwargs):
        """Create a new UNIX cron-style job and add it to the schedule.
           Returns an object implementing the IJob interface.
        """

    def removeJob(job_id):
        """Cancels the next execution of the job and removes it 
           from the schedule.
        """

    def cancelJob(job_id):
        """ """

    def resumeJob(job_id):
        """ """

    def pauseJob(job_id):
        """ """

    def getJobs():
        """Returns a list of jobs in the scheduler."""

    def getPausedJobs():
        """Returns a list of paused jobs."""

    def getJob(job_id):
        """Returns the job that has job_id==job_id or None."""
