#!/usr/bin/python

import classad
import htcondor
import logging
import os

import util


class JobException(Exception):
    pass


logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                    level=logging.INFO)
log = logging.getLogger(__name__)


class Job(object):
    def __init__(self, job=None, ad=None):

        if job and ad:
            log.error("Must specify a job with either ad or job, no more")
            raise JobException("Invalid Job")
        self.job = job
        self.ad = ad
        self._jobdata = None
        self.cid = None
        self.pid = 0

        self.schedd = htcondor.Schedd()

    @classmethod
    def from_ad_dict(cls, d):
        return cls(ad=classad.ClassAd(d))

    @classmethod
    def from_queue(cls, jobid, procid=0):
        sched = htcondor.Schedd()
        if isinstance(jobid, int):
            _cid = jobid
            _pid = procid
        elif isinstance(jobid, str):
            if '.' in jobid:
                _cid, _pid = map(int, jobid.split('.'))
            else:
                _cid = int(jobid)
                _pid = procid
        else:
            raise JobException('%s.from_queue needs a string or int for jobid' % cls.__name__)

        const = 'ClusterId == %d && ProcId == %d' % (_cid, _pid)
        j = cls()
        l = list(sched.xquery(const))

        if len(l) < 1:
            raise JobException("Job %s not found in queue" % jobid)
        elif len(l) > 1:
            raise Exception('Bug! Should not happen, must return 1 job')
        j._jobdata = l[0]
        j.cid = _cid
        return j

    def __str__(self):
        return str(self.jobad)

    @property
    def _constraint(self):
        return 'ClusterId == %d && ProcId == %d' % (self.cid, self.pid)

    def _query(self, projection=[], procid=None):
        aditer = self.schedd.xquery(projection=projection,
                                    requirements=self._constraint)
        return aditer

    def submit(self):
        if self._jobdata:
            raise JobException("Job already submitted!")
        if self.ad:
            return self._submit_ad()
        else:
            self.cid = self._submit_new()
            return self.cid

    def _submit_new(self):
        with self.schedd.transaction() as txn:
            job = htcondor.Submit(self.job)
            return job.queue(txn, ad_results=self._jobdata)

    def _submit_old(self):
        return self.schedd.submit(self.ad, count=1, ad_results=self._jobdata)

    def wait(self):
        """ Wait for a job to finish """
        if not self.cid:
            raise JobException("Cannot wait(), job not submitted")

    def update(self):
        self._jobdata = list(self._query())[0]

    def get(self, attr, refresh=False):
        if refresh:
            self.update()
        return self._jobdata[attr]

    @property
    def status(self):
        return int(list(self._query(['JobStatus']))[0]['JobStatus'])


class JobCluster(Job):

    def __init__(self, clusterid=None):
        super(JobCluster, self).__init__(job=None, ad=None)
        self._jobdata = list()
        self.nproc = 0
        if clusterid:
            self.cid = clusterid
            for x in self.schedd.xquery('Clusterid == %d' % self.cid):
                self._jobdata.append(x)
            if len(self._jobdata) == 0:
                raise JobException("Job %d not found in queue" % self.cid)
            self.nproc = len(self._jobdata)

    @property
    def _constraint(self):
        return 'ClusterId == %d' % self.cid

    def __str__(self):
        return str(self.jobad)

    def _query(self, projection=[], procid=None):
        aditer = self.schedd.xquery(projection=projection,
                                    requirements=self._constraint)
        return aditer

    def submit(self):
        if self._jobdata:
            raise JobException("Job already submitted!")

    @property
    def status(self):
        return {ad['ProcId']: ad['JobStatus'] for ad in self._query(['ProcId', 'JobStatus'])}


# All properties of htcondor.JobAction that start with an upper-case letter
# are enum-like actions, so we create a method on the class for each of these
# actions (named the action but lower case, e.g. Hold -> hold() ) calling
# Schedd.act(action, cluster) and returning the result.
for act in (x for x in dir(htcondor.JobAction) if x[0].isupper()):
    method_name = act.lower()
    condor_method = getattr(htcondor.JobAction, act)

    def method(self, action=condor_method):
        if not self.cid:
            raise JobException('Cannot act upon a job not in the queue')
        return self.schedd.act(action, 'ClusterId == %d' % self.cid)

    setattr(JobCluster, method_name, method)
