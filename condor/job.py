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


class JobCluster(object):

    def __init__(self, job=None, ad=None, file=None):

        if [x is not None for x in (job, ad, file)].count(True) > 1:
            log.error("Must specify a job with either ad or file or job, no more")
            raise JobException("Invalid Job")
        self.job = job
        self.ad = ad
        self.jdf = file
        self._jobdata = list()

        self.schedd = htcondor.Schedd()

    @classmethod
    def from_file(cls, path):
        if not os.path.exists(path):
            log.warning("Unable to find job-description %s", path)
            return None
        return cls(file=path)

    @classmethod
    def from_ad_dict(cls, d):
        return cls(ad=classad.ClassAd(d))

    @classmethod
    def from_queue(cls, clusterid):
        sched = htcondor.Schedd()
        const = 'ClusterId == %d' % clusterid
        j = cls()
        for x in sched.xquery(const):
            j._jobdata.append(x)
        if len(j._jobdata) == 0:
            raise JobException("Job %d not found in queue" % clusterid)
        j.cid = clusterid
        return j

    def __str__(self):
        return str(self.jobad)

    def _submit_jdf(self):
        o, e, r = util.command_str('condor_submit -terse "{}"'.format(self.jdf))
        if e or r != 0:
            log.error("Error in condor_submit (rv=%d):\nMsg: %s\nErr: %s", r, o, e)
            return None
        ranges = [x.strip() for x in o.split('-')]
        print ranges
        self.cid = int(ranges[0].split('.')[0])
        self.nproc = int(ranges[1].split('.')[1])
        for x in self._query():
            self._jobdata.append(x)
        return self.cid

    def _query(self, projection=[], procid=None):
        aditer = self.schedd.xquery(projection=projection,
                                    requirements='ClusterId == %d' % self.cid)
        return aditer

    def submit(self):
        if self._jobdata:
            raise JobException("Job already submitted!")
        if self.jdf:
            return self._submit_jdf()
        elif self.ad:
            return self._submit_ad()
        else:
            self.cid = self._submit_new()
            return self.cid

    def _submit_new(self):
        with self.schedd.transaction() as txn:
            job = htcondor.Submit(self.job)
            self.nproc = 1
            return job.queue(txn, ad_results=self._jobdata)

    def wait(self):
        """ Wait for a job to finish """
        if not self.cid:
            raise JobException("Cannot wait(), job not submitted")

    @property
    def status(self):
        for x in self._query(['ProcId', 'JobStatus']):
            pass


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
        print action
        return self.schedd.act(action, 'ClusterId == %d' % self.cid)

    print method

    setattr(JobCluster, method_name, method)
