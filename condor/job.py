#!/usr/bin/python

import classad
import htcondor
import tempfile
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
            raise JobException(
                '%s.from_queue needs a string or int for jobid' % cls.__name__)

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

    def __init__(self, clusterid=None, jdf=None, jdfstr=None):
        super(JobCluster, self).__init__(job=None, ad=None)
        self._jobdata = list()
        self.nproc = 0
        if jdf and jdfstr:
            raise JobException('Cannot specify both jdf and jdfstr')
        self.jdf = jdf
        self.jdfstr = jdfstr

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

    def get(self, attr, refresh=False):
        if refresh:
            self.update()
        return {ad['ProcId']: ad[attr] for ad in self._jobdata}

    @staticmethod
    def _submit_jdf(path):
        o, e, r = util.command_str('condor_submit -terse "{}"'.format(path))
        if e or r != 0:
            log.error(
                "Error in condor_submit (rv=%d):\nMsg: %s\nErr: %s", r, o, e)
            raise JobException("condor_submit failed: %s", e)
        ranges = [x.strip() for x in o.split('-')]
        cid = int(ranges[0].split('.')[0])
        nproc = int(ranges[1].split('.')[1])
        return cid, nproc

    def submit(self):
        if self.jdfstr:
            fd, nam = tempfile.mkstemp()
            stream = fdopen(fd)
            stream.write(self.jdfstr)
            path = nam
            stream.close()
        else:
            path = self.jdf
        self.cid, self.nproc = self._submit_jdf(path)
        return self.cid

    def from_jdf(cls, path):
        j = cls(jdf=path)
        j.cid, j.nproc = j.submit()

    def update(self):
        self._jobdata = list(self._query())

    @property
    def status(self):
        return {ad['ProcId']: ad['JobStatus'] for ad in self._query(['ProcId', 'JobStatus'])}


class JobGroup(JobCluster):

    def __init__(self, clusterids):
        super(JobGroup, self).__init__()
        self.cid = clusterids

    @property
    def _constraint(self):
        return 'stringListMember(string(ClusterId), "%s")' % ','.join(map(str, self.cid))

    @property
    def status(self):
        return {'%d.%d'% (ad['ClusterId'], ad['ProcId']): ad['JobStatus']
                for ad in self._query(['ClusterId', 'ProcId', 'JobStatus'])}


# All properties of htcondor.JobAction that start with an upper-case letter
# are enum-like actions, so we create a method on the class for each of these
# actions (named the action but lower case, e.g. Hold -> hold() ) calling
# Schedd.act(action, cluster) and returning the result.
for act, condor_method in htcondor.JobAction.names.iteritems():
    method_name = act.lower()

    def method(self, action=condor_method):
        if not self.cid:
            raise JobException('Cannot act upon a job not in the queue')
        return self.schedd.act(action, self._constraint)

    setattr(Job, method_name, method)
