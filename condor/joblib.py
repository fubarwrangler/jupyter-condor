from job import Job, JobCluster, JobGroup
import htcondor
import classad

import tempfile
import cloudpickle
import collections
import time

import os.path as path

class CondorMapper(object):

    def __init__(self, executable, inputs, argtmpl='{}', log=None, outputs=None, outtmpl='{}'):
        self.exe = executable
        self.iter = inputs
        self._arg = argtmpl
        self._jobs = list()
        self.log = log

    def submit(self):
        for x in self.iter:
            a = self._arg.format(*x)
            ad = {'Executable': self.exe, 'Arguments': a}
            if self.log:
                ad['UserLog'] = self.log
            print ad
            self._jobs.append(Job(ad))
        cids = list()
        for x in self._jobs:
            cids.append(x.submit())
        print cids
        return JobGroup(cids)


def logistic(r, len=100):
    d = collections.deque(maxlen=len)
    x = 0.4
    for _ in xrange(5 * 10**7):
        x = x * r * (1.0 - x)
        d.append(x)
    return list(d)



def condormap(fn, data):

    mydir = tempfile.mkdtemp()
    func = path.join(mydir, 'fn.pkl')

    cloudpickle.dump(fn, open(, 'w'))

    {'Executable': }
