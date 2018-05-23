from job import Job, JobGroup
import htcondor
import classad

import tempfile
import cloudpickle
import collections
import time
import glob
import os

import os.path as path


class CondorMapper(object):

    def __init__(self, executable, inputs, argtmpl='{}', log=None, outputs=None, outtmpl='{}'):
        self.exe = executable
        self.in_iter = inputs
        self.out_iter = outputs
        self._arg = argtmpl
        self._outtmpl = outtmpl
        self._jobs = list()
        self.log = log

    def submit(self):
        for x in self.in_iter:
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


def logistic(r, len=4):
    d = collections.deque(maxlen=len)
    x = 0.4
    for _ in xrange(5 * 10**7):
        x = x * r * (1.0 - x)
        d.append(x)
    return list(d)


def condormap(fn, data):

    mydir = tempfile.mkdtemp(prefix="cjob-", suffix="-wkdir")
    func = path.join(mydir, 'fn.pkl')

    with open(func, 'w') as fp:
        cloudpickle.dump(fn, fp)

    in_tmpl = path.join(mydir, 'in.{}')
    jobdict = {'Executable': 'picklerunner.py', 'Arguments': func,
               'transfer_input_files': func, 'Log': path.join(mydir, 'jobs.log'), }

    jobs = list()
    for idx, item in enumerate(data):
        with open(in_tmpl.format(idx), 'w') as fp:
            cloudpickle.dump(item, fp)

        jobdict.update({"Input": path.join(mydir, 'in.%d' % idx),
                        "Output": path.join(mydir, 'out.%d' % idx),
                        "Error": path.join(mydir, 'err.%d' % idx), })
        j = Job(jobdict)
        jobs.append(j.submit())

    print jobs
    g = JobGroup(jobs)
    g.wait()

    for output in sorted(glob.glob(path.join(mydir, 'out.*')),
                         key=lambda x: int(x.split('.')[1])):
        yield cloudpickle.load(open(output))


print list(condormap(logistic, [3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9]))
