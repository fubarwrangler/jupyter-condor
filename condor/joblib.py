from job import Job, JobCluster, JobGroup
import htcondor
import classad

import tempfile
import cloudpickle


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


def condormap(fn, data):
    fd, name = tempfile.mkstemp(prefix='cmap')
    fp = fdopen(fd, 'w')
    fndata = cloudpickle.dump(fn, fp)
    fp.close()
    


if __name__ == "__main__":
    import glob
