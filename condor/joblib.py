from job import Job, JobCluster, JobGroup
import htcondor
import classad


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


if __name__ == "__main__":
    import glob

    g = CondorMapper('tests/code/longsum.sh',
                     zip(sorted(glob.glob('tests/data/rand.*')),
                         ['tests/output/out.%d' % (x+1) for x in range(9)]),
                         '{0} {1}').submit()
