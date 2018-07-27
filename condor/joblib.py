from job import Job, JobGroup

import tempfile
import cloudpickle
import collections
import numpy
import glob
import os

import os.path as path


def condormap(fn, data, batchsize=1, tmpdir=None, cleanup=True, withdata=False):

    mydir = tempfile.mkdtemp(prefix="cjob-", suffix="-wkdir")
    func = path.join(mydir, 'fn.pkl')

    with open(func, 'w') as fp:
        cloudpickle.dump(fn, fp)

    in_tmpl = path.join(mydir, 'in.{}')
    jobdict = {'Executable': 'picklerunner.py', 'Arguments': func,
               'transfer_input_files': func, 'Log': path.join(mydir, 'jobs.log'), }

    jobspecs = list()
    for idx, item in enumerate(data):
        with open(in_tmpl.format(idx), 'w') as fp:
            cloudpickle.dump(item, fp)

        jobspecs.append({"Input": path.join(mydir, 'in.%d' % idx),
                         "Output": path.join(mydir, 'out.%d' % idx),
                         "Error": path.join(mydir, 'err.%d' % idx), })

    jobs = JobGroup.from_data(jobdict, jobspecs)
    jobs.wait()

    for idx, output in enumerate(sorted(glob.glob(path.join(mydir, 'out.*')),
                                        key=lambda x: int(x.split('.')[1]))):
        res = cloudpickle.load(open(output))
        yield (data[idx], res) if withdata else res

    if cleanup:
        for f in os.listdir(mydir):
            os.unlink(path.join(mydir, f))
        os.rmdir(mydir)


# Sample function
def logistic(r, len=10):
    d = collections.deque(maxlen=len)
    x = 0.4
    for _ in xrange(5 * 10**7):
        x = x * r * (1.0 - x)
        d.append(x)
    return list(d)


for k, d in condormap(logistic, numpy.arange(3.5, 3.6, 0.01), withdata=True):
    print sorted(d)
    t = set(round(x, 5) for x in d)
    print k, "Mode ", len(t)
