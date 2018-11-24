from condorsubmit import JobGroup

import tempfile
import cloudpickle

import glob
import os

import os.path as path


def condormap(fn, data, batchsize=1, tmpdir=None, cleanup=True, withdata=False):

    mydir = tempfile.mkdtemp(prefix="cjob-", suffix="-wkdir")
    func = path.join(mydir, 'fn.pkl')

    with open(func, 'w') as fp:
        cloudpickle.dump(fn, fp)

    in_tmpl = path.join(mydir, 'in.{}')
    runner = path.join(path.dirname(path.abspath(__file__)), 'picklerunner.py')
    jobdict = {'Executable': runner, 'Arguments': func, 'GetEnv': 'true',
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
