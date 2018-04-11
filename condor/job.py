#!/usr/bin/python

import htcondor
import tempfile
import sys
import os

# schedd = htcondor.Schedd()
#
# filelist = sys.argv[1:]
# code = sys.stdin.read()
#
# fd, pyfilename = tempfile.mkstemp(suffix='.py', dir='workdir')
# pyfile = os.fdopen(fd, 'w')
# pyfile.write(code)
# pyfile.close()
#
jobdef = {
    'Executable': '/usr/bin/python',
    'Arguments': pyfilename,
    'Output': 'foo.$(Clusterid).$(Procid)',
}
#
# submit = htcondor.Submit(jobdef)
# with schedd.transaction() as tx:
#     for f in filelist:
#         submit["Input"] = f
#         submit.queue(tx)


class JobCluster(object):
    def __init__(self, command, args, env=None):
        self.command = command
        self.args = args
        self.env = os.environ if env is None else env

    def from_file(path):
        o, _, _ = util.command('condor_submit -dump %s' % path)
        print o
