#!/usr/bin/env python

import condorsubmit
import sys

# job = condorsubmit.JobCluster.from_file(sys.argv[1])
# print job.submit()


# j = condorsubmit.Job({"Executable": "/bin/sleep", "Arguments": '2000'})
# print j.submit()


base = {'executable': '/bin/sleep', '+job_type': '"jupyter"', 'request_memory': '10M',
        'Log': 'sleep.log'}
args = ({'arguments': '100'}, {'arguments': '150'}, {'arguments': '200', '+job_type': '"foo"'})

g = condorsubmit.JobGroup.from_data(base, args)
g.wait()
