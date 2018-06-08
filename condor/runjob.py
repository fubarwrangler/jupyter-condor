#!/usr/bin/env python

import job
import sys

# job = job.JobCluster.from_file(sys.argv[1])
# print job.submit()


# j = job.Job({"Executable": "/bin/sleep", "Arguments": '2000'})
# print j.submit()


base = {'executable': '/bin/sleep', '+job_type': 'jupyter', 'request_memory': '10M'}
args = ({'arguments': '100'}, {'arguments': '150'}, {'arguments': '200'})

job.JobCluster.from_data(base, args)
