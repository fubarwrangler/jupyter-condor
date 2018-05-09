#!/usr/bin/env python

import job
import sys

# job = job.JobCluster.from_file(sys.argv[1])
# print job.submit()


# j = job.Job({"Executable": "/bin/sleep", "Arguments": '2000'})
# print j.submit()


j = job.Job.from_queue(98)
# print j.submit()
