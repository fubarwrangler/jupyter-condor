#!/usr/bin/env python

import job
import sys

# job = job.JobCluster.from_file(sys.argv[1])
# print job.submit()


j = job.JobCluster({"Executable": "/bin/sleep", "Arguments": '2000'})
print j.submit()
