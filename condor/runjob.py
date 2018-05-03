#!/usr/bin/env python

import job
import sys

#job = job.JobCluster.from_file(sys.argv[1])
#job.submit()


j = job.JobCluster({"Executable": "/bin/sleep", "Arguments": '20'})
print j.submit()
