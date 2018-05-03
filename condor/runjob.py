#!/usr/bin/env python

import job
import sys

job = job.JobCluster.from_file(sys.argv[1])
job.submit()
