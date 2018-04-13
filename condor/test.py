#!/usr/bin/env python

import job

for ad in job.JobCluster.from_file('tests/jdfs/keygen.job'):
    print 'XX', ad
