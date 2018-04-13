#!/usr/bin/python

import classad
import htcondor
import tempfile
import sys
import os

import util


class JobCluster(object):
    def __init__(self, classad):
        self.jobad = classad

    @staticmethod
    def read_file(path):
        proc = util.command_fp('condor_submit -dump - %s' % path)
        ad = classad.parseAds(proc.stdout, parser=classad.Parser.Old)
        return ad

    @classmethod
    def from_file(cls, path):
        jobs = list(cls.read_file(path))
        if len(jobs) == 1:
            return cls(jobs[0])
        else:
            # FIXME: I know this doesn't work...
            return [cls(x) for x in jobs]

    def from_dict(cls, dict):
        self.ad = ad
