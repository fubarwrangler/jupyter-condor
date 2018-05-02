#!/usr/bin/python

import classad
import htcondor
import tempfile
import sys
import os

import util


class JobCluster(object):
    def __init__(self, ad):
        self.jobad = ad

    @staticmethod
    def read_file(path):
        proc = util.command_fp('condor_submit -dump - %s' % path)
        aditer = classad.parseAds(proc.stdout, parser=classad.Parser.Old)
        return aditer

    @classmethod
    def from_file(cls, path):
        jobs = list(cls.read_file(path))
        if len(jobs) == 1:
            return cls(jobs[0])
        else:
            # FIXME: I know this doesn't work...
            return [cls(x) for x in jobs]

    @classmethod
    def from_ad_dict(cls, d):
        return cls(classad.ClassAd(d))

    def from_dict(f):
        pass

    def __str__(self):
        return str(self.jobad)
