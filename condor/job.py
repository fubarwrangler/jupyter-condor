#!/usr/bin/python

import classad
import htcondor
import pprint
import tempfile
import sys
import os

import util


class JobCluster(object):

    def __init__(self, ad):

        self.jobad = ad
        self.jdf = file
        self.schedd = htcondor.Schedd()

    @staticmethod
    def read_file(path):
        proc = util.command_fp('condor_submit -dump - %s' % path)
        aditer = classad.parseAds(proc.stdout, parser=classad.Parser.Old)
        return aditer

    @classmethod
    def from_file(cls, path):
        jobs = list(cls.read_file(path))
        if len(jobs) == 1:
            return cls(ad=jobs)
        else:
            # FIXME: I know this doesn't work...
            return cls(ad=jobs)

    @classmethod
    def from_ad_dict(cls, d):
        return cls(classad.ClassAd(d))

    def __str__(self):
        return str(self.jobad)

    def submit(self):
        if len(self.jobad) == 1:
            self.cid = self.schedd.submit(self.jobad[0])
            # sub = htcondor.Submit(self.jobad)
            # with self.schedd.transaction() as txn:
            #     self.cid = sub.queue(txn)
        elif len(self.jobad) > 1:
            clusters = [(x, 1) for x in self.jobad[1:]]
            print clusters
            ads = []
            self.cid = self.schedd.submitMany(self.jobad[0], clusters, False, ads)
