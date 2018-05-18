#!/usr/bin/python

import os, pickle, time, random

testlog = '/tmp/test.log'

events = pickle.load(open('foolog'))

with open(testlog, 'w') as log:
    for x in events:
        time.sleep(random.random() * 2)
        log.write(x)
        log.flush()
