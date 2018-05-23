#!/usr/bin/env python
import cloudpickle
import sys

import os

print >>sys.stderr, os.getcwd()
scratch_dir = os.getenv('TEMP')
os.chdir(scratch_dir)

code = cloudpickle.load(open(sys.argv[1]))
data = cloudpickle.load(sys.stdin)

cloudpickle.dump(code(data), sys.stdout)
