#!/usr/bin/env python
import cloudpickle
import sys

import os

print os.getcwd()
scratch_dir = os.getenv('TEMP')
os.chdir(scratch_dir)
print os.getcwd()

data = sys.argv[2]
code = cloudpickle.load(open(sys.argv[1]))

cloudpickle.dump(code(data), open('output', 'w'))
