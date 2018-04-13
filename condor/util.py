from subprocess import Popen, PIPE
import shlex


def command_str(arg):
    """ Return output, error, and return value of shelling out @arg """
    proc = Popen(shlex.split(arg), stdout=PIPE, stderr=PIPE)
    o, e = proc.communicate()
    return o, e, proc.returncode

def command_fp(arg):
    """ Return output, error, and return value of shelling out @arg """
    return Popen(shlex.split(arg), stdout=PIPE, stderr=PIPE)
