from condormap import condormap
import collections
import numpy


# Sample function
def logistic(r, len=10):
    d = collections.deque(maxlen=len)
    x = 0.4
    for _ in xrange(5 * 10**7):
        x = x * r * (1.0 - x)
        d.append(x)
    return list(d)


for k, d in condormap(logistic, numpy.arange(3.5, 3.6, 0.01), withdata=True):
    print sorted(d)
    t = set(round(x, 5) for x in d)
    print k, "Mode ", len(t)
