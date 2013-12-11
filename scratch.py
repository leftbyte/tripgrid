# tripQueue = [[0 for x in xrange(5)] for x in xrange(3)]


# d[4]['fare'] = 4
# d[4]['fare'] = 4
# >>> d[4]['fare']
# d[4]['fare']
# 4
# >>> d[4]['points']
# d[4]['points']
# ()
# >>> d
# d
# {4: {'fare': 4, 'points': ()}}

>>> mydict[4]['locations'] = []
>>> mydict[4]['locations'].append((2,3))
mydict[4]['locations'].append((2,3))
>>> mydict
mydict
{4: {'fare': 0, 'locations': [(2, 3)]}}

>>> myq = [[() for x in xrange(5)] for x in xrange(3)]
myq = [[() for x in xrange(5)] for x in xrange(3)]
>>> myq
myq
[[(), (), (), (), ()], [(), (), (), (), ()], [(), (), (), (), ()]]
>>> myq[1][1].append((4,5))
myq[1][1].append((4,5))
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
AttributeError: 'tuple' object has no attribute 'append'
>>> myq = [[[] for x in xrange(5)] for x in xrange(3)]
myq = [[[] for x in xrange(5)] for x in xrange(3)]
>>> myq[1][1].append((4,5))
myq[1][1].append((4,5))
>>> myq
myq
[[[], [], [], [], []], [[], [(4, 5)], [], [], []], [[], [], [], [], []]]

>>> myq = [[{} for x in xrange(5)] for x in xrange(3)]
myq = [[{} for x in xrange(5)] for x in xrange(3)]
>>> myq
myq
[[{}, {}, {}, {}, {}], [{}, {}, {}, {}, {}], [{}, {}, {}, {}, {}]]
>>> myq[1][1] = {'fare':0, 'locations':[]}
myq[1][1] = {'fare':0, 'locations':[]}
>>> myq[1][1]['locations'].append(23,32)
myq[1][1]['locations'].append(23,32)
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
TypeError: append() takes exactly one argument (2 given)
>>> myq[1][1]['locations'].append((23,32))
myq[1][1]['locations'].append((23,32))
>>> myq
myq
[[{}, {}, {}, {}, {}], [{}, {'fare': 0, 'locations': [(23, 32)]}, {}, {}, {}], [{}, {}, {}, {}, {}]]
>>> 
>>> myq[0][0] = {'fare':0, 'locations':[(4,5)]}
myq[0][0] = {'fare':0, 'locations':[(4,5)]}
>>> print myq[0][0]['locations']
print myq[0][0]['locations']
[(4, 5)]
