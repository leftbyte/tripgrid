#!/usr/bin/env python

##
# TripCommon.py -
#
#    Common imports across Trip modules.
##

g_maxLatitude   =   90
g_minLatitude   =  -90
g_maxLongitude  =  180
g_minLongitude  = -180
g_numLatQueues  =   10
g_numLongQueues =   10
g_maxDelta      =    5 # movement from last point

def getTripKey(id, type):
    '''
    Translate the trip ID and type to the database Trip queue key.
    '''
    return ("tripQueue:%d" % id) + ":" + type

def getTripGridKey(x, y, type):
    '''
    Translate the location  and type to the database grid queue key.
    '''
    return ("tripGrid:%d:%d" % (x, y)) + ":" + type

def latitudeToGridQueue(locationX):
    '''
    Maps the latitude to the X coordinate of the queue.  For simplicity, we only
    handle integer location units.
    '''
    delta = (g_maxLatitude - g_minLatitude) / g_numLatQueues
    if (g_maxLatitude - g_minLatitude) % g_numLatQueues:
        raise Exception("Error: Deltas between queues are not uniform.")
    queueX = 0
    for x in range(g_minLatitude, g_maxLatitude, delta):
        if (x <= locationX < x + delta):
            return queueX
        queueX += 1
    raise Exception("Error: undefined coordinate. Range is -90:89.")

def longitudeToGridQueue(locationY):
    '''
    Maps the longitude to the Y coordinate of the queue.  For simplicity, we only
    handle integer location units.
    '''
    delta = (g_maxLongitude - g_minLongitude) / g_numLongQueues
    if (g_maxLongitude - g_minLongitude) % g_numLongQueues:
        raise Exception("Error: Deltas between queues are not uniform.")
    queueY = 0
    for y in range(g_minLongitude, g_maxLongitude, delta):
        if (y <= locationY < y + delta):
            return queueY
        queueY += 1
    raise Exception("Error: undefined coordinate. Range is -180:179.")

def locationToGridQueues(location):
    '''
    I'm so fucking lazy.  Translate a pair of locations to their grid queues.
    '''
    return (latitudeToGridQueue(location[0]), longitudeToGridQueue(location[1]))

def locationsToGridQueues(locations):
    '''
    I'm so fucking lazy.  Translate a whole array of locations to their grid queues.
    '''
    indices = []
    for l in locations:
        indices += [(locationToGridQueues(l))]
    return indices

def checkLocation(location):
    '''
    Asserts whether the location is valid in our coordinate system.
    '''
    if location[0] < g_minLatitude or location[0] > g_maxLatitude:
        raise Exception("Error: bad X location value", location)
    if location[1] < g_minLongitude or location[1] > g_maxLongitude:
        raise Exception("Error: bad Y location value", location)
