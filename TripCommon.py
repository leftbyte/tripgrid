#!/usr/bin/env python

##
# TripCommon.py -
#
#    Common imports across Trip modules.
##

g_maxLatitude = 90
g_minLatitude = -90
g_maxLongitude = 180
g_minLongitude = -180
g_numLatQueues = 10
g_numLongQueues = 10

def getTripKey(id, type):
    return ("tripQueue:%d" % id) + ":" + type

def getTripGridKey(x, y, type):
    return ("tripGrid:%d:%d" % (x, y)) + ":" + type

def LatitudeToGridQueue(location):
    '''
    Maps the latitude to the X coordinate of the queue.  For simplicity, we only
    handle integer location units.
    '''
    delta = (g_maxLatitude - g_minLatitude) / g_numLatQueues
    if (g_maxLatitude - g_minLatitude) % g_numLatQueues:
        raise Exception("Error: Deltas between queues are not uniform.")
    queueX = 0
    for x in range(g_minLatitude, g_maxLatitude, delta):
        if (x <= location < x + delta):
            return queueX
        queueX += 1
    return queueX

def LongitudeToGridQueue(location):
    '''
    Maps the longitude to the Y coordinate of the queue.  For simplicity, we only
    handle integer location units.
    '''
    delta = (g_maxLongitude - g_minLongitude) / g_numLongQueues
    if (g_maxLongitude - g_minLongitude) % g_numLongQueues:
        raise Exception("Error: Deltas between queues are not uniform.")
    queueY = 0
    for y in range(g_minLongitude, g_maxLongitude, delta):
        if (y <= location < y + delta):
            return queueY
        queueY += 1
    return queueY
