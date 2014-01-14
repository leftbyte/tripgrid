#!/usr/bin/env python

##
# TripCommon.py -
#
#    Common imports across Trip modules.
##

g_minLatitude   =  -90
g_maxLatitude   =   90
g_minLongitude  = -180
g_maxLongitude  =  180
g_numLatQueues  =   10
g_numLongQueues =   10
g_maxDelta      =    5 # movement from last point

# XXX Change to Enum if using Python 3.4
TOP_LEFT        = 1
TOP_RIGHT       = 2
BOTTOM_RIGHT    = 3
BOTTOM_LEFT     = 4
LEFT_EDGE       = 5
TOP_EDGE        = 6
RIGHT_EDGE      = 7
BOTTOM_EDGE     = 8

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
    # maximum value inclusive
    if (locationX != g_maxLatitude):
        raise Exception("Error: undefined coordinate %d. Latitude range is -90:90."
                        % locationX)
    return queueX - 1

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
    # maximum value inclusive
    if (locationY != g_maxLongitude):
        raise Exception ("Error: undefined coordinate %d. Longitude range is -180:180."
                         % locationY)
    return queueY - 1

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
