#!/usr/bin/env python

##
# TripQuery.py -
#    A set of examples that query the TripGrid Redis database.
##


import sys
import getopt
import redis
from TripCommon import *

r = redis.Redis("localhost")
g_debugLevel = 3

def log(loglevel, *args):
    if loglevel <= g_debugLevel:
        print args

def SearchLeftEdge(x, y0, y1, type):
    additionalTrips = []
    gridX = latitudeToGridQueue(x)
    gridY0 = longitudeToGridQueue(y0)
    gridY1 = longitudeToGridQueue(y1)
    yRange = range(gridY0, gridY1 + 1)
    log (5, "pruning left edge with x = %d from %d:%d" % (x, y0, y1))
    for gridY in yRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrange(key, 0, -1)
        log(5, "  trips in key %r: %r" % (key, tripIDs))
        for id in tripIDs:
            locations = r.zrange(getTripKey(int(id), "locations"), 0, -1)
            log(5, "  finding locations in trip %r where %d <= %r" % (id, x, locations))
            for l in locations:
                locStr = l.strip("()").split(",")
                log(7, "  comparing %d <= %d" % (x, int(locStr[0])))
                if (x <= int(locStr[0])):
                    log(5, "  found location in left edge %d %d %d <= %r" % (x, y0, y1, l))
                    additionalTrips.append(id)
                    break
    log (5, "additional left edge trips:", additionalTrips)
    return additionalTrips

def SearchRightEdge(x, y0, y1, type):
    additionalTrips = []
    gridX = latitudeToGridQueue(x)
    gridY0 = longitudeToGridQueue(y0)
    gridY1 = longitudeToGridQueue(y1)
    yRange = range(gridY0, gridY1 + 1)
    log (5, "pruning right edge with x = %d from %d:%d" % (x, y0, y1))
    for gridY in yRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrange(key, 0, -1)
        log(5, "  trips in key %r: %r" % (key, tripIDs))
        for id in tripIDs:
            locations = r.zrange(getTripKey(int(id), "locations"), 0, -1)
            log(5, "  finding locations in trip %r where %d >= %r" % (id, x, locations))
            for l in locations:
                locStr = l.strip("()").split(",")
                log(7, "  comparing %d <= %d" % (x, int(locStr[0])))
                if (x >= int(locStr[0])):
                    log(5, "  found location in right edge %d %d %d >= %r" % (x, y0, y1, l))
                    additionalTrips.append(id)
                    break

    log (5, "additional right edge trips:", additionalTrips)
    return additionalTrips

def SearchBottomEdge(y, x0, x1, type):
    additionalTrips = []
    gridY = longitudeToGridQueue(y)
    gridX0 = latitudeToGridQueue(x0)
    gridX1 = latitudeToGridQueue(x1)
    xRange = range(gridX0, gridX1 + 1)
    log (5, "pruning bottom edge with y = %d from %d:%d" % (y, x0, x1))
    for gridX in xRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrange(key, 0, -1)
        log(5, "  trips in key %r: %r" % (key, tripIDs))
        for id in tripIDs:
            locations = r.zrange(getTripKey(int(id), "locations"), 0, -1)
            log(5, "  finding locations in trip %r where %d <= %r" % (id, y, locations))
            for l in locations:
                locStr = l.strip("()").split(",")
                log(7, "  comparing %d <= %d" % (y, int(locStr[1])))
                if (y <= int(locStr[1])):
                    log(5, "  Found location above bottom edge %d %d %d <= %r" % (y, x0, x1, l))
                    additionalTrips.append(id)
                    break
    log (5, "additional bottom edge trips:", additionalTrips)
    return additionalTrips

def SearchTopEdge(y, x0, x1, type):
    additionalTrips = []
    gridY = longitudeToGridQueue(y)
    gridX0 = latitudeToGridQueue(x0)
    gridX1 = latitudeToGridQueue(x1)
    xRange = range(gridX0, gridX1 + 1)
    log (5, "pruning top edge with y = %d from %d:%d" % (y, x0, x1))
    for gridX in xRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrange(key, 0, -1)
        log(5, "  trips in key %r: %r" % (key, tripIDs))
        for id in tripIDs:
            locations = r.zrange(getTripKey(int(id), "locations"), 0, -1)
            log(5, "  finding locations in trip %r where %d >= %r" % (id, y, locations))
            for l in locations:
                locStr = l.strip("()").split(",")
                log(7, "  comparing %d <= %d" % (y, int(locStr[1])))
                if (y >= int(locStr[1])):
                    log(5, "  Found location below top edge %d %d %d <= %r" % (y, x0, x1, l))
                    additionalTrips.append(id)
                    break
    log (5, "additional bottom edge trips:", additionalTrips)
    return additionalTrips

def CheckEdges(bl, tr, type):
    '''
    Check the edges of our bounding box and inspect each tripID queue
    to make sure the trip should be included in our trip count.
    '''
    # If the edges of the bounding box land on our queue edges, then we don't
    # need to inspect each trip queue since the locations should be accurate.
    delta = (g_maxLatitude - g_minLatitude) / g_numLatQueues
    lat_left_edges = range(g_minLatitude, g_maxLatitude, delta)
    lat_right_edges = [x + delta for x in range(g_minLatitude, g_maxLatitude, delta)]
    long_bottom_edges = range(g_minLongitude, g_maxLongitude, delta)
    long_top_edges = [x + delta for x in range(g_minLongitude, g_maxLongitude, delta)]

    additionalTrips = []
    # log(2, "XXX", bl[0], "in", lat_left_edges)
    # if bl[0] not in lat_left_edges:
    additionalTrips += SearchLeftEdge(bl[0], bl[1], tr[1], type)
    # log(2, "XXX", tr[0], "in", lat_right_edges)
    # if tr[0] not in lat_right_edges:
    additionalTrips += SearchRightEdge(tr[0], bl[1], tr[1], type)
    # log(2, "XXX", bl[1], "in", long_bottom_edges)
    # if bl[1] not in long_bottom_edges:
    additionalTrips += SearchBottomEdge(bl[1], bl[0], tr[0], type)
    # log(2, "XXX", tr[1], "in", long_top_edges)
    # if tr[1] not in long_top_edges:
    additionalTrips += SearchTopEdge(tr[1], bl[0], tr[0], type)
    log(5, "additional trips added", additionalTrips)
    return additionalTrips

def TripsInRect(bl, tr):
    '''
    Query how many trips passed through a given geo-rect, as defined by the
    (bottom,left) and (top,right) points.
    '''
    # First we generate the list of grid locations we want to query
    blX = latitudeToGridQueue(bl[0])
    blY = longitudeToGridQueue(bl[1])
    trX = latitudeToGridQueue(tr[0])
    trY = longitudeToGridQueue(tr[1])

    # XXX: we should check the indicies and makes sure we're still legit after
    # the subtraction of the edges.

    # We have to check the edges separately.
    xRange = range(blX + 1, trX)
    yRange = range(blY + 1, trY)
    trips = []
    for x in xRange:
        for y in xRange:
            key = getTripGridKey(x, y, "ALL")
            log(5, "  XXX key %r" % (key,))
            newtrips = r.zrange(key, 0, -1)
            log(5, "  trips added for %r: %r" % (key, newtrips))
            trips += newtrips

    trips += CheckEdges(bl, tr, "ALL")
    trips = [int(x) for x in trips]
    print "Trips: ", len(set(trips)), list(set(sorted(trips)))
    return len(trips)

def TripsStartedInRect(bl, tr):
    '''
    Query how many trips started in a given geo-rect, as defined by the
    (bottom,right) and (top,left) points.
    '''

def TripsEndedInRect(bl, tr):
    '''
    Query how many trips ended in a given geo-rect, as defined by the
    (bottom,right) and (top,left) points.
    '''

def TripsAtTime(t):
    '''
    Query how many trips were occurring at a given point in time.
    '''
    # numTrips += r.zcount(key, "-inf", "inf")

def usage():
    print '  usage: TripQuery.py opts'
    print '       -h: this message'
    print '       -a: query number of trips passing through a given rect.'
    print '       -s: query number of trips starting at a given rect.'
    print '       -e: query number of trips ending at a given rect.'
    print '       -t: query number of trips at a given time.'
    print '  The rect is specified in x,y coordinates between [-90:90], [-180:180] coordinates,'
    print '  and are end-point inclusive.'
    print ''
    print '  examples:'
    print '       TripQuery.py -a --bl 5,5 --tr 7,8'
    print '       TripQuery.py -t --time ???'

def main(argv):
    queryRectAll = False
    queryRectStart = False
    queryRectEnd = False
    queryTime = False
    bl = None
    tr = None
    t = None
    try:
        opts, args = getopt.getopt(argv,"haset",["help","bl=","tr=","time="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-a"):
            queryRectAll = True
        elif opt in ("-s"):
            queryRectStart = True
        elif opt in ("-e"):
            queryRectEnd = True
        elif opt in ("-t"):
            queryTime = True
        elif opt in ("--bl"):
            bl = [int(n) for n in arg.split(',')]
        elif opt in ("--tr"):
            tr = [int(n) for n in arg.split(',')]
        elif opt in ("--time"):
            t = float(arg)

    # Sanity check some combinations
    if queryRectAll or queryRectStart or queryRectEnd:
        if bl is None or tr is None:
            print "Error: options a, s, and e require the bl and tr args."
            sys.exit(2)
        if len(bl) != 2 or len(tr) != 2:
            print "Error: bl or tr coordinates are not correctly specified."
            sys.exit(2)
        if bl[0] >= tr[0]:
            print "Error: x value of bl rect must be less than x value of tr rect."
            sys.exit(2)
        if bl[1] >= tr[1]:
            print "Error: y value of bl rect must be less than y value of tr rect."
            sys.exit(2)
    elif bl is not None or tr is not None:
        print "Warning: rects are specified without a relevant query."

    if queryTime:
        if t is None:
            print "Error: option t requires --time to be specified."
            sys.exit(2)
    elif t is not None:
        print "Warning: time is specified without 't' being specified."

    # XXX: currently I'm leaving this open to compound queries, like
    # if they specify a time and a rect, we should find how many trips
    # went through that rect at that time.
    if queryRectAll:
        TripsInRect(bl, tr)

if __name__ == "__main__":
    main(sys.argv[1:])

