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
g_debugLevel = 5

def log(loglevel, *args):
    if loglevel <= g_debugLevel:
        print args

def PruneLeftEdge(x, y0, y1, type):
    numPrunes = 0
    gridX = LatitudeToGridQueue(x)
    gridY0 = LongitudeToGridQueue(y0)
    gridY1 = LongitudeToGridQueue(y1)
    yRange = range(gridY0, gridY1)
    for gridY in yRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrange(key, 0, -1)
        for id in tripIDs:
            locations = r.zrange(getTripKey(int(id), "locations"), 0, -1)
            pruneThisTrip = True
            for l in locations:
                if (l[0] >= x):
                    log(5, "Found location in left edge %r >= %d %d %d" % (l, x, y0, y1))
                    pruneThisTrip = False
                    break
            if pruneThisTrip:
                numPrunes += 1
    log (5, "pruning left edge:", numPrunes)
    return numPrunes

def PruneRightEdge(x, y0, y1, type):
    numPrunes = 0
    gridX = LatitudeToGridQueue(x)
    gridY0 = LongitudeToGridQueue(y0)
    gridY1 = LongitudeToGridQueue(y1)
    yRange = range(gridY0, gridY1)
    for gridY in yRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrange(key, 0, -1)
        for id in tripIDs:
            locations = r.zrange(getTripKey(int(id), "locations"), 0, -1)
            pruneThisTrip = True
            for l in locations:
                if (l[0] <= x):
                    log(5, "Found location in right edge %r <= %d %d %d" % (l, x, y0, y1))
                    pruneThisTrip = False
                    break
            if pruneThisTrip:
                numPrunes += 1
    log (5, "pruning right edge:", numPrunes)
    return numPrunes

def PruneBottomEdge(y, x0, x1, type):
    numPrunes = 0
    gridY = LongitudeToGridQueue(y)
    gridX0 = LatitudeToGridQueue(x0)
    gridX1 = LatitudeToGridQueue(x1)
    xRange = range(gridX0, gridX1 + 1)
    for gridX in xRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrange(key, 0, -1)
        for id in tripIDs:
            locations = r.zrange(getTripKey(int(id), "locations"), 0, -1)
            pruneThisTrip = True
            for l in locations:
                if (l[0] >= y):
                    log(5, "Found location above bottom %r >= %d %d %d" % (l, x, y0, y1))
                    pruneThisTrip = False
                    break
            if pruneThisTrip:
                numPrunes += 1
    log (5, "pruning bottom edge:", numPrunes)
    return numPrunes

def PruneTopEdge(y, x0, x1, type):
    numPrunes = 0
    gridY = LongitudeToGridQueue(y)
    gridX0 = LatitudeToGridQueue(x0)
    gridX1 = LatitudeToGridQueue(x1)
    xRange = range(gridX0, gridX1 + 1)
    for gridX in xRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrange(key, 0, -1)
        for id in tripIDs:
            locations = r.zrange(getTripKey(int(id), "locations"), 0, -1)
            pruneThisTrip = True
            for l in locations:
                if (l[0] <= y):
                    log(5, "Found location below top %r >= %d %d %d" % (l, x, y0, y1))
                    pruneThisTrip = False
                    break
            if pruneThisTrip:
                numPrunes += 1
    log (5, "pruning top edge:", numPrunes)
    return numPrunes

def CheckEdges(numTrips, bl, tr, type):
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

    if bl[0] not in lat_left_edges:
        numTrips -= PruneLeftEdge(bl[0], bl[1], tr[1], type)
    if tr[0] not in lat_right_edges:
        numTrips -= PruneRightEdge(tr[0], bl[1], tr[1], type)
    if bl[1] not in long_bottom_edges:
        numTrips -= PruneBottomEdge(bl[1], bl[0], tr[0], type)
    if tr[1] not in long_top_edges:
        numTrips -= PruneTopEdge(tr[1], bl[0], tr[0], type)
    return numTrips

def TripsInRect(bl, tr):
    '''
    Query how many trips passed through a given geo-rect, as defined by the
    (bottom,left) and (top,right) points.
    '''
    # First we generate the list of grid locations we want to query
    blX = LatitudeToGridQueue(bl[0])
    blY = LongitudeToGridQueue(bl[1])
    trX = LatitudeToGridQueue(tr[0])
    trY = LongitudeToGridQueue(tr[1])

    xRange = range(blX, trX)
    yRange = range(blY, trY)
    numTrips = 0
    for x in xRange:
        for y in xRange:
            key = getTripGridKey(x, y, "ALL")
            numTrips += r.zcard(key)

    numTrips = CheckEdges(numTrips, bl, tr, "ALL")

    print "Trips: ", numTrips
    return numTrips

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

