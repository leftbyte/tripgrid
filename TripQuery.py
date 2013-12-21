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

# XXX TODOs
# XXX One overlapping inefficiency on the corners of the search.  They're always
# checked twice.
# XXX The only difference between the 4 edge search functions are the indices
# (which can be generated further, and the comparison.  We should be able to
# refactor, but it's nice having the debug logging right now...
#
# XXX: How would I define a "struct" to pass around instead of
# all these params (bl, tr, type, t0, t1, withFare?
#
# XXX: should try passing in 'operator'
def SearchLeftEdge(x, y0, y1, type, t0, t1, withFare, results):
    gridX = latitudeToGridQueue(x)
    gridY0 = longitudeToGridQueue(y0)
    gridY1 = longitudeToGridQueue(y1)
    yRange = range(gridY0, gridY1 + 1)
    log (5, "pruning left edge with x = %d from %d:%d" % (x, y0, y1))
    for gridY in yRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrangebyscore(key, t0, t1)
        log(5, "  trips in key %r: %r" % (key, tripIDs))
        for id in tripIDs:
            locations = r.zrangebyscore(getTripKey(int(id), "locations"), t0, t1)
            log(5, "  finding locations in trip %r where x: %d <= %r" % (id, x, locations))

            for l in locations:
                locStr = l.strip("()").split(",")
                locInt = [int(_x) for _x in locStr]
                log(7, "  comparing %d <= %d" % (locInt[0], x))
                if (x <= locInt[0]):
                    log(5, "  found location in left edge %d <= %d and %d <= %d <= %d" %
                        (x, locInt[0], y0, locInt[1], y1))
                    results['trips'].append(id)
                    if withFare:
                        farekey = getTripKey(int(id), "fare")
                        results['fare'] += int(r.get(farekey))
                    break

def SearchRightEdge(x, y0, y1, type, t0, t1, withFare, results):
    gridX = latitudeToGridQueue(x)
    gridY0 = longitudeToGridQueue(y0)
    gridY1 = longitudeToGridQueue(y1)
    yRange = range(gridY0, gridY1 + 1)
    log (5, "pruning right edge with x = %d from %d:%d" % (x, y0, y1))
    for gridY in yRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrangebyscore(key, t0, t1)
        log(5, "  trips in key %r: %r" % (key, tripIDs))
        for id in tripIDs:
            locations = r.zrangebyscore(getTripKey(int(id), "locations"), t0, t1)
            log(5, "  finding locations in trip %r where x: %d >= %r" % (id, x, locations))
            for l in locations:
                locStr = l.strip("()").split(",")
                locInt = [int(_x) for _x in locStr]
                log(7, "  comparing %d <= %d" % (locInt[0], x))
                if (locInt[0] <= x):
                    log(5, "  found location in right edge %d <= %d and %d <= %d <= %d" %
                        (locInt[0], x, y0, locInt[1], y1))
                    results['trips'].append(id)
                    if withFare:
                        farekey = getTripKey(int(id), "fare")
                        results['fare'] += int(r.get(farekey))
                    break

def SearchBottomEdge(y, x0, x1, type, t0, t1, withFare, results):
    gridY = longitudeToGridQueue(y)
    gridX0 = latitudeToGridQueue(x0)
    gridX1 = latitudeToGridQueue(x1)
    xRange = range(gridX0, gridX1 + 1)
    log (5, "pruning bottom edge with y = %d from %d:%d" % (y, x0, x1))
    for gridX in xRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrangebyscore(key, t0, t1)
        log(5, "  trips in key %r: %r" % (key, tripIDs))
        for id in tripIDs:
            locations = r.zrangebyscore(getTripKey(int(id), "locations"), t0, t1)
            log(5, "  finding locations in trip %r where: y: %d <= %r" % (id, y, locations))
            for l in locations:
                locStr = l.strip("()").split(",")
                locInt = [int(_y) for _y in locStr]
                log(7, "  comparing %d <= %d" % (y, locInt[1]))
                if (y <= locInt[1]):
                    log(5, "  found location above bottom edge %d <= %d and %d <= %d <= %d" %
                        (y, locInt[1], x0, locInt[0], x1))
                    results['trips'].append(id)
                    if withFare:
                        farekey = getTripKey(int(id), "fare")
                        results['fare'] += int(r.get(farekey))
                    break

def SearchTopEdge(y, x0, x1, type, t0, t1, withFare, results):
    gridY = longitudeToGridQueue(y)
    gridX0 = latitudeToGridQueue(x0)
    gridX1 = latitudeToGridQueue(x1)
    xRange = range(gridX0, gridX1 + 1)
    log (5, "pruning top edge with y = %d from %d:%d" % (y, x0, x1))
    for gridX in xRange:
        key = getTripGridKey(gridX, gridY, type)
        tripIDs = r.zrangebyscore(key, t0, t1)
        log(5, "  trips in key %r: %r" % (key, tripIDs))
        for id in tripIDs:
            locations = r.zrangebyscore(getTripKey(int(id), "locations"), t0, t1)
            log(5, "  finding locations in trip %r where y: %d >= %r" % (id, y, locations))
            for l in locations:
                locStr = l.strip("()").split(",")
                locInt = [int(_y) for _y in locStr]
                log(7, "  comparing %d <= %d" % (locInt[1], y))
                if (locInt[1] <= y):
                    log(5, "  found location below top edge %d <= %d and %d <= %d <= %d" %
                        (locInt[1], y, x0, locInt[0], x1))
                    results['trips'].append(id)
                    if withFare:
                        farekey = getTripKey(int(id), "fare")
                        results['fare'] += int(r.get(farekey))
                    break

def CheckEdges(bl, tr, type, t0, t1, withFare, results):
    '''
    Check the edges of our bounding box and inspect each tripID queue
    to make sure the trip should be included in our trip count.
    '''
    SearchLeftEdge(bl[0], bl[1], tr[1], type, t0, t1, withFare, results)
    SearchRightEdge(tr[0], bl[1], tr[1], type, t0, t1, withFare, results)
    SearchBottomEdge(bl[1], bl[0], tr[0], type, t0, t1, withFare, results)
    SearchTopEdge(tr[1], bl[0], tr[0], type, t0, t1, withFare, results)

def QueryRect(bl, tr, type, t0, t1, withFare, results):
    '''
    Query the trip database for the number of trips taken and the fare given a
    rect and time slice.
    '''
    # First we generate the list of grid locations we want to query
    blX = latitudeToGridQueue(bl[0])
    blY = longitudeToGridQueue(bl[1])
    trX = latitudeToGridQueue(tr[0])
    trY = longitudeToGridQueue(tr[1])

    # We have to check the edges separately.
    # XXX this is an edge case we need to handle
    if blX == trX or blY == trY:
        raise Exception("Error: Bad grid queue indices.")
    xRange = range(blX + 1, trX)
    yRange = range(blY + 1, trY)
    for x in xRange:
        for y in xRange:
            key = getTripGridKey(x, y, type)
            newtrips = r.zrangebyscore(key, t0, t1)
            log(5, "  trips added for %r: %r" % (key, newtrips))
            if len(newtrips) == 0:
                continue
            results['trips'] += newtrips
            if withFare:
                farekey = getTripGridKey(x, y, "END:fare")
                fare = r.get(farekey)
                if fare is not None:
                    results['fare'] += int(fare)

    CheckEdges(bl, tr, type, t0, t1, withFare, results)

    # We only want the number of unique trips.
    trips = results['trips']
    trips = [int(x) for x in trips]
    uniqueTrips = list(set(sorted(trips)))
    results['trips'] = uniqueTrips

    if withFare:
        print "Trips: ", len(uniqueTrips), "$%d" % int(results['fare']), uniqueTrips
    else:
        print "Trips: ", len(uniqueTrips), uniqueTrips

def TripsGoingThroughRect(bl, tr, results):
    '''
    Query how many trips passed through a given geo-rect, as defined by the
    (bottom,left) and (top,right) points and returns it in the passed in
    dictionary.
    '''
    return QueryRect(bl, tr, "ALL", "-inf", "inf", False, results)

def TripsStartedInRect(bl, tr, results):
    '''
    Query how many trips started in a given geo-rect, as defined by the
    (bottom,right) and (top,left) points.
    '''
    return QueryRect(bl, tr, "BEGIN", "-inf", "inf", False, results)

def TripsEndedInRect(bl, tr, results):
    '''
    Query how many trips ended in a given geo-rect, as defined by the
    (bottom,right) and (top,left) points.
    '''
    return QueryRect(bl, tr, "END", "-inf", "inf", True, results)

def TripsAtTime(t0, t1, results):
    '''
    Query how many trips were occurring at a given point in time.
    '''
    return QueryRect((-90, -180), (79, 179), "ALL", t0, t1, True, results)

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
    t0 = None
    t1 = None
    try:
        opts, args = getopt.getopt(argv,"haset",["help","bl=","tr=","t0=", "t1="])
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
        elif opt in ("--t0"):
            t0 = float(arg)
        elif opt in ("--t1"):
            t1 = float(arg)

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
        if t0 is None or t1 is None:
            print "Error: option t requires t0 and t1 to be specified."
            sys.exit(2)
    elif t0 is not None or t1 is not None:
        print "Warning: t0 or t1 is specified without 't' being specified."

    # XXX: currently I'm leaving this open to compound queries, like
    # if they specify a time and a rect, we should find how many trips
    # went through that rect at that time.

    resultsDict = {}
    resultsDict['trips'] = []
    resultsDict['fare'] = 0
    if queryRectAll:
        TripsGoingThroughRect(bl, tr, resultsDict)

    if queryRectStart:
        TripsStartedInRect(bl, tr, resultsDict)

    if queryRectEnd:
        TripsEndedInRect(bl, tr, resultsDict)

    if queryTime:
        TripsAtTime(t0, t1, resultsDict)

if __name__ == "__main__":
    main(sys.argv[1:])
