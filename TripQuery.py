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
g_debugLevel = 1

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
    log (5, "searching left edge with x = %d from %d:%d" % (x, y0, y1))
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
    log (5, "searching right edge with x = %d from %d:%d" % (x, y0, y1))
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
    log (5, "searching bottom edge with y = %d from %d:%d" % (y, x0, x1))
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
    log (5, "searching top edge with y = %d from %d:%d" % (y, x0, x1))
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

    # for i in range(-90, 90, 18):
    #     g = latitudeToGridQueue(i)
    #     print "lat loc %d -> grid %d" %(i, g)

    # for i in range(-180, 180, 36):
    #     g = longitudeToGridQueue(i)
    #     print "long loc %d -> grid %d" %(i, g)

    # print "XXX bl[0] %d -> blX %d, bl[1] %d -> blY %d" % (bl[0], blX, bl[1], blY)
    # print "XXX tr[0] %d -> trX %d, tr[1] %d -> trY %d" % (tr[0], trX, tr[1], trY)
    # print "checking from X queues %d:%d Y queues %d:%d" %(blX, trX, blY, trY)

    # We have to check the edges separately.
    # XXX this is an edge case we need to handle
# XXX I don't think this assert even makes sense...
#    if blX == trX or blY == trY:
#        print "bl[0] %d -> blX %d, bl[1] %d -> blY %d" % (bl[0], blX, bl[1], blY)
#        print "tr[0] %d -> trX %d, tr[1] %d -> trY %d" % (tr[0], trX, tr[1], trY)
#        raise Exception("Error: Bad grid queue indices.")
# XXX, can't remember why we had range(blX + 1, trX) before...
    xRange = range(blX, trX + 1)
    yRange = range(blY, trY + 1)
    for x in xRange:
        for y in yRange:
            key = getTripGridKey(x, y, type)
            newtrips = r.zrangebyscore(key, t0, t1)
            # log(1, "  XXX trips added for %r: %r" % (key, newtrips))
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
        log(3, "Trips: ", len(uniqueTrips), "$%d" % int(results['fare']), uniqueTrips)
    else:
        log(3, "Trips: ", len(uniqueTrips), uniqueTrips)

def TripsGoingThroughRect(bl, tr, t0, t1, withFare, results):
    '''
    Query how many trips passed through a given geo-rect, as defined by the
    (bottom,left) and (top,right) points and returns it in the passed in
    dictionary.
    '''
    return QueryRect(bl, tr, "ALL", t0, t1, withFare, results)

def TripsStartedInRect(bl, tr, t0, t1, withFare, results):
    '''
    Query how many trips started in a given geo-rect, as defined by the
    (bottom,right) and (top,left) points.
    '''
    return QueryRect(bl, tr, "BEGIN", t0, t1, withFare, results)

def TripsEndedInRect(bl, tr, t0, t1, withFare, results):
    '''
    Query how many trips ended in a given geo-rect, as defined by the
    (bottom,right) and (top,left) points.
    '''
    return QueryRect(bl, tr, "END", t0, t1, withFare, results)

def TripsAtTime(t0, t1, withFare, results):
    '''
    Query how many trips were occurring at a given point in time.
    '''
    return QueryRect((-90, -180), (79, 179), "ALL", t0, t1, withFare, results)

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
    t0 = "-inf"
    t1 = "inf"
    withFare = False

    try:
        opts, args = getopt.getopt(argv,"hasetf",["help","bl=","tr=","t0=", "t1="])
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
        elif opt in ("-f"):
            withFare = True
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

    if queryRectAll:
        TripsGoingThroughRect(bl, tr, t0, t1, withFare, resultsDict)

    if queryRectStart:
        TripsStartedInRect(bl, tr, t0, t1, withFare, resultsDict)

    if queryRectEnd:
        TripsEndedInRect(bl, tr, t0, t1, True, resultsDict)

    if queryTime:
        TripsAtTime(t0, t1, withFare, resultsDict)

if __name__ == "__main__":
    main(sys.argv[1:])
