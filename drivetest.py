#!/usr/bin/env python

##
# simulator.py -
#    Module for driving the Trip generator.
##

from __future__ import absolute_import
import sys
import time
import redis
from tripgrid.Trip import *
from tripgrid.TripQuery import *

# XXX What is the pythonic way to have a header file?  E.g. To have an API of
# what the results dict is composed of?

# redis and celery aren't as synchrounous as needed for testing, so we need a reasonable
# sleep to ensure the data we query has enough time to sync.
DATA_SYNC_SLEEP_TIME_SEC = 3

def initRedis():
    r = redis.Redis()
    r.flushall()
    time.sleep(DATA_SYNC_SLEEP_TIME_SEC)

    # r.save()
    keys = r.keys("*")
    # dbsize = r.dbsize()
    # assert(dbsize == 0), "dbsize %d not equal to zero." % dbsize
    assert(len(keys) == 0), "dbsize %d not equal to zero. %r" % (len(keys), keys)

    return r

def startAllTrips(numTrips, trips, testQuadrant=None):
    for x in range(0, numTrips):
        trip = Trip(0.1, testQuadrant)
        trip.start()
        trips.append(trip)

def waitForAllTrips(trips):
    for trip in trips:
        isAlive = True
        while isAlive:
            trip.join()
            isAlive = trip.isAlive()
    time.sleep(DATA_SYNC_SLEEP_TIME_SEC)

def printAllTrips(r):
    trips = r.smembers("tripIDs")
    for t in trips:
        locations = r.zrange("tripQueue:%s:locations" % t, 0, -1)
        print "XXX tripID %s: %d %r" % (t, len(locations), locations)

def _testQuadrant(bl, tr, expectedTrips, quadStr):
    results = {}
    results['trips'] = []
    results['fare'] = 0
    # printAllTrips(r)

    QueryRect(bl, tr, "ALL", "-inf", "inf", False, results)

    assert(len(results['trips']) == expectedTrips), \
        "Error, testQuadrant %s reported %d trips != %d: %r" \
        % (quadStr, len(results['trips']), expectedTrips, results['trips'])
    print "testQuadrant %s pass" % (quadStr,)

def testAllQuadrants():
    r = initRedis()

    numTrips = 10
    trips = []
    startAllTrips(numTrips, trips)

    # XXX Bug where the trips don't seem to be blocking on join() correctly...maybe
    # because the thread hasn't really started?
    now = time.time()
    waitForAllTrips(trips)
#    print "XXX waited %f seconds" % (time.time() - now)

    expectedTrips = numTrips
    _testQuadrant((-90, -180), (90, 180), expectedTrips, "all quads")

#    print "XXX testAllQuadrants pass: %r" % results['trips']
    return True

def testQuadrant(quadrant):
    r = initRedis()

    numTrips = 10
    trips = []
    print "Testing Quadrant", quadrant
    startAllTrips(numTrips, trips, quadrant)
    waitForAllTrips(trips)

    expectedTrips = 0
    if quadrant == TOP_LEFT:
        expectedTrips = numTrips
    if quadrant != LEFT_EDGE and quadrant != TOP_EDGE:
        _testQuadrant((-90, 0), (0, 180), expectedTrips, "top left")

    # print "XXX, testQuadrant top-left reported %d trips != %d: %r" \
    #     % (len(results['trips']), numTrips, results['trips'])

    expectedTrips = 0
    if quadrant == TOP_RIGHT:
        expectedTrips = numTrips
    if quadrant != RIGHT_EDGE and quadrant != TOP_EDGE:
        _testQuadrant((0, 0), (90, 180), expectedTrips, "top right")

    # print "XXX, testQuadrant top-right reported %d trips != %d: %r" \
    #     % (len(results['trips']), numTrips, results['trips'])

    expectedTrips = 0
    if quadrant == BOTTOM_RIGHT:
        expectedTrips = numTrips
    if quadrant != RIGHT_EDGE and quadrant != BOTTOM_EDGE:
        _testQuadrant((0, -180), (90, 0), expectedTrips, "bottom right")

    # print "XXX, testQuadrant bottom-right reported %d trips != %d: %r" \
    #     % (len(results['trips']), numTrips, results['trips'])

    expectedTrips = 0
    if quadrant == BOTTOM_LEFT:
        expectedTrips = numTrips
    if quadrant != LEFT_EDGE and quadrant != BOTTOM_EDGE:
        _testQuadrant((-90, -180), (0, 0), expectedTrips, "bottom left")

    # print "XXX, testQuadrant bottom-left reported %d trips != %d: %r" \
    #     % (len(results['trips']), numTrips, results['trips'])

    expectedTrips = 0
    if quadrant == LEFT_EDGE:
        expectedTrips = numTrips
    _testQuadrant((-90, -180), (-90, 180), expectedTrips, "left edge")

    expectedTrips = 0
    if quadrant == TOP_EDGE:
        expectedTrips = numTrips
    _testQuadrant((-90, 180), (90, 180), expectedTrips, "top edge")

    expectedTrips = 0
    if quadrant == RIGHT_EDGE:
        expectedTrips = numTrips
    _testQuadrant((90, -180), (90, 180), expectedTrips, "right edge")

    expectedTrips = 0
    if quadrant == BOTTOM_EDGE:
        expectedTrips = numTrips
    _testQuadrant((-90, -180), (90, -180), expectedTrips, "bottom edge")

    return True

def testOneTrip():
    r = initRedis()

    trips = []
    trip = Trip(0.1)
    trips.append(trip)
    trip.start()
    now = time.time()
    trip.join()
    waitForAllTrips(trips)
#    print "XXX waited %f seconds" % (time.time() - now)

    results = {}
    results['trips'] = []
    results['fare'] = 0

    # r.save()
    QueryRect((-90, -180), (90, 180), "ALL", "-inf", "inf", False, results)

    # Let's see what locations these trips have
    trips = r.smembers("tripIDs")
    for t in trips:
        locations = r.zrange("tripQueue:%s:locations" % t, 0, -1)
#        print "XXX tripID %s: %d %r" % (t, len(locations), locations)

    # XXX REMOVE ME
    # if len(results['trips']) != 1:
    #     print "Error, testOneTrip reported %d trips != %d: %r" \
    #         % (len(results['trips']), 1, results['trips'])
    #     sys.exit()
    assert(len(results['trips']) == 1), \
         "Error, testOneTrip reported %d trips != %d: %r" \
         % (len(results['trips']), 1, results['trips'])
    print "testOneTrip pass"
    return True

def main():
    testOneTrip()
    testAllQuadrants()
    testQuadrant(TOP_LEFT)
    testQuadrant(TOP_RIGHT)
    testQuadrant(BOTTOM_RIGHT)
    testQuadrant(BOTTOM_LEFT)
    testQuadrant(LEFT_EDGE)
    testQuadrant(TOP_EDGE)
    testQuadrant(RIGHT_EDGE)
    testQuadrant(BOTTOM_EDGE)

if __name__ == "__main__":
    main()
