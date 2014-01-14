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

# redis and celery aren't as synchrounous as needed for testing, so we need a reasonable
# sleep to ensure the data we query has enough time to sync.
DATA_SYNC_SLEEP_TIME_SEC = 5
g_debugLevel = 1

def log(loglevel, *args):
    if loglevel <= g_debugLevel:
        print args

def initRedis():
    r = redis.Redis()

    dbsize = r.dbsize()
    itr = 0
    while dbsize != 0:
        r.flushall()
        dbsize = r.dbsize()
        itr += 1
    log(3, "iterations to flush redis: %d", itr)

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
        log(3, "tripID %s: %d %r" % (t, len(locations), locations))

def _testQuadrant(bl, tr, expectedTrips, quadStr):
    results = {
        'trips': [],
        'fare': 0,
        }

    QueryRect(bl, tr, "ALL", "-inf", "inf", False, results)
    log(5, "testQuadrant %s reported %d trips ?= %d: %r" \
            % (quadStr, len(results['trips']), expectedTrips, results['trips']))

    assert(len(results['trips']) == expectedTrips), \
        "Error, testQuadrant %s reported %d trips != %d: %r" \
        % (quadStr, len(results['trips']), expectedTrips, results['trips'])
    log(1, "testQuadrant %s pass" % (quadStr,))

def testAllQuadrants():
    r = initRedis()

    numTrips = 10
    trips = []
    startAllTrips(numTrips, trips)
    waitForAllTrips(trips)

    expectedTrips = numTrips
    _testQuadrant((-90, -180), (90, 180), expectedTrips, "all quads")

    return True

def testQuadrant(quadrant):
    r = initRedis()

    numTrips = 10
    trips = []
    log(1, "Testing Quadrant", quadrant)
    startAllTrips(numTrips, trips, quadrant)
    waitForAllTrips(trips)

    expectedTrips = 0
    if quadrant == TOP_LEFT:
        expectedTrips = numTrips
    if quadrant != LEFT_EDGE and quadrant != TOP_EDGE:
        _testQuadrant((-90, 0), (0, 180), expectedTrips, "top left")

    expectedTrips = 0
    if quadrant == TOP_RIGHT:
        expectedTrips = numTrips
    if quadrant != RIGHT_EDGE and quadrant != TOP_EDGE:
        _testQuadrant((0, 0), (90, 180), expectedTrips, "top right")

    expectedTrips = 0
    if quadrant == BOTTOM_RIGHT:
        expectedTrips = numTrips
    if quadrant != RIGHT_EDGE and quadrant != BOTTOM_EDGE:
        _testQuadrant((0, -180), (90, 0), expectedTrips, "bottom right")

    expectedTrips = 0
    if quadrant == BOTTOM_LEFT:
        expectedTrips = numTrips
    if quadrant != LEFT_EDGE and quadrant != BOTTOM_EDGE:
        _testQuadrant((-90, -180), (0, 0), expectedTrips, "bottom left")

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
    trip.join()

    waitForAllTrips(trips)

    results = {
        'trips': [],
        'fare': 0,
        }

    QueryRect((-90, -180), (90, 180), "ALL", "-inf", "inf", False, results)

    assert(len(results['trips']) == 1), \
         "Error, testOneTrip reported %d trips != %d: %r" \
         % (len(results['trips']), 1, results['trips'])
    log(1, "testOneTrip pass")

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
