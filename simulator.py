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

g_resetDB = FALSE
g_debugLevel = 1

def initRedis():
    r = redis.Redis()

    dbsize = r.dbsize()
    itr = 0
    while dbsize != 0:
        r.flushall()
        dbsize = r.dbsize()
        itr += 1
    # print "iterations to flush redis: %d", itr

def log(loglevel, *args):
    if loglevel <= g_debugLevel:
        print args

def main():
    if g_resetDB:
        initRedis()

    numTrips = 500
    trips = []
    while True:
        log(3, "adding %d trips" % (numTrips))
        for x in range(0, numTrips):
            trip = Trip()
            trip.start()
            trips.append(trip)

        log(1, "trips running: ", len(trips))

        # wait for the first trip to end to give the trips
        # some time to run.
        now = time.time()
        trips[0].join()
        log(5, "first trip ended in %d seconds" % (time.time() - now))

        numTrips = 0
        for trip in trips:
            if trip.ended():
                numTrips += 1
                trips.remove(trip)

if __name__ == "__main__":
    main()
