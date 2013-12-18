#!/usr/bin/env python

##
# TripGrid.py -
#
#    Module to store the trip data as a grid on top of Redis.
#
#    Types of data stored:
#    Full trip locations
#    - tripIDs = set(of valid IDs), so that we don't have to use 'keys()'
#    - tripID:ID:locations = ordered set(location, time)
#    - tripID:ID:fare      = fare
#
#    Grid to optimize queries
#    - grid:X:Y:ALL:locations   = ordered set(location, time)
#    - grid:X:Y:BEGIN:locations = ordered set(location, time)
#    - grid:X:Y:END:locations   = ordered set(location, time)
#    - grid:X:Y:END:fare        = fare
##

from __future__ import absolute_import
import os
import sys
import time
import redis
from tripgrid.celery import app
from tripgrid.TripCommon import *

# We will split up the virtual space by a grid with each queue representing a
# geographic region.
g_debugLevel = 5
redisServer = redis.Redis("localhost")

def log(loglevel, *args):
    if loglevel <= g_debugLevel:
        print args

@app.task
def startTrip(tripID, location):
    '''
    Record the start of a trip.
    '''
    log(4, "Starting trip %d %r" % (tripID,location))
    now = time.time()
    # add to trip queue
    tripQueueLocKey = getTripKey(tripID, "locations")
    log(5, "ZADD %r %r %r" % (tripQueueLocKey, now, location))
    redisServer.zadd(tripQueueLocKey, location, now)
    log(5, "SADD %r %r" % ("tripIDs", tripID))
    redisServer.sadd("tripIDs", tripID)

    # add to the gridQueue(s)
    x = latitudeToGridQueue(location[0])
    y = longitudeToGridQueue(location[1])
    gridBeginKey = getTripGridKey(x, y, "BEGIN")
    log(5, "ZADD %r %r %r" % (gridBeginKey, now, tripID))
    redisServer.zadd(gridBeginKey, tripID, now)
    gridAllKey = getTripGridKey(x, y, "ALL")
    log(5, "ZADD %r %r %r" % (gridAllKey, now, tripID))
    redisServer.zadd(gridAllKey, tripID, now)

@app.task
def updateTrip(tripID, location):
    '''
    Record an update to a trip.
    '''
    log(4, "Updating trip %d %r" % (tripID,location))
    now = time.time()
    # add to trip queue
    tripQueueLocKey = getTripKey(tripID, "locations")
    log(5, "ZADD %r %r %r" % (tripQueueLocKey, now, location))
    redisServer.zadd(tripQueueLocKey, location, now)

    # add to the gridQueue(s)
    x = latitudeToGridQueue(location[0])
    y = longitudeToGridQueue(location[1])
    gridAllKey = getTripGridKey(x, y, "ALL")
    log(5, "ZADD %r %r %r" % (gridAllKey, now, tripID))
    redisServer.zadd(gridAllKey, tripID, now)

@app.task
def endTrip(tripID, fare, location):
    '''
    Record an end to a trip.
    '''
    log(4, "Ending trip %d $%d %r" % (tripID, fare, location))
    now = time.time()
    # add to trip queue
    tripQueueLocKey = getTripKey(tripID, "locations")
    log(5, "ZADD %r %r %r" % (tripQueueLocKey, now, location))
    redisServer.zadd(tripQueueLocKey, location, now)
    tripQueueFareKey = getTripKey(tripID, "fare")
#     if redisServer.get(tripQueueFareKey) is not None:
#         raise Exception("ERROR: EndTrip fare already exists for %d." % tripID)
    log(5, "SET %r %r" % (tripQueueFareKey, fare))
    redisServer.set(tripQueueFareKey, fare)

    # append to the gridQueue
    x = latitudeToGridQueue(location[0])
    y = longitudeToGridQueue(location[1])
    gridEndKey = getTripGridKey(x, y, "END")
    log(5, "ZADD %r %r %r" % (gridEndKey, now, tripID))
    redisServer.zadd(gridEndKey, tripID, now)
    gridFareKey = getTripGridKey(x, y, "END:fare")
    log(5, "INCRBY %r %r" % (gridFareKey, fare))
    redisServer.incrby(gridFareKey, fare)
    gridAllKey = getTripGridKey(x, y, "ALL")
    log(5, "ZADD %r %r %r" % (gridAllKey, now, tripID))
    redisServer.zadd(gridAllKey, tripID, now)

@app.task
def logTrips():
    for id in redisServer.smembers("tripIDs"):
        locations = redisServer.zrange(getTripKey(int(id), "locations"),
                                       0, -1, withscores=False)
        fare = redisServer.get(getTripKey(int(id), "fare"))
        log(0, "trip %d $%d: %r" % (int(id), int(fare), locations))
