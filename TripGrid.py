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

# We import from tripgrid.LocationGenerator for the globals...though, we should
# probably just move that out to some common defines.
from tripgrid.LocationGenerator import *

# We will split up the virtual space by a grid with each queue representing a
# geographic region.
g_numLatQueues = 10
g_numLongQueues = 10
g_debugLevel = 2
redisServer = redis.Redis("localhost")

def log(loglevel, *args):
    if loglevel <= g_debugLevel:
        print args

def GetQueueX(location):
    '''
    Maps the latitude to the X coordinate of the queue.  For simplicity, we only
    handle integer location units.
    '''
    delta = (g_maxLatitude - g_minLatitude) / g_numLatQueues
    if (g_maxLatitude - g_minLatitude) % g_numLatQueues:
        raise Exception("Error: Deltas between queues are not uniform.")
    queueX = 0
    for x in xrange(g_minLatitude, g_maxLatitude, delta):
        if (x <= location[0] < x + delta):
            return queueX
        queueX += 1

def GetQueueY(location):
    '''
    Maps the longitude to the Y coordinate of the queue.  For simplicity, we only
    handle integer location units.
    '''
    delta = (g_maxLongitude - g_minLongitude) / g_numLongQueues
    if (g_maxLongitude - g_minLongitude) % g_numLongQueues:
        raise Exception("Error: Deltas between queues are not uniform.")
    queueY = 0
    for y in xrange(g_minLongitude, g_maxLongitude, delta):
        if (y <= location[0] < y + delta):
            return queueY
        queueY += 1

def getTripKey(id, type):
    return ("tripQueue:%d" % id) + ":" + type

def getTripGridKey(x, y, type):
    return ("tripGrid:%d:%d" % (x, y)) + ":" + type

@app.task
def startTrip(tripID, location):
    '''
    Record the start of a trip.
    '''
    log(4, "Starting trip %d %r" % (tripID,location))
    now = time.time()
    # add to trip queue
    tripQueueLocKey = getTripKey(tripID, "locations")
    redisServer.zadd(tripQueueLocKey, location, now)
    redisServer.sadd("tripIDs", tripID)

    # add to the gridQueue(s)
    x = GetQueueX(location)
    y = GetQueueY(location)
    gridBeginKey = getTripGridKey(x, y, "BEGIN")
    redisServer.zadd(gridBeginKey, tripID, now)
    gridAllKey = getTripGridKey(x, y, "ALL")
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
    redisServer.zadd(tripQueueLocKey, location, now)

    # add to the gridQueue(s)
    x = GetQueueX(location)
    y = GetQueueY(location)
    gridAllKey = getTripGridKey(x, y, "ALL")
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
    redisServer.zadd(tripQueueLocKey, location, now)
    tripQueueFareKey = getTripKey(tripID, "fare")
#     if redisServer.get(tripQueueFareKey) is not None:
#         raise Exception("ERROR: EndTrip fare already exists for %d." % tripID)
    redisServer.set(tripQueueFareKey, fare)

    # append to the gridQueue
    x = GetQueueX(location)
    y = GetQueueY(location)
    gridEndKey = getTripGridKey(x, y, "END")
    redisServer.zadd(gridEndKey, tripID, now)
    gridFareKey = getTripGridKey(x, y, "END:fare")
    redisServer.incrby(gridFareKey, fare)
    gridAllKey = getTripGridKey(x, y, "ALL")
    redisServer.zadd(gridAllKey, tripID, now)

@app.task
def logTrips():
    for id in redisServer.smembers("tripIDs"):
        locations = redisServer.zrange(getTripKey(int(id), "locations"),
                                       0, -1, withscores=False)
        fare = redisServer.get(getTripKey(int(id), "fare"))
        log(0, "trip %d $%d: %r" % (int(id), int(fare), locations))
