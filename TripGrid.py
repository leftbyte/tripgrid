#!/usr/bin/env python

##
# TripGrid.py -
#
#    Module to store the trip data as a grid.
##

from __future__ import absolute_import
import os
import sys
import time
from tripgrid.celery import app
from celery.contrib.methods import task_method

# We import from tripgrid.LocationGenerator for the globals...though, we should
# probably just move that out to some common defines.
from tripgrid.LocationGenerator import *

g_debugLevel = 2

# We will split up the virtual space by a grid with each queue representing a
# geographic region.
g_numLatQueues = 10
g_numLongQueues = 10

# Each Lat/Long queue will contain a set of queues to optimize data queries
# 0: ALL
# 1: BEGIN
# 2: END
g_numQueueTypes = 3
QUEUE_ALL    = 0
QUEUE_BEGIN  = 1
QUEUE_END    = 2

class TripGrid():
    '''TripGrid -
    A collection of queues to support tracking and querying of Trip data across
    a virtual grid.
    '''
    def __init__(self):
        self.debugLevel = g_debugLevel

        print "Create TripGrid", self

        # XXX Keep the grid queues local for now, check performance, and we can
        # spawn one task worker per queue if performance is too bad.
        #
        # XXX another way to do this would be to have each task be a grid queue and have
        # each message be: Message Type:(Message Data)
        #
        # The gridQueue will be indexed by: tripQueue[Lat][Long][Queue type].
        self.gridQueue = [[[[] for t in xrange(g_numQueueTypes)]      \
                               for y in xrange(g_numLongQueues)]      \
                               for x in xrange(g_numLatQueues)]
        # The trip queue maps an index to its fare and all its locations.
        self.tripQueue = {}

    def log(self, loglevel, *args):
        if loglevel <= self.debugLevel:
            print args

    def GetQueueX(self, location):
        '''Maps the latitude to the X coordinate of the queue.  For simplicity, we only
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

    def GetQueueY(self, location):
        '''Maps the longitude to the Y coordinate of the queue.  For simplicity, we only
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

    def initTrip(self, tripID):
        self.tripQueue[tripID] = {'fare': 0, 'locations': []}
        print "Added trip ID %d to tripQueue: %r" % (tripID,self.tripQueue.keys())

    @app.task(filter=task_method)
    def startTrip(self, tripID, location):
        if tripID not in self.tripQueue:
            self.initTrip(tripID)
        self.tripQueue[tripID]['locations'].append(location)

        # append to the gridQueue
        x = self.GetQueueX(location)
        y = self.GetQueueY(location)
        self.gridQueue[x][y][QUEUE_BEGIN].append(tripID)
        self.gridQueue[x][y][QUEUE_ALL].append(tripID)

    @app.task(filter=task_method)
    def updateTrip(self, tripID, location):
        if tripID not in self.tripQueue:
            self.initTrip(tripID)
        self.tripQueue[tripID]['locations'].append(location)

        # append to the gridQueue
        x = self.GetQueueX(location)
        y = self.GetQueueY(location)
        self.gridQueue[x][y][QUEUE_ALL].append(tripID)

    @app.task(filter=task_method)
    def endTrip(self, tripID, fare, location):
        if tripID not in self.tripQueue:
            self.initTrip(tripID)
        self.tripQueue[tripID]['locations'].append(location)
        self.tripQueue[tripID]['fare'] = fare

        # append to the gridQueue
        x = self.GetQueueX(location)
        y = self.GetQueueY(location)
        # swap out the default list to a dict to hold a fare sum in the location
        if (len(self.gridQueue[x][y][QUEUE_END]) == 0):
            self.gridQueue[x][y][QUEUE_END] = {'fare':0, 'locations':[]}

        self.gridQueue[x][y][QUEUE_ALL].append(tripID)
        self.gridQueue[x][y][QUEUE_END]['locations'].append(tripID)
        self.gridQueue[x][y][QUEUE_END]['fare'] += fare

    @app.task(filter=task_method)
    def logTrips(self):
        print "Logging trips:"
        for k in self.tripQueue.keys():
            print "tripID %d $%d: %r" % (k,
                                         self.tripQueue[k]['fare'],
                                         self.tripQueue[k]['locations'])
