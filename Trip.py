#!/usr/bin/env python

##
# Trip.py -
#    A trip generator.  Not as good as other drugs.
##

import os
import sys
import threading
import time

from LocationGenerator import *

g_tripID = 0
g_delaySec = 1 # delay between points
g_maxDistance = 10 # max points of travel
g_debugLevel = 2

class Trip (threading.Thread):
    '''Trip -
    A collection of points (longitude, latitude) that have a start, end,
    and fare.
    '''
    def __init__(self):
        self.debugLevel = g_debugLevel

        # Threading
        threading.Thread.__init__(self)
        self.exitFlag = False
        self.started = False

        # Trip data
        global g_tripID
        self.tripID = g_tripID
        g_tripID += 1
        self.fare = 0
        self.location = LocationGenerator()
        self.locations = [] # local copy of trip points

        # Each trip will be of a randomized distance
        # XXX We could have this be a param into our class
        from random import randint
        self.distance = randint(1, g_maxDistance)

    def getTripID(self):
        return self.tripID

    def addFare(self, fare):
        self.fare += fare
        return self.fare

    def log(self, loglevel, *args):
        if loglevel <= self.debugLevel:
            print args

    def addLocation(self, newLocation):
        # XXX Instead of appending to
        # this array
        self.locations.append(newLocation)
        self.distance -= 1
        self.log(4, "%d: appending %s distance left %d"
                 % (self.tripID, newLocation, self.distance))

    def startTrip(self):
        # We only allow the trip to be started once.
        self.log(3, "starting %d distance %d" %(self.tripID, self.distance))
        assert len(self.locations) == 0
        if not self.started:
            self.started = True
            self.addLocation(self.location.getNext())
            self.start()

    def endTrip(self):
        if self.started:
            self.addLocation(self.location.getNext())
            self.exitFlag = True
            self.started = False

    def run(self):
        self.travel()
        self.endTrip()
        self.log(2, "trip data: ", self.tripID, self.locations)

    def travel(self):
        '''Generates random travel points for self.distance.  This could be replaced
           and driven by an external source that uses the Trip class.
        '''
        while self.distance > 1:
            if self.exitFlag:
                return
            time.sleep(g_delaySec)
            self.addLocation(self.location.getNext())
