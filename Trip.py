#!/usr/bin/env python

##
# Trip.py -
#    A trip generator.
##

from __future__ import absolute_import

import os
import sys
import threading
import time
from random import randint
from tripgrid.LocationGenerator import *
import tripgrid.TripGrid

g_debugLevel = 2
g_tripID = 0            # trip identifier
g_delaySec = 1          # delay between points
g_maxDistance = 10      # max points of travel
g_pricePerUnit = 1      # $ / distance travelled.

class Trip(threading.Thread):
    '''
    Trip - A trip generator that runs as a thread.  startTrip is used to run the
    thread, from which a random collection of points (longitude, latitude) are
    generated and sent to the database using tasks defined by the TripGrid.
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
        self.locations = [] # local copy of trip points, for debugging
        self.previousLocation = None

        # Each trip will be of a randomized distance
        self.distance = randint(1, g_maxDistance)

    def log(self, loglevel, *args):
        if loglevel <= self.debugLevel:
            print args

    def getTripID(self):
        return self.tripID

    def addFare(self, newLocation):
        # we calculate fare as each x and y unit moved.
        if self.previousLocation is not None:
            xMoved = abs(self.previousLocation[0] - newLocation[0])
            yMoved = abs(self.previousLocation[1] - newLocation[1])
            self.fare += g_pricePerUnit * (xMoved + yMoved)
        self.previousLocation = newLocation
        return self.fare

    def addLocation(self, newLocation):
        self.locations.append(newLocation)
        self.distance -= 1
        self.addFare(newLocation)
        self.log(4, "%d: appending %s distance left %d current fare %d"
                 % (self.tripID, newLocation, self.distance, self.fare))

    def startTrip(self):
        '''
        Start the trip thread.
        '''
        # We only allow the trip to be started once.
        assert len(self.locations) == 0
        if not self.started:
            self.log(2, "starting %d distance %d" %(self.tripID, self.distance))
            self.started = True
            nextLoc = self.location.getNext()
            self.addLocation(nextLoc)
            tripgrid.TripGrid.startTrip.delay(self.tripID, nextLoc)
            self.start()

    def endTrip(self):
        '''
        End the trip.
        '''
        if not self.started:
            raise Exception("Error: endTrip called without having been started.")
        nextLoc = self.location.getNext()
        self.addLocation(nextLoc)
        tripgrid.TripGrid.endTrip.delay(self.tripID, self.fare, nextLoc)
        self.exitFlag = True
        self.started = False

    def run(self):
        '''
        Main thread method
        '''
        self.travel()
        self.log(2, "trip data: %d $%d %r" % (self.tripID, self.fare, self.locations))

    def travel(self):
        '''
        Generates random travel points for self.distance.  This could be replaced
        and driven by an external source that uses the Trip class.
        '''
        while self.distance > 1:
            if self.exitFlag:
                return
            time.sleep(g_delaySec)
            nextLoc = self.location.getNext()
            self.addLocation(nextLoc)
            tripgrid.TripGrid.updateTrip.delay(self.tripID, nextLoc)
        self.endTrip()
