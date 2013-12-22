#!/usr/bin/env python

##
# simulator.py -
#    Module for driving the Trip generator.
##

from __future__ import absolute_import
import sys
import time
from tripgrid.Trip import *

def main():
    numTrips = 500
    trips = []
    while True:
        print "adding %d trips" % (numTrips)
        for x in range(0, numTrips):
            trip = Trip()
            trips.append(trip)
            trip.startTrip()

        print "trips running: ", len(trips)
        time.sleep(5)

        numTrips = 0
        for trip in trips:
            if trip.ended():
                numTrips += 1
                trips.remove(trip)

if __name__ == "__main__":
    main()
