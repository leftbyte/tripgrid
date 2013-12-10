#!/usr/bin/env python

##
# simulator.py -
#    Module for driving the Trip and TripProcessor.
##

from __future__ import absolute_import
import sys
import time
from tripgrid.Trip import *

def main():
    numTrips = 6
    # XXX we actually don't need to keep the trips around...
    trips = []
    for x in range(0, numTrips):
        trip = Trip()
        trips.append(trip)
        trip.startTrip()
        # at this point the Trip is creating new points every second.

    # Wait for some time, then Issue some queries

if __name__ == "__main__":
    main()
