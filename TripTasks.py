#!/usr/bin/env python

##
# TripTasks.py -
#
#    Module defining the celery tasks used by the Trip generator and processed
#    by the TripProcessor.
##

from __future__ import absolute_import

from tripgrid.celery import app
from tripgrid.TripGrid import *

# XXX: I don't think there's one static instance of this motherfucker
g_tripGrid = TripGrid()

@app.task
def TripBegin(id, lat, long):
    print "TripBegin %d: %d,%d" %(id, lat, long)
    g_tripGrid.startTrip(id, (lat, long))

@app.task
def TripUpdate(id, lat, long):
    print "TripUpdate %d: %d,%d" %(id, lat, long)
    g_tripGrid.updateTrip(id, (lat, long))

@app.task
def TripEnd(id, fare, lat, long):
    print "TripEnd %d: %d,%d = $%d" %(id, lat, long, fare)
    g_tripGrid.endTrip(id, fare, (lat, long))
