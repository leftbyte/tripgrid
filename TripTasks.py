from __future__ import absolute_import

from tripgrid.celery import app

@app.task
def TripBegin(id, lat, long):
    print "TripBegin %d: %d,%d" %(id, lat, long)

@app.task
def TripUpdate(id, lat, long):
    print "TripUpdate %d: %d,%d" %(id, lat, long)

@app.task
def TripEnd(id, fare, lat, long):
    print "TripEnd %d: %d,%d = $%d" %(id, lat, long, fare)


