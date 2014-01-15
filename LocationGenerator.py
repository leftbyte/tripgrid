#!/usr/bin/env python

##
# LocationGenerator.py -
#
#    Module for generating psuedo "longitude" and "latitude" data pairs.
#
#    XXX: change to actual latitude/longitude points.
#    XXX: import actual city map and snap to intersections.
#    This module generates points between the max/min long/lat values, at
#    integer increments.  We probably want to generate something to 4 decimal
#    points (scale of meters).
#
#    see:
#      http://en.wikipedia.org/wiki/Decimal_degrees
#
##

import os
import sys
from random import randint
from TripCommon import *

class LocationGenerator:
    '''
    Module for generating psuedo longitude and latitude data pairs.
    '''
    def __init__(self, testQuadrant=None):
        self.lastX = None
        self.lastY = None

        global g_minLatitude
        global g_maxLatitude
        global g_minLongitude
        global g_maxLongitude

        self.minLatitude = g_minLatitude
        self.maxLatitude = g_maxLatitude
        self.minLongitude = g_minLongitude
        self.maxLongitude = g_maxLongitude

        if testQuadrant is not None:
            if testQuadrant == TOP_LEFT:
                self.minLatitude   =  -72
                self.maxLatitude   =   -1
                self.minLongitude  =   36
                self.maxLongitude  =  143
            elif testQuadrant == TOP_RIGHT:
                self.minLatitude   =   18
                self.maxLatitude   =   71
                self.minLongitude  =   36
                self.maxLongitude  =  143
            elif testQuadrant == BOTTOM_RIGHT:
                self.minLatitude   =   18
                self.maxLatitude   =   71
                self.minLongitude  = -144
                self.maxLongitude  =   -1
            elif testQuadrant == BOTTOM_LEFT:
                self.minLatitude   =  -72
                self.maxLatitude   =   -1
                self.minLongitude  = -144
                self.maxLongitude  =   -1
            elif testQuadrant == LEFT_EDGE:
                self.minLatitude   =  -90
                self.maxLatitude   =  -90
                self.minLongitude  = -144
                self.maxLongitude  =  143
            elif testQuadrant == TOP_EDGE:
                self.minLatitude   =  -72
                self.maxLatitude   =   71
                self.minLongitude  =  180
                self.maxLongitude  =  180
            elif testQuadrant == RIGHT_EDGE:
                self.minLatitude   =   90
                self.maxLatitude   =   90
                self.minLongitude  = -144
                self.maxLongitude  =  143
            elif testQuadrant == BOTTOM_EDGE:
                self.minLatitude   =  -72
                self.maxLatitude   =   71
                self.minLongitude  = -180
                self.maxLongitude  = -180
            else:
                raise Exception("Unknown Test Quadrant:", testQuadrant)

    def getNext(self):
        if self.lastX is None or self.lastY is None:
            self.lastX = randint(self.minLatitude, self.maxLatitude)
            self.lastY = randint(self.minLongitude, self.maxLongitude)
            return (self.lastX, self.lastY)
        else:
            xMove = randint(-g_maxDelta, g_maxDelta)
            # cap the X new value
            if self.minLatitude <= (self.lastX + xMove) <= self.maxLatitude:
                newX = self.lastX + xMove
            elif self.minLatitude >= (self.lastX + xMove):
                newX = self.minLatitude
            else:
                newX = self.maxLatitude

            yMove = randint(-g_maxDelta, g_maxDelta)
            # cap the Y new value
            if self.minLongitude <= (self.lastY + yMove) <= self.maxLongitude:
                newY = self.lastY + yMove
            elif self.minLongitude >= (self.lastY + yMove):
                newY = self.minLongitude
            else:
                newY = self.maxLongitude

            checkLocation((newX, newY))
            return (newX, newY)
