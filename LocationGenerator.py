#!/usr/bin/env python

##
# LocationGenerator.py -
#
#    Module for generating psuedo "longitude" and "latitude" data pairs.
#
#    Actually, it just generates points between the max/min long/lat values, at
#    integer increments.  We probably want to generate something to 4 decimal
#    points (scale of meters).
#
#    XXX read:
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
    def __init__(self):
        self.lastX = None
        self.lastY = None

    def getNext(self):
        if self.lastX is None or self.lastY is None:
            self.lastX = randint(g_minLatitude, g_maxLatitude)
            self.lastY = randint(g_minLongitude, g_maxLongitude)
            return (self.lastX, self.lastY)
        else:
            xMove = randint(-g_maxDelta, g_maxDelta)
            # cap the X new value
            if g_minLatitude <= (self.lastX + xMove) <= g_maxLatitude:
                newX = self.lastX + xMove
            elif g_minLatitude >= (self.lastX + xMove):
                newX = g_minLatitude
            else:
                newX = g_maxLatitude

            yMove = randint(-g_maxDelta, g_maxDelta)
            # cap the Y new value
            if g_minLongitude <= (self.lastY + yMove) <= g_maxLongitude:
                newY = self.lastY + yMove
            elif g_minLongitude >= (self.lastY + yMove):
                newY = g_minLongitude
            else:
                newY = g_maxLongitude

            checkLocation((newX, newY))
            return (newX, newY)
