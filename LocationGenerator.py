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

g_maxLatitude = 90
g_minLatitude = -90
g_maxLongitude = 180
g_minLongitude = -180
g_maxDelta = 5 # +/- from previous point

class LocationGenerator:
    '''Module for generating psuedo longitude and latitude data pairs.
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
            yMove = randint(-g_maxDelta, g_maxDelta)
            return (self.lastX + xMove, self.lastY + yMove)
