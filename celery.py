#!/usr/bin/env python

##
# celery.py -
#
#    Module defining the celery configuration.
##

from __future__ import absolute_import

from celery import Celery

app = Celery('tripgrid',
             broker='amqp://',
             backend='amqp://',
             include=['tripgrid.TripTasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)

if __name__ == '__main__':
    app.start()
