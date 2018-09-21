import redis
from rq import Worker, Connection, get_current_job
import time

import socket

import pandas as pd
import numpy as np

from sklearn import svm
from sklearn.externals import joblib

from io import StringIO

import datetime
from dateutil import tz

REDIS_URL = 'redis://redis:6379/0'
QUEUES = ['aggregator']


redis_connection = redis.from_url(REDIS_URL) 
if __name__ == '__main__':
  with Connection(redis_connection):
    worker = Worker(QUEUES)
    worker.work()
