#AGGREGATOR

import time
from rq import get_current_job, Queue, Connection
import socket

import pandas as pd
import numpy as np

from sklearn import svm
from sklearn.externals import joblib

from io import StringIO

import time
import datetime
from dateutil import tz
import redis

#Aggregator

NODE_COUNT = '_node_count'
DONE_NODE_COUNT = '_done_node_count'
NODES = '_nodes'

REDIS_URL = 'redis://redis:6379/0'
QUEUES = ['default']

# redis_connection = redis.from_url(REDIS_URL)
redis_connection = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

#this might be slower, and i should probably put the tasks inside the worker.py
#or can I just preload the libraries there?

#Move these redis api to some other file
def delRedisFromAllKeys(r, K):
  try:
    for key in K:
      r.delete(K)
  except:
    return false

def setRedisKV(r, K, V):
  try:
    # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.set(K, V)
    return True
  except:
    return False

def incrRedisKV(r, K):
  try:
    # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.incr(K)
    return True
  except:
    return False

def getRedisV(r, K):
  # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  output = r.get(K)
  if output is not None:
    return output
  else:
    return "Empty"

def appendToListK(r, K, V):
  try:
    # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.rpush(K, V)
    return True
  except:
    return False

def getListK(r, K):
  # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  output = r.lrange(K, 0, -1)
  if output is not None:
    return output
  else:
    return "Empty"

def get_current_time():
  HERE = tz.gettz('Asia/Tokyo')
  UTC = tz.gettz('UTC')

  ts = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(HERE)
  # local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
  local_time = ts.strftime('%Y-%m-%d %H:%M:%S %Z')
  return local_time

# def aggregate_data(unique_ID):
#   redis_connection = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
#   node_count = getRedisV(redis_connection, unique_ID + NODE_COUNT)
#   done_node_count = getRedisV(redis_connection, unique_ID + DONE_NODE_COUNT)
#   status = getRedisV(redis_connection, unique_ID)

#   if status == "finished":
#     with Connection(redis.from_url(REDIS_URL)):
#       node_task_id_list = getListK(redis_connection, unique_ID + NODES)

#   d = {'JP': 'Talusan', 'result': status, 'unique_ID': unique_ID, 'done_node_count': done_node_count, 'node_task_id_list': node_task_id_list}
#   return d

def aggregate_data(unique_ID):
  tic = time.clock()
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = get_current_time()
  job.meta['progress'] = 0.0
  job.meta['unique_ID'] = unique_ID

  redis_connection = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  # redis_connection = redis.from_url(REDIS_URL)
  node_count = getRedisV(redis_connection, unique_ID + NODE_COUNT)
  done_node_count = getRedisV(redis_connection, unique_ID + DONE_NODE_COUNT)
  status = getRedisV(redis_connection, unique_ID)
  if status == "finished":
    with Connection(redis.from_url(REDIS_URL)):
      node_task_id_list = getListK(redis_connection, unique_ID + NODES)
      # all_results = node_task_id_list
      #TODO: Assuming the return of the processors are lists
      all_results = []
      for task_id in node_task_id_list:
        # all_results += task_id
        q = Queue('default')
        task = q.fetch_job(task_id)
        if task is not None:
          # all_results.append(task.result)
          #or
          all_results += task.result
        # all_results += task.result.decode("utf-8")

      d = {'result': all_results, 'unique_ID': unique_ID, 'done_node_count': done_node_count, 'node_task_id_list': node_task_id_list}

      toc = time.clock()
      job.meta['progress'] = toc - tic
      job.save_meta()
      return d