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
import collections

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
    r.set(K, V)
    return True
  except:
    return False

def incrRedisKV(r, K):
  try:
    r.incr(K)
    return True
  except:
    return False

def getRedisV(r, K):
  output = r.get(K)
  if output is not None:
    return output
  else:
    return "Empty"

def appendToListK(r, K, V):
  try:
    r.rpush(K, V)
    return True
  except:
    return False

def getListK(r, K):
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
  local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z')
  return local_time

def aggregate_nuts_data(unique_ID):
  #I think this is because of the print in the debug of dashboard?
  #Add sleep if using docker-compose up debug mode
  #Looks like this needs to be here
  # time.sleep(2)
  tic = time.clock()
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = get_current_time()
  job.meta['progress'] = 0.0
  # job.meta['unique_ID'] = unique_ID

  redis_connection = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  node_count = getRedisV(redis_connection, unique_ID + NODE_COUNT)
  done_node_count = getRedisV(redis_connection, unique_ID + DONE_NODE_COUNT)
  status = getRedisV(redis_connection, unique_ID)
  if status == "finished" and node_count == done_node_count:
    with Connection(redis.from_url(REDIS_URL)):
      node_task_id_list = getListK(redis_connection, unique_ID + NODES)
      # all_results = node_task_id_list
      #TODO: Assuming the return of the processors are lists
      all_results = []
      #Checking sequence just in case, but costs another for loop
      sequence_dict = {}
      for task_id in node_task_id_list:
        # all_results += task_id
        q = Queue('default')
        task = q.fetch_job(task_id)
        if task is not None:
          all_results += task.result['output']

      d = {'result': all_results, 
           'unique_ID': unique_ID, 
           'node_count': node_count, 
           'done_node_count': done_node_count, 
           'node_task_id_list': node_task_id_list}

      toc = time.clock()
      job.meta['progress'] = toc - tic
      job.save_meta()
      return d

def aggregate_data(unique_ID):
  tic = time.clock()
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = get_current_time()
  job.meta['progress'] = 0.0
  # job.meta['unique_ID'] = unique_ID

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

      #Checking sequence just in case, but costs another for loop
      sequence_dict = {}
      for task_id in node_task_id_list:
        # all_results += task_id
        q = Queue('default')
        task = q.fetch_job(task_id)
        if task is not None:
          # all_results.append(task.result)
          #or
          task_results = task.result["output"]
          sequence_ID = task.result["sequence_ID"]
          sequence_dict[sequence_ID] = task_results
        # all_results += task.result.decode("utf-8")
      ordered_sequence_dict = collections.OrderedDict(sorted(sequence_dict.items()))
      for k, v in ordered_sequence_dict.items(): 
        print(k, v)
        all_results += v

      d = {'result': all_results, 'unique_ID': unique_ID, 'done_node_count': done_node_count, 'node_task_id_list': node_task_id_list}

      toc = time.clock()
      job.meta['progress'] = toc - tic
      job.save_meta()
      return d
