#PROCESSOR

import time
from rq import get_current_job
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
from rq import Queue, Connection

NODE_COUNT = '_node_count'
DONE_NODE_COUNT = '_done_node_count'
NODES = '_nodes'

REDIS_URL = 'redis://redis:6379/0'
QUEUES = ['default']

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

def get_current_time():
  HERE = tz.gettz('Asia/Tokyo')
  UTC = tz.gettz('UTC')

  ts = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(HERE)
  # local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
  local_time = ts.strftime('%Y-%m-%d %H:%M:%S %Z')
  return local_time

def create_task(task_type, unique_ID):
  job = get_current_job()
  timer = int(task_type) * 10
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = get_current_time()
  job.save_meta()
  for i in range(timer):
    job.meta['progress'] = 100.0 * i / timer
    job.save_meta()
    time.sleep(1)
  job.meta['progress'] = 100.0
  job.save_meta()

  redis_connection = redis.StrictRedis(host="redis", port=6379, password="", charset="utf-8", decode_responses=True)

  setRedisKV(redis_connection, unique_ID, "finished")
  incrRedisKV(redis_connection, unique_ID + DONE_NODE_COUNT)
  done_node_count = getRedisV(redis_connection, unique_ID + DONE_NODE_COUNT)
  if done_node_count is not None:
    #Maybe pass queue to aggregator
    d = {'result': 'blahblah', 'unique_ID': unique_ID, 'done_node_count': done_node_count}
  else:
    d = {'result': 'blahblah', 'unique_ID': unique_ID, 'done_node_count': 'None'}
  return d

  # return {'result':unique_ID}

def classify_iris(input_data):
  tic = time.clock()

  input_dataIO = StringIO(input_data)
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = get_current_time()
  job.meta['progress'] = 0.0
  job.save_meta()

  df = pd.read_csv(input_dataIO, header=None)
  print(df.shape)

  clf = joblib.load('models/SVM_iris.pkl')
  prob = clf.predict_proba(df)

  df_prob = pd.DataFrame(prob)
  prob_idx = df_prob.idxmax(axis=1)

  toc = time.clock()
  job.meta['progress'] = toc - tic
  job.save_meta()
  iris_classes = ['setosa', 'versicolor', 'virginica']

  output = [iris_classes[x] for x in prob_idx]
  print(output)
  return output

def classify_iris_dist(input_data, nodes, unique_ID):
  job = get_current_job()
  redis_connection = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  tic = time.clock()

  appendToListK(redis_connection, unique_ID + NODES, job.id)

  input_dataIO = StringIO(input_data.decode("utf-8"))
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = get_current_time()
  job.meta['progress'] = 0.0
  job.meta['unique_ID'] = unique_ID
  job.save_meta()

  df = pd.read_csv(input_dataIO, header=None)
  print(df.shape)

  clf = joblib.load('models/SVM_iris.pkl')
  prob = clf.predict_proba(df)

  df_prob = pd.DataFrame(prob)
  prob_idx = df_prob.idxmax(axis=1)

  toc = time.clock()
  job.meta['progress'] = toc - tic
  job.save_meta()
  iris_classes = ['setosa', 'versicolor', 'virginica']

  output = [iris_classes[x] for x in prob_idx]
  print(output)

  incrRedisKV(redis_connection, unique_ID + DONE_NODE_COUNT)
  node_count = getRedisV(redis_connection, unique_ID + NODE_COUNT)
  done_node_count = getRedisV(redis_connection, unique_ID + DONE_NODE_COUNT)

  if node_count == done_node_count:
    setRedisKV(redis_connection, unique_ID, "finished")
    with Connection(redis.from_url(REDIS_URL)):
      #Maybe add a differnetname?
      q = Queue('aggregator')
      t = q.enqueue('tasks.aggregate_data', unique_ID)

      # print('Starting aggregation: ' + t.get_id())
  else:
    print('still not done processing')

  return output