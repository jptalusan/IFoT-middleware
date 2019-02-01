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
import csv
import json

from common.mqtt_utils import MqttLog
from common.defs import *

#Aggregator

NODE_COUNT = '_node_count'
DONE_NODE_COUNT = '_done_node_count'
NODES = '_nodes'

TASK_COUNT      = '_task_count'
DONE_TASK_COUNT = '_done_task_count'
TASK_SUFFIX     = '_task'
TASK_HANDLER_SUFFIX = '_queue'
EXEC_TIME_INFO  = 'exec_time_info'

REDIS_URL = 'redis://redis:6379/0'
QUEUES = ['default']

# redis_connection = redis.from_url(REDIS_URL)
redis_connection = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

#this might be slower, and i should probably put the tasks inside the worker.py
#or can I just preload the libraries there?

def aggregate_nuts_data(unique_ID):
  #I think this is because of the print in the debug of dashboard?
  #Add sleep if using docker-compose up debug mode
  #Looks like this needs to be here
  # time.sleep(2)
  tic = time.clock()
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  #job.meta['handled_time'] = get_current_time()
  job.meta['handled_time'] = int(time.time())
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
      model_type = '' 
      for task_id in node_task_id_list:
        # all_results += task_id
        q = Queue('default')
        task = q.fetch_job(task_id)
        if task is not None:
          all_results += task.result['output']
          model_type = task.result['model_type']

      d = {'result': all_results, 
           'unique_ID': unique_ID, 
           'node_count': node_count, 
           'done_node_count': done_node_count, 
           'node_task_id_list': node_task_id_list,
           'model_type': model_type}

      toc = time.clock()
      job.meta['progress'] = toc - tic
      job.save_meta()
      return d

def aggregate_data(unique_ID):
  tic = time.clock()
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  #job.meta['handled_time'] = get_current_time()
  job.meta['handled_time'] = int(time.time())

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

def aggregate_activity_classifs(unique_id):
  aggregation_start_time = get_redis_server_time()
  tic = time.clock()
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())

  job.meta['progress'] = 0.0

  redis_connection = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  task_count = getRedisV(redis_connection, unique_id + TASK_COUNT)
  done_task_count = getRedisV(redis_connection, unique_id + DONE_TASK_COUNT)
  status = getRedisV(redis_connection, unique_id)
  if status == "finished":
    with Connection(redis.from_url(REDIS_URL)):
      node_task_id_list = getListK(redis_connection, unique_id + TASK_SUFFIX)
      # all_results = node_task_id_list
      #TODO: Assuming the return of the processors are lists
      all_results = []

      #Checking sequence just in case, but costs another for loop
      sequence_dict = {}
      for task_id in node_task_id_list:
        q = Queue("default")
        task = q.fetch_job(task_id)

        if task is not None:
          task_results = task.result["output"]
          sequence_ID = task.result["sequence_ID"]
          sequence_dict[sequence_ID] = task_results

      ordered_sequence_dict = collections.OrderedDict(sorted(sequence_dict.items()))
      for k, v in ordered_sequence_dict.items():
        all_results += v

      d = { 'result': all_results,
            'unique_id': unique_id,
            'done_task_count': done_task_count,
            'node_task_id_list': node_task_id_list }

      toc = time.clock()
      job.meta['progress'] = toc - tic
      job.save_meta()

      # Log execution time info to redis
      add_exec_time_info(unique_id, "aggregation", aggregation_start_time, get_redis_server_time())

      return d

# def aggregate_average_speeds(task_graph, reference_id, params):
#   # Resolve task info and parameters
#   task_info     = task_graph[reference_id]
#   unique_id     = task_info['unique_id']
#   node_id       = task_info['node_id']
# 
#   # Log the beginning of processing event
#   log = MqttLog("AGG-001", unique_id)
#   log.event('aggregation', 'started')
#   aggregation_start_time = get_redis_server_time()
#   tic = time.clock()
# 
#   # Notify queue of job pick up
#   job = get_current_job()
#   notify_job_pick_up(job, unique_id, task_info)
# 
#   # TODO Get status of the task processing
#   redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
#   status = getRedisV(redis_conn, unique_id)
# 
#   if status == "finished":
#     with Connection(redis.from_url(REDIS_URL)):
#       # Get a list of task ids
#       task_id_list_all = getListK(redis_conn, R_TASKS_LIST.format(unique_id))
# 
#       # Resolve information about each task given their task ids
#       task_list = []
#       for task_id in task_id_list_all:
#         json_task_info = getRedisV(redis_conn, R_TASK_INFO.format(unique_id, task_id))
#         finished_task = json.loads(json_task_info)
# 
#         # Check if the finished task directly targets this node
#         for dest_task in finished_task['dest']:
#           if dest_task['node_id'] == task_info['node_id'] and \
#              dest_task['type'] == task_info['type'] and \
#              dest_task['order'] == task_info['order']:
# 
#             task_list.append(finished_task)
#             break
# 
#       # Create a list connecting task ids to their assigned queue ids
#       task_to_queue_list = [ { 'task_id' : t['task_id'], 'queue_id' : t['queue_id'] } for t in task_list ]
# 
#       #Checking sequence just in case, but costs another for loop
#       agg_result = {}
#       for queued_task in task_to_queue_list:
#         q = Queue(queued_task['queue_id'])
#         task = q.fetch_job(queued_task['task_id'])
# 
#         if task is not None:
#           sequence_ID = task.result["sequence_ID"]
# 
#           for result in task.result["output"]["aggregated_speeds"]:
#             rsu_id = str(result[0])
#             # direction = str(result[1])
#             speed = float(result[1])
#             count = int(result[2])
# 
#             if not rsu_id in agg_result:
#               agg_result[rsu_id] = {'speed' : 0, 'count' : 0}
# 
#             agg_result[rsu_id]['speed'] += speed
#             agg_result[rsu_id]['count'] += count
# 
# 
#       for rsu_id in agg_result.keys():
#         speed_info = agg_result[rsu_id]
#         agg_result[rsu_id] = speed_info['speed'] / speed_info['count']
# 
#       d = { 'result': agg_result,
#             'unique_id': unique_id,
#             # 'done_task_count': done_task_count,
#             'node_task_id_list': task_to_queue_list }
# 
#       toc = time.clock()
#       job.meta['progress'] = toc - tic
#       job.save_meta()
# 
#       # Log execution time info to redis
#       aggregation_end_time = get_redis_server_time()
#       add_exec_time_info(unique_id, "aggregation", aggregation_start_time, aggregation_end_time)
#       log.event('aggregation', 'finished')
#       log.exec_time('aggregation', aggregation_start_time, aggregation_end_time)
# 
#       # Log aggregation results
#       print("Logging results...")
#       log.results(d)
#       print("Done.")
# 
#       return d

def most_common(lst):
  return max(set(lst), key=lst.count)

def aggregate_decision_trees(unique_id):
  aggregation_start_time = get_redis_server_time()
  tic = time.clock()
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())

  job.meta['progress'] = 0.0

  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  task_count = getRedisV(redis_conn, unique_id + TASK_COUNT)
  done_task_count = getRedisV(redis_conn, unique_id + DONE_TASK_COUNT)
  status = getRedisV(redis_conn, unique_id)
  if status == "finished":
    with Connection(redis.from_url(REDIS_URL)):
      node_task_id_list = getListK(redis_conn, unique_id + TASK_SUFFIX)
      # all_results = node_task_id_list
      #TODO: Assuming the return of the processors are lists
      all_results = []

      #Checking sequence just in case, but costs another for loop
      agg_result = {}
      for task_id in node_task_id_list:
        q = Queue('default')
        task = q.fetch_job(task_id)

        if task is not None:
          sequence_ID = task.result["sequence_ID"]

          output = task.result["output"]["aggregate_decision_trees"]

          # j = json.loads(output)
          # result = j['result']
          result = output.replace(";", "\n")

          buff = StringIO(result)
          reader = csv.reader(buff)

          labels = []
          for line in reader:
            label = line[0]
            llist = line[1:]
            ddict = {}
            ddict[label] = most_common(llist)
            labels.append(ddict)
      # for rsu_id in agg_result.keys():
      #   speed_info = agg_result[rsu_id]
      #   agg_result[rsu_id] = speed_info['speed'] / speed_info['count']

      d = { 'result': labels,
            'unique_id': unique_id,
            'done_task_count': done_task_count,
            'node_task_id_list': node_task_id_list }

      toc = time.clock()
      job.meta['progress'] = toc - tic
      job.save_meta()

      # Log execution time info to redis
      add_exec_time_info(unique_id, "aggregation", aggregation_start_time, get_redis_server_time())

      return d

###
##    S0003: Redis Queue Interaction Functions
###
def get_redis_server_time():
  # Start a redis connection
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  sec, microsec = r.time()
  return ((sec * 1000000) + microsec)

def add_exec_time_info(unique_id, operation, time_start, time_end):
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Add the unique_id to the execution time info list if it does not yet exist
  r.sadd(EXEC_TIME_INFO, unique_id)

  # Push an operation to the execution time log for this unique_id
  log_obj = {
    'operation'   : operation,
    'start_time'  : str(time_start),
    'end_time'    : str(time_end),
    'duration'    : str(float(time_end - time_start) / 1000000.0),
  }
  r.lpush("{}_{}".format(EXEC_TIME_INFO, unique_id), json.dumps(log_obj))

  return True

def notify_job_pick_up(job, unique_id, task_info):
  # Update job meta information
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())
  job.meta['progress'] = 0.0
  job.meta['unique_id'] = unique_id
  job.meta['task_type'] = task_info['type']
  job.meta['task_index'] = task_info['seq_id']
  job.meta['queue_id'] = task_info['node_id']

  job.save_meta()

  # Update redis task entry
  task_info = {
    'task_id'       : job.id,
    'unique_id'     : job.meta['unique_id'],
    'type'          : job.meta['task_type'],
    'index'         : job.meta['task_index'],
    'queue_id'      : job.meta['queue_id'],
    'handled_by'    : job.meta['handled_by'],
    'handled_time'  : job.meta['handled_time'],
    'progress'      : job.meta['progress'],
  }

  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Save task info to redis
  redis_task_info_key = R_TASK_INFO.format(unique_id, job.id)
  redis_conn.set(redis_task_info_key, json.dumps(task_info))

  return

def notify_job_exec_start(job, unique_id, worker_id, task_info):
  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Push handled job to the redis execution list for this worker
  redis_task_list_key = R_TASKS_LIST.format(unique_id)
  utils.appendToListK(redis_conn, unique_id + TASK_SUFFIX, job.id )
  # utils.appendToListK(redis_conn, unique_id + TASK_HANDLER_SUFFIX, json.dumps({ str(job.id) : str(worker_id) }) )

  return

def notify_job_exec_end(job, unique_id, worker_id, task_info, task_start, task_end):
  task_type_id  = "{}_{}".format(task_info['type'], task_info['order'])

  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Update job progress
  job.meta['progress'] = task_end - task_start
  job.save_meta()

  # Increment finished nodes counter
  redis_done_count_key = R_TASK_TYPE_DONE.format(unique_id, task_type_id)
  redis_conn.incr(redis_done_count_key)

  return

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


