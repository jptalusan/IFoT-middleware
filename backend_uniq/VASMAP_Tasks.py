import os
import redis
import rq
import time
import socket
import json
import requests
import pandas as pd
import numpy as np
import datetime as dt

from rq import Queue, Connection
from common.mqtt_utils import MqttLog
from common.defs import *
from common import utils

###
##    S0001: Main Functions
###
def collect_rsu_data(task_graph, reference_id, params):
  # Resolve task info and parameters
  task_info     = task_graph[reference_id]
  unique_id     = task_info['unique_id']
  node_id       = task_info['node_id']
  db_info       = params['db_info']
  start_time    = params['start_time']
  end_time      = params['end_time']

  # Notify queue of job pick up
  job = rq.get_current_job()
  notify_job_pick_up(job, unique_id, task_info)

  # Log the beginning of collection event
  log = MqttLog(node_id, unique_id)
  log.event('collection', 'started')
  process_start_time = get_redis_server_time()
  tic = time.perf_counter()

  # Add handled job to execution list
  notify_job_exec_start(job, unique_id, node_id, task_info)

  # Retrieve the data from the InfluxDB
  resp = query_influx_db( start_time, end_time,
                          host=db_info['host'],
                          port=db_info['port'],
                          influx_db=db_info['name'],
                          influx_ret_policy=db_info['ret_policy'],
                          influx_meas=db_info['meas'])

  # Split into columns and values
  columns = json.loads(resp.text)['results'][0]['series'][0]['columns']
  values =  json.loads(resp.text)['results'][0]['series'][0]['values']

  # Load the data
  df = pd.DataFrame(values, columns=columns)

  # TODO Route the data to each destination node
  for dest_info in task_info['dest']:
    print(dest_info['node_id'])
    node_df = df[df['rsu_id'] == dest_info['node_id']]
    print(node_df.shape)
    params = {
      'columns' : list(node_df.columns.values),
      'values'  : list(node_df.values),
      'db_info' : db_info,
    }

    # Get the matching task in the task graph
    dest_task = None
    for task in task_graph:
        if task['node_id'] == dest_info['node_id'] and \
           task['type'] == dest_info['type'] and \
           task['order'] == dest_info['order']:
          
          dest_task = task
          break

    if dest_task == None: 
        continue

    q = Queue(dest_task['node_id'])
    t = q.enqueue(dest_task['func'], task_graph, dest_task['ref_id'], params, depends_on=job.id) #job is this current job

  # Update job progress
  toc = time.clock()
  notify_job_exec_end(job, unique_id, node_id, task_info, tic, toc)

  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Retrieve the task counts for this
  task_type_id  = "{}_{}".format(task_info['type'], task_info['order'])
  redis_done_count_key  = R_TASK_TYPE_DONE.format(unique_id, task_type_id)
  redis_total_count_key = R_TASK_TYPE_TOTAL.format(unique_id, task_type_id)
  task_count      = utils.getRedisV(redis_conn, redis_total_count_key)
  done_task_count = utils.getRedisV(redis_conn, redis_done_count_key)

  # Create metadata
  metas = {
    'job_id' : job.id,
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
  }

  # TODO Delay

  # Log execution time info to redis
  process_end_time = get_redis_server_time()
  add_exec_time_info(unique_id, "{}-{}".format(task_type_id, task_info['seq_id']), process_start_time, process_end_time)
  log.event("collection", "finished")
  log.exec_time('collection', process_start_time, process_end_time)

  return { 'sequence_ID': task_info['seq_id'], 
           'metas' : metas, 
           'output': [], 
           'outsize': 0, }

def average_by_rsu(task_graph, reference_id, params):
  # Resolve task info and parameters
  task_info     = task_graph[reference_id]
  unique_id     = task_info['unique_id']
  node_id       = task_info['node_id']
  db_info       = params['db_info']
  columns       = params['columns']
  values        = params['values']

  # Notify queue of job pick up
  job = rq.get_current_job()
  notify_job_pick_up(job, unique_id, task_info)

  # Log the beginning of processing event
  log = MqttLog(node_id, unique_id)
  log.event('processing', 'started')
  process_start_time = get_redis_server_time()
  tic = time.perf_counter()

  # Add handled job to execution list
  notify_job_exec_start(job, unique_id, node_id, task_info)

  # Load the data
  df = pd.DataFrame(values, columns=columns)

  # Get the sum of all speeds by RSU ID how many were summed in each step
  agg_speeds = df.groupby(['rsu_id'], as_index=False)['speed'].sum()
  agg_speeds['count'] = df.groupby(['rsu_id'], as_index=False)['speed'].count()['speed']

  results = {
    'aggregated_speeds' : agg_speeds.values.tolist(),
  }

  # Update job progress
  toc = time.clock()
  notify_job_exec_end(job, unique_id, node_id, task_info, tic, toc)

  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Retrieve the task counts for this
  task_type_id  = "{}_{}".format(task_info['type'], task_info['order'])
  redis_done_count_key  = R_TASK_TYPE_DONE.format(unique_id, task_type_id)
  redis_total_count_key = R_TASK_TYPE_TOTAL.format(unique_id, task_type_id)
  task_count      = utils.getRedisV(redis_conn, redis_total_count_key)
  done_task_count = utils.getRedisV(redis_conn, redis_done_count_key)

  # Create metadata
  metas = {
    'job_id' : job.id,
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
  }

  #TODO: Check if result is not yet done before aggregation
  #http://python-rq.org/docs/ --> need to wait a while until the worker is finished
  if task_count == done_task_count:
    utils.setRedisKV(redis_conn, unique_id, "finished")

    # Retrieve the next task
    next_tasks = []
    for t in task_graph:
        dest_tasks = task_info['dest']

        is_next_task = False
        for dt in dest_tasks:
            if not t['node_id'] == dt['node_id']:
                continue

            if not t['type'] == dt['type']:
                continue

            if not t['order'] == dt['order']:
                continue

            is_next_task = True
            break
  
        if is_next_task:
            # If all conditions are satisfied, add this to the next task list
            next_tasks.append(t)

    with Connection(redis_conn):
        for next_task in next_tasks:
          #Maybe add a differnetname?
          q = Queue(next_task['node_id'])
          t = q.enqueue(next_task['func'], task_graph, next_task['ref_id'], None, depends_on=job.id) #job is this current job
          metas['agg_task_id'] = t.id

  # TODO Delay

  # Log execution time info to redis
  process_end_time = get_redis_server_time()
  add_exec_time_info(unique_id, "{}-{}".format(task_type_id, task_info['seq_id']), process_start_time, process_end_time)
  log.event("processing", "finished")
  log.exec_time('processing', process_start_time, process_end_time)

  # Log partial results
  log.results(results['aggregated_speeds'], metas=metas)

  return { 'sequence_ID': task_info['seq_id'], 
           'metas' : metas, 
           'output': results, 
           'outsize': len(results['aggregated_speeds'])}

def aggregate_average_speeds(task_graph, reference_id, params):
  # Resolve task info and parameters
  task_info     = task_graph[reference_id]
  unique_id     = task_info['unique_id']
  node_id       = task_info['node_id']

  # Notify queue of job pick up
  job = rq.get_current_job()
  notify_job_pick_up(job, unique_id, task_info)

  # Log the beginning of processing event
  log = MqttLog(node_id, unique_id)
  log.event('aggregation', 'started')
  aggregation_start_time = get_redis_server_time()
  tic = time.clock()

  # Add handled job to execution list
  notify_job_exec_start(job, unique_id, node_id, task_info)

  # TODO Get status of the task processing
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  status = utils.getRedisV(redis_conn, unique_id)

  if status == "finished":
    with Connection(redis.from_url(REDIS_URL)):
      # Get a list of task ids
      task_id_list_all = utils.getListK(redis_conn, R_TASKS_LIST.format(unique_id))

      # Resolve information about each task given their task ids
      task_list = []
      for task_id in task_id_list_all:
        json_task_info = utils.getRedisV(redis_conn, R_TASK_INFO.format(unique_id, task_id))
        finished_task = json.loads(json_task_info)

        # Check if the finished task directly targets this node
        for dest_task in finished_task['dest']:
          if dest_task['node_id'] == task_info['node_id'] and \
             dest_task['type'] == task_info['type'] and \
             dest_task['order'] == task_info['order']:

            task_list.append(finished_task)
            break

      # Create a list connecting task ids to their assigned queue ids
      task_to_queue_list = [ { 'task_id' : t['task_id'], 'queue_id' : t['queue_id'] } for t in task_list ]

      #Checking sequence just in case, but costs another for loop
      agg_result = {}
      for queued_task in task_to_queue_list:
        q = Queue(queued_task['queue_id'])
        task = q.fetch_job(queued_task['task_id'])

        if task is not None:
          sequence_ID = task.result["sequence_ID"]

          for result in task.result["output"]["aggregated_speeds"]:
            rsu_id = str(result[0])
            # direction = str(result[1])
            speed = float(result[1])
            count = int(result[2])

            if not rsu_id in agg_result:
              agg_result[rsu_id] = {'speed' : 0, 'count' : 0}

            agg_result[rsu_id]['speed'] += speed
            agg_result[rsu_id]['count'] += count


      for rsu_id in agg_result.keys():
        speed_info = agg_result[rsu_id]
        agg_result[rsu_id] = speed_info['speed'] / speed_info['count']

      d = { 'result': agg_result,
            'unique_id': unique_id,
            # 'done_task_count': done_task_count,
            'node_task_id_list': task_to_queue_list }

      toc = time.clock()
      job.meta['progress'] = toc - tic
      job.save_meta()

      # Log execution time info to redis
      aggregation_end_time = get_redis_server_time()
      add_exec_time_info(unique_id, "aggregation", aggregation_start_time, aggregation_end_time)
      log.event('aggregation', 'finished')
      log.exec_time('aggregation', aggregation_start_time, aggregation_end_time)

      # Log aggregation results
      print("Logging results...")
      log.results(d, subtype='aggregation')
      print("Done.")

      return d

###
##    S0002: Utility Functions
###
def error_result(message):
  return { 'unique_id': unique_id, 
           'metas' : { 'status' : 'failed', 'message' : message }  }

def query_influx_db(start, end, fields="*",
                                influx_db='IFoT-GW2',
                                influx_ret_policy='autogen',
                                influx_meas='IFoT-GW2-Meas',
                                host='163.221.68.191',
                                port='8086',
                                rsu_id=None):

    # Build the filter clause
    where = ""
    if start < EXPECTED_TIME_RANGE:
      start = int(start) * NANO_SEC_ADJUSTMENT

    if end < EXPECTED_TIME_RANGE:
      end = int(end) * NANO_SEC_ADJUSTMENT

    source = '"{}"."{}"."{}"'.format(influx_db, influx_ret_policy, influx_meas)
    where  = 'WHERE time >= {} AND time <= {}'.format(start, end)
    if rsu_id != None:
      where += " AND rsu_id = '{}'".format(rsu_id)

    query = "SELECT {} from {} {} LIMIT 1000".format(fields, source, where)

    payload = {
        "db": influx_db,
        "pretty": True,
        "epoch": 'ms',
        "q": query
    }

    influx_url = "http://{}:{}/query".format(host, port)
    return requests.get(influx_url, params=payload)

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
    'ref_id'        : task_info['ref_id'],
    'unique_id'     : job.meta['unique_id'],
    'type'          : job.meta['task_type'],
    'index'         : job.meta['task_index'],
    'queue_id'      : job.meta['queue_id'],
    'handled_by'    : job.meta['handled_by'],
    'handled_time'  : job.meta['handled_time'],
    'progress'      : job.meta['progress'],
    'dest'          : task_info['dest'],
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
  utils.appendToListK(redis_conn, redis_task_list_key, job.id )
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



