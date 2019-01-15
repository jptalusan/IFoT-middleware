import os
import redis
import rq
import time
import socket
import json
import requests
import pandas as pd
import numpy as np
import NUTS_Funcs
import datetime as dt
from rq import Queue, Connection

class Defs():
  TASK_COUNT = '_task_count'
  DONE_TASK_COUNT = '_done_task_count'
  TASK_SUFFIX = '_task'
  EXEC_TIME_INFO  = 'exec_time_info'

  NANO_SEC_ADJUSTMENT = 1000000000
  EXPECTED_TIME_RANGE = NANO_SEC_ADJUSTMENT ** 2

def get_redis_server_time():
  # Start a redis connection
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  sec, microsec = r.time()
  return ((sec * 1000000) + microsec)

def add_exec_time_info(unique_id, operation, time_start, time_end):
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Add the unique_id to the execution time info list if it does not yet exist
  r.sadd(Defs.EXEC_TIME_INFO, unique_id)

  # Push an operation to the execution time log for this unique_id
  log_obj = {
    'operation'   : operation,
    'start_time'  : str(time_start),
    'end_time'    : str(time_end),
    'duration'    : str(float(time_end - time_start) / 1000000.0),
  }
  r.lpush("{}_{}".format(Defs.EXEC_TIME_INFO, unique_id), json.dumps(log_obj))

  return True

def average(unique_id, sequence_id, columns, values, rsu_list):
  process_start_time = get_redis_server_time()
  tic = time.perf_counter()
  job = rq.get_current_job()

  # Notify queue of job pick up
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())
  job.save_meta()

  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Push handled job to the redis execution list for this worker
  NUTS_Funcs.appendToListK(redis_conn, unique_id + Defs.TASK_SUFFIX, job.id )

  # Load the data
  df = pd.DataFrame(values, columns=columns)

  # Filter only the RSU IDs we want
  df = df[df['rsu_id'].isin(rsu_list)]

  # Get the sum of all speeds by RSU ID how many were summed in each step
  agg_speeds = df.groupby(['rsu_id'], as_index=False)['speed'].sum()
  agg_speeds['count'] = df.groupby(['rsu_id'], as_index=False)['speed'].count()['speed']

  results = {
    'aggregated_speeds' : agg_speeds.values.tolist(),
  }

  # Update job progress
  toc = time.clock()
  job.meta['progress'] = toc - tic
  job.save_meta()

  # Increment finished nodes counter
  redis_conn.incr(unique_id + Defs.DONE_TASK_COUNT)
  task_count      = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.TASK_COUNT)
  done_task_count = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.DONE_TASK_COUNT)

  # Create metadata
  metas = {
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
  }

  #TODO: Check if result is not yet done before aggregation
  #http://python-rq.org/docs/ --> need to wait a while until the worker is finished
  if task_count == done_task_count:
    NUTS_Funcs.setRedisKV(redis_conn, unique_id, "finished")
    with Connection(redis_conn):
      #Maybe add a differnetname?
      q = Queue('aggregator')
      t = q.enqueue('tasks.aggregate_average_speeds', unique_id, depends_on=job.id) #job is this current job
      metas['agg_task_id'] = t.id

  # Log execution time info to redis
  add_exec_time_info(unique_id, "processing-{}".format(sequence_id), process_start_time, get_redis_server_time())

  return { 'sequence_ID': sequence_id, 'metas' : metas, 'output': results, 'outsize': len(results['aggregated_speeds'])}

def average_by_rsu(unique_id, sequence_id, rsu_id, db_info, start_time, end_time):
  process_start_time = get_redis_server_time()
  tic = time.perf_counter()
  job = rq.get_current_job()

  # Notify queue of job pick up
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())
  job.save_meta()

  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Push handled job to the redis execution list for this worker
  NUTS_Funcs.appendToListK(redis_conn, unique_id + Defs.TASK_SUFFIX, job.id )

  # Retrieve the data from the InfluxDB
  resp = query_influx_db( start_time, end_time,
                          host=db_info['host'],
                          port=db_info['port'],
                          influx_db=db_info['name'],
                          influx_ret_policy=db_info['ret_policy'],
                          influx_meas=db_info['meas'],
                          rsu_id=rsu_id )

  # Split into columns and values
  columns = json.loads(resp.text)['results'][0]['series'][0]['columns']
  values =  json.loads(resp.text)['results'][0]['series'][0]['values']

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
  job.meta['progress'] = toc - tic
  job.save_meta()

  # Increment finished nodes counter
  redis_conn.incr(unique_id + Defs.DONE_TASK_COUNT)
  task_count      = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.TASK_COUNT)
  done_task_count = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.DONE_TASK_COUNT)

  # Create metadata
  metas = {
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
  }

  #TODO: Check if result is not yet done before aggregation
  #http://python-rq.org/docs/ --> need to wait a while until the worker is finished
  if task_count == done_task_count:
    NUTS_Funcs.setRedisKV(redis_conn, unique_id, "finished")
    with Connection(redis_conn):
      #Maybe add a differnetname?
      q = Queue('aggregator')
      t = q.enqueue('tasks.aggregate_average_speeds', unique_id, depends_on=job.id) #job is this current job
      metas['agg_task_id'] = t.id

  # Log execution time info to redis
  add_exec_time_info(unique_id, "processing-{}".format(sequence_id), process_start_time, get_redis_server_time())

  return { 'sequence_ID': sequence_id, 'metas' : metas, 'output': results, 'outsize': len(results['aggregated_speeds'])}

def query_influx_db(start, end, fields="*",
                                influx_db='IFoT-GW2',
                                influx_ret_policy='autogen',
                                influx_meas='IFoT-GW2-Meas',
                                host='163.221.68.191',
                                port='8086',
                                rsu_id=None):

    # Build the filter clause
    where = ""
    if start < Defs.EXPECTED_TIME_RANGE:
      start = int(start) * Defs.NANO_SEC_ADJUSTMENT

    if end < Defs.EXPECTED_TIME_RANGE:
      end = int(end) * Defs.NANO_SEC_ADJUSTMENT

    source = '"{}"."{}"."{}"'.format(influx_db, influx_ret_policy, influx_meas)
    where  = 'WHERE time >= {} AND time <= {}'.format(start, end)
    if rsu_id != None:
      where += " AND rsu_id = '{}'".format(rsu_id)

    query = "SELECT {} from {} {}".format(fields, source, where)

    payload = {
        "db": influx_db,
        "pretty": True,
        "epoch": 'ms',
        "q": query
    }

    influx_url = "http://{}:{}/query".format(host, port)
    return requests.get(influx_url, params=payload)

