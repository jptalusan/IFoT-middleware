from flask import current_app

import redis
from rq import Queue, Connection

import json
import time
import multiprocessing
import requests
import numpy as np

from ..api import utils
from ..services.defs import *

def enqueue_classify_task(queue, unique_id, seq_id, model, sensor_list, param_list,
                          columns, values):

  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')

    task = q.enqueue('ActivityRecog_Tasks.classify', unique_id, seq_id,
                        model, sensor_list, param_list,
                        columns, values)

  queue.put(task.get_id())
  return

#TODO: Breakdown the time/duration and then send the unix times to the various nodes.
def classify(request, unique_id=None):
  tic = time.perf_counter()
  req = request.get_json(force=True)
  influx_ip   = req['influx_ip']
  model = req['model']
  sensor_list = req['sensor_list']  # List(string), sensor addresses to use for training
  param_list = req['param_list']    # List(string), params to use
  start_time  = req['start_time']
  end_time    = req['end_time']
  split_count = int(req['split_count'])

  # Obtain a unique ID if not yet assigned
  if unique_id == None:
      unique_id = utils.initialize_query(split_count, count_suffix=TASK_COUNT, done_count_suffix=DONE_TASK_COUNT)

  # Retrieve the data to be classified from Influx DB
  if influx_ip == "" or influx_ip == "DEFAULT":
    influx_ip = 'IFoT-GW2'

  resp = utils.query_influx_db(start_time, end_time, influx_db=influx_ip)

  columns = np.asarray( json.loads(resp.text)['results'][0]['series'][0]['columns'] )
  values = np.asarray( json.loads(resp.text)['results'][0]['series'][0]['values'] )

  # Based on the number of columns we received assess how we would split
  split_rows_count = int(len(values) / split_count) + 1

  task_ids = []
  response = {}
  response['query_ID'] = unique_id
  response['query_received'] = utils.get_current_time()

  start_idx = 0
  end_idx   = split_rows_count
  seq_id    = 0

  processes = []
  mpq = multiprocessing.Queue()
  while start_idx < len(values):
    p = multiprocessing.Process(target=enqueue_classify_task,
                                args=(mpq, unique_id, seq_id, model, sensor_list, param_list,
                                      columns, values[start_idx:end_idx]))

    processes.append(p)
    p.start()

    # Update the sequence id for the next
    seq_id += 1

    # Update our indices
    start_idx = end_idx + 1
    if end_idx + split_rows_count > len(values):
      end_idx = len(values)
      continue

    end_idx += split_rows_count

  for p in processes:
    task = mpq.get()
    task_ids.append(task)

  for p in processes:
    p.join()

  toc = time.perf_counter()
  response_object = {
    'status': 'success',
    'unique_ID': unique_id,
    'params': {
      'start_time'  : start_time,
      'end_time'    : end_time,
      'split_count' : split_count
    },
    'data': {
      'task_id': task_ids
    },
    "benchmarks" : {
      "exec_time" : str(toc - tic),
    }
  }

  # return str(start_time) + ',' + str(end_time) + ',' + feature
  response['response_object'] = response_object

  return response

def train(request, unique_id=None):
  tic = time.perf_counter()
  req = request.get_json(force=True)
  influx_ip   = req['influx_ip']
  start_time  = req['start_time']   # Long Integer, training data start timestamp
  end_time    = req['end_time']     # Long Integer, training data end timestamp
  sensor_list = req['sensor_list']  # List(string), sensor addresses to use for training
  strategy    = req['strategy']     # String,       strategy for distributed training: 'one_per_sensor' (default), 'one_for_all', 'all'
  split_count = req['split_count']  # Integer,      number of training tasks to create; ignored if 'one_per_sensor' is used
  model       = req['model']        # String,       machine learning model to train

  # If we are using the 'one_per_sensor' strategy, then we have to
  #   override the split_count to generate one training task for
  #   each sensor to be used
  if strategy == 'one_per_sensor':
    split_count = len(sensor_list)

  # Obtain a unique ID if not yet assigned
  if unique_id == None:
      unique_id = utils.initialize_query(split_count, count_suffix=TASK_COUNT, done_count_suffix=DONE_TASK_COUNT)

  # Retrieve the data to be classified from Influx DB
  if influx_ip == "" or influx_ip == "DEFAULT":
    influx_ip = 'IFoT-GW2'

  resp = utils.query_influx_db(start_time, end_time, influx_db=influx_ip)

  # Split into columns and values
  columns = np.asarray( json.loads(resp.text)['results'][0]['series'][0]['columns'] )
  values = np.asarray( json.loads(resp.text)['results'][0]['series'][0]['values'] )

  # Prepare the JSON response in advance
  response = {}
  response['query_ID'] = unique_id
  response['query_received'] = utils.get_current_time()

  # Distribute data for training
  task_ids = []
  seq_id = 0
  param_list = [ 'humidity', 'light', 'noise', 'rssi', 'temperature' ]
  for i in range(0, split_count):
    # By default, indicate that all sensor addresses are to be used
    use_sensors = sensor_list

    # Except when using the 'one_per_sensor' strategy
    if strategy == 'one_per_sensor':
      use_sensors = [ sensor_list[i] ]

    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
      q = Queue('default')
      task = q.enqueue( 'ActivityRecog_Tasks.train',
                        unique_id, seq_id, model, use_sensors, param_list,
                        columns, values,
                        "http://163.221.68.242:5001/api/get_training_labels",
                        "http://163.221.68.242:5001/api/upload_classifier" )
      task_ids.append( task.get_id() )
      seq_id += 1

  toc = time.perf_counter()

  # Finalize the response
  response_object = {
    'status': 'success',
    'unique_ID': unique_id,
    'params': {
      'start_time'  : start_time,
      'end_time'    : end_time,
      'split_count' : split_count
    },
    'data': {
      'task_id': task_ids
    },
    "benchmarks" : {
      "exec_time" : str(toc - tic),
    }
  }

  # return str(start_time) + ',' + str(end_time) + ',' + feature
  response['response_object'] = response_object

  return response

