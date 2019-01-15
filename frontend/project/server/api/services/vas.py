from flask import current_app

import redis
from rq import Queue, Connection

import time
import multiprocessing

from ..api import utils

def enqueue_average_speed_task(queue, unique_id, seq_id, columns, values, rsu_list):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')

    task = q.enqueue('VASMAP_Tasks.average', unique_id, seq_id, columns, values, rsu_list)

  queue.put(task.get_id())
  return

def enqueue_average_speed_by_rsu_task(queue, unique_id, seq_id, rsu_id, db_info, start_time, end_time):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')

    task = q.enqueue('VASMAP_Tasks.average_by_rsu', unique_id, seq_id, rsu_id, db_info, start_time, end_time)

  queue.put(task.get_id())
  return

def get_average_speeds(request):
  tic = time.perf_counter()
  req = request.get_json(force=True)

  influx_ip   = req['influx_ip']
  rsu_list    = req['rsu_list']
  start_time  = req['start_time']       # Long Integer
  end_time    = req['end_time']         # Long Integer
  split_count = len(rsu_list) # Integer

  # Obtain a unique ID
  unique_id = utils.initialize_query(split_count, count_suffix=TASK_COUNT, done_count_suffix=DONE_TASK_COUNT)

  # Retrieve the data to be classified from Influx DB
  if influx_ip == "" or influx_ip == "DEFAULT":
    influx_ip = '163.221.68.206'

  # Set the influx db info
  db_info = {
    'name' : 'VASMAP',
    'host' : influx_ip,
    'port' : '8086',
    'ret_policy' : 'default_rp',
    'meas' : 'rsu_data',
  }

  task_ids = []
  json_response = {}
  json_response['query_ID'] = unique_id
  json_response['query_received'] = utils.get_current_time()

  seq_id    = 0

  processes = []
  mpq = multiprocessing.Queue()

  for rsu_id in rsu_list:
    p = multiprocessing.Process(target=enqueue_average_speed_by_rsu_task,
                                args=(mpq, unique_id, seq_id, rsu_id,
                                      db_info, start_time, end_time))

    processes.append(p)
    p.start()

    # Update the sequence id for the next
    seq_id += 1

  for p in processes:
    task = mpq.get()
    task_ids.append(task)

  for p in processes:
    p.join()

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
  response = {}
  response['response_object'] = response_object

  return response

