from flask import current_app

import redis
from rq import Queue, Connection

import os
import json
import time as tm
import datetime
import multiprocessing
import requests

#import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import PCA

from ..api import utils, metas
from ..services.defs import *
from ..main.lynx.influxdb.client import LynxClient

def classify(request, unique_id=None):
  tic = tm.perf_counter()

  # Load the parameters
  subtask_start_time = utils.get_redis_server_time()
  req = request.get_json(force=True)
  influx_ip   = req['influx_ip']
  start_time  = datetime.datetime.fromtimestamp(int(req['start_time']))
  end_time    = datetime.datetime.fromtimestamp(int(req['end_time']))
  node_count  = int(req['node_count'])
  duration    = req['duration']

  # Obtain a unique ID if not yet assigned
  if unique_id == None:
      unique_id = utils.initialize_query(split_count, count_suffix=TASK_COUNT, done_count_suffix=DONE_TASK_COUNT)

  # Convert times to the required format
  fmtd_start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
  fmtd_end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

  # Load label data from Lynx
  lynx = LynxClient(host='ubi-lynx-db.naist.jp',
                  port=443,
                  username='lynxadmin',
                  password='Greenlynx',
                  database='lynx',
                  ssl=True,
                  verify_ssl=True)

  # Log end of collection startup
  metas.add_exec_time_info(unique_id, "collection/startup",
                            subtask_start_time,
                            utils.get_redis_server_time())

  label = lynx.label(fmtd_start_time, fmtd_end_time)
  label_sampled = pd.DataFrame(data=label)
  label_sampled = label_sampled.resample(duration).fillna(method="ffill")
  label_sampled = label_sampled.fillna(0)
  label_sampled = label_sampled.drop(label_sampled.index[0])

  # Load data from Lynx (!! Warning: This will be slow !!)
  print("Retrieving positioning data...", end='')
  pos = lynx.positioning(fmtd_start_time, fmtd_end_time, duration)
  print("done.")

  print("Retrieving door data...", end='')
  door = lynx.enocean_door(fmtd_start_time, fmtd_end_time,duration)
  print("done.")

  print("Retrieving motion data...", end='')
  motion = lynx.enocean_motion(fmtd_start_time, fmtd_end_time, duration)
  print("done.")

  print("Retrieving operation status...", end='')
  opration_status = lynx.echonet_operation_status(fmtd_start_time, fmtd_end_time, duration)
  print("done.")

  print("Retrieving power data...", end='')
  watt = lynx.echonet_power_distribution_board_metering(fmtd_start_time, fmtd_end_time, duration)
  print("done.")
  metas.add_exec_time_info(unique_id, "collection/data-download",
                            subtask_start_time,
                            utils.get_redis_server_time())

  # Consolidate the data
  subtask_start_time = utils.get_redis_server_time()
  time = pd.DataFrame(data={"time": label_sampled.index.to_perioddelta('10s').seconds},
                      index=label_sampled.index)
  df = pd.concat([label_sampled, pos, motion, door, opration_status, watt, time],
                 axis=1, join_axes=[label_sampled.index]).fillna(0)
  df.index = df.index.tz_convert('Asia/Tokyo')

  # Obtain the label list
  label_list = lynx.label_list()
  label_list = label_sampled.columns.tolist()[1:]
  X = df.drop(np.append(label_list, "user"), axis=1).values

  # Apply the scaler
  scaler = MinMaxScaler()       # TODO Save for later
  X_scaled = scaler.fit_transform(X)
  metas.add_exec_time_info(unique_id, "collection/data-preprocessing",
                            subtask_start_time,
                            utils.get_redis_server_time())

  # Apply PCA
  subtask_start_time = utils.get_redis_server_time()
  pca = PCA(n_components=0.95)  # TODO Save for later!
  X_trans = pca.fit_transform(X_scaled)
  metas.add_exec_time_info(unique_id, "collection/data-pca",
                            subtask_start_time,
                            utils.get_redis_server_time())

  # Save as CSV
  subtask_start_time = utils.get_redis_server_time()
  csv_data = pd.DataFrame(X_trans).to_csv(header=False, index=False)

  # TODO Classify with the backend, launching up to N instances
  task_ids = []
  seq_id = 0

  mpq = multiprocessing.Queue()
  processes = []
  for i in range(0, node_count):
    p = multiprocessing.Process(target=enqueue_classify_task,
                                args=(mpq, unique_id, seq_id, "all", csv_data))

    processes.append(p)
    p.start()

    # Update the sequence id for the next
    seq_id += 1

  for p in processes:
    task = mpq.get()
    task_ids.append(task)

  for p in processes:
    p.join()

  metas.add_exec_time_info(unique_id, "collection/distribution",
                            subtask_start_time,
                            utils.get_redis_server_time())

  # TODO Obtain the task ids so that we can return them
  toc = tm.perf_counter()

  # Create the response object
  response_object = {
    'status': 'success',
    'unique_ID': unique_id,
    'params': {
      'start_time'  : start_time,
      'end_time'    : end_time,
      'node_count' : node_count
    },
    'data': {
      'task_id': task_ids
    },
    "benchmarks" : {
      "exec_time" : str(toc - tic),
    }
  }
  response = {}
  response['response_object'] = response_object

  return response


def train(request, unique_id=None):
  tic = tm.perf_counter()

  # Load the parameters
  subtask_start_time = utils.get_redis_server_time()
  req = request.get_json(force=True)
  influx_ip   = req['influx_ip']
  start_time  = datetime.datetime.fromtimestamp(int(req['start_time']))
  end_time    = datetime.datetime.fromtimestamp(int(req['end_time']))
  node_count  = int(req['node_count'])
  tree_count  = int(req['tree_count'])
  print(req.keys())
  duration    = req['duration']

  # Obtain a unique ID if not yet assigned
  if unique_id == None:
      unique_id = utils.initialize_query(split_count, count_suffix=TASK_COUNT, done_count_suffix=DONE_TASK_COUNT)

  # Convert times to the required format
  fmtd_start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
  fmtd_end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

  lynx = LynxClient(host='ubi-lynx-db.naist.jp',
                  port=443,
                  username='lynxadmin',
                  password='Greenlynx',
                  database='lynx',
                  ssl=True,
                  verify_ssl=True)

  # Log end of collection startup
  metas.add_exec_time_info(unique_id, "collection/startup",
                            subtask_start_time,
                            utils.get_redis_server_time())

  # Load data from Lynx (!! Warning: This will be slow !!)
  subtask_start_time = utils.get_redis_server_time()
  print("Retrieving labels...", end='')
  label = lynx.label(fmtd_start_time, fmtd_end_time)
  print("done.")

  print("Retrieving positioning data...", end='')
  pos = lynx.positioning(fmtd_start_time, fmtd_end_time, duration)
  print("done.")

  print("Retrieving door data...", end='')
  door = lynx.enocean_door(fmtd_start_time, fmtd_end_time,duration)
  print("done.")

  print("Retrieving motion data...", end='')
  motion = lynx.enocean_motion(fmtd_start_time, fmtd_end_time, duration)
  print("done.")

  print("Retrieving operation status...", end='')
  opration_status = lynx.echonet_operation_status(fmtd_start_time, fmtd_end_time, duration)
  print("done.")

  print("Retrieving power data...", end='')
  watt = lynx.echonet_power_distribution_board_metering(fmtd_start_time, fmtd_end_time, duration)
  print("done.")

  # Load label data from Lynx
  label_sampled = pd.DataFrame(data=label)
  label_sampled = label_sampled.resample(duration).fillna(method="ffill")
  label_sampled = label_sampled.fillna(0)
  label_sampled = label_sampled.drop(label_sampled.index[0])
  metas.add_exec_time_info(unique_id, "collection/data-download",
                            subtask_start_time,
                            utils.get_redis_server_time())

  # Consolidate the data
  subtask_start_time = utils.get_redis_server_time()
  time = pd.DataFrame(data={"time": label_sampled.index.to_perioddelta(duration).seconds},
                      index=label_sampled.index)
  df = pd.concat([label_sampled, pos, motion, door, opration_status, watt, time],
                 axis=1, join_axes=[label_sampled.index]).fillna(0)
  df.index = df.index.tz_convert('Asia/Tokyo')

  # Obtain the label list
  label_list = lynx.label_list()
  label_list = label_sampled.columns.tolist()[1:]
  X = df.drop(np.append(label_list, "user"), axis=1).values

  # Apply the scaler
  scaler = MinMaxScaler()       # TODO Save for later
  X_scaled = scaler.fit_transform(X)
  metas.add_exec_time_info(unique_id, "collection/data-preprocessing",
                            subtask_start_time,
                            utils.get_redis_server_time())

  # Apply PCA
  subtask_start_time = utils.get_redis_server_time()
  pca = PCA(n_components=0.95)  # TODO Save for later!
  X_trans = pca.fit_transform(X_scaled)
  metas.add_exec_time_info(unique_id, "collection/data-pca",
                            subtask_start_time,
                            utils.get_redis_server_time())

  # Save as CSV
  subtask_start_time = utils.get_redis_server_time()
  csv_label_data = label_sampled.to_csv()
  csv_data = pd.DataFrame(X_trans).to_csv()

  # Train with the backend, launching up to N instances
  task_ids = []
  seq_id = 0

  mpq = multiprocessing.Queue()
  processes = []
  for i in range(0, node_count):
    p = multiprocessing.Process(target=enqueue_train_task,
                                args=(mpq, unique_id, seq_id, label_list, csv_label_data, tree_count, csv_data))

    processes.append(p)
    p.start()

    # Update the sequence id for the next
    seq_id += 1

  for p in processes:
    task = mpq.get()
    task_ids.append(task)

  for p in processes:
    p.join()

  metas.add_exec_time_info(unique_id, "collection/distribution",
                            subtask_start_time,
                            utils.get_redis_server_time())

  toc = tm.perf_counter()

  # Create the response object
  response_object = {
    'status': 'success',
    'unique_ID': unique_id,
    'params': {
      'start_time'  : start_time,
      'end_time'    : end_time,
      'node_count' : node_count
    },
    'data': {
      'task_id': task_ids
    },
    "benchmarks" : {
      "exec_time" : str(toc - tic),
    }
  }
  response = {}
  response['response_object'] = response_object

  return response


#########################
#   Utility Functions   #
#########################
def enqueue_train_task(queue, unique_id, seq_id, labels, label_data, tree_count, csv_data):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')

    #task = q.enqueue('DistRF_Tasks.train_classifier', unique_id, seq_id, tree_count, csv_data)
    task = q.enqueue('SMARTHOME_Tasks.train_classifier', unique_id, seq_id, labels, label_data, tree_count, csv_data)

  queue.put(task.get_id())
  return

def enqueue_classify_task(queue, unique_id, seq_id, label, csv_data):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')

    task = q.enqueue('SMARTHOME_Tasks.classify_data', unique_id, seq_id, label, csv_data)

  queue.put(task.get_id())
  return


