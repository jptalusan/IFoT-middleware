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

def classify(request):
  tic = tm.perf_counter()

  # Load the parameters
  req = request.get_json(force=True)
  influx_ip   = req['influx_ip']
  start_time  = datetime.datetime.fromtimestamp(int(req['start_time']))
  end_time    = datetime.datetime.fromtimestamp(int(req['end_time']))
  node_count = int(req['node_count'])

  # Obtain a unique ID
  unique_id = utils.initialize_query(node_count, count_suffix=TASK_COUNT, done_count_suffix=DONE_TASK_COUNT)

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

  duration = "1h"
  label = lynx.label(fmtd_start_time, fmtd_end_time)
  label_sampled = pd.DataFrame(data=label)
  label_sampled = label_sampled.resample(duration).fillna(method="ffill")
  label_sampled = label_sampled.fillna(0)
  label_sampled = label_sampled.drop(label_sampled.index[0])

  # Load data from Lynx (!! Warning: This will be slow !!)
  pos = lynx.positioning(fmtd_start_time, fmtd_end_time, duration)
  door = lynx.enocean_door(fmtd_start_time, fmtd_end_time,duration)
  motion = lynx.enocean_motion(fmtd_start_time, fmtd_end_time, duration)
  opration_status = lynx.echonet_operation_status(fmtd_start_time, fmtd_end_time, duration)
  watt = lynx.echonet_power_distribution_board_metering(fmtd_start_time, fmtd_end_time, duration)

  # Consolidate the data
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

  # Apply PCA
  pca = PCA(n_components=0.95)  # TODO Save for later!
  X_trans = pca.fit_transform(X_scaled)

  # Save as CSV
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


def train(request):
  tic = tm.perf_counter()

  collect_prepare_start_time = utils.get_redis_server_time()
  # Load the parameters
  req = request.get_json(force=True)
  influx_ip   = req['influx_ip']
  start_time  = datetime.datetime.fromtimestamp(int(req['start_time']))
  end_time    = datetime.datetime.fromtimestamp(int(req['end_time']))
  node_count = int(req['node_count'])
  tree_count = int(req['tree_count'])

  # Obtain a unique ID
  unique_id = utils.initialize_query(node_count, count_suffix=TASK_COUNT, done_count_suffix=DONE_TASK_COUNT)

  # Convert times to the required format
  fmtd_start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
  fmtd_end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
  duration = "1h"

  # Log end of collection preparations
  metas.add_exec_time_info(unique_id, "collection/preparations",
                            collect_prepare_start_time,
                            utils.get_redis_server_time())

  lynx = LynxClient(host='ubi-lynx-db.naist.jp',
                  port=443,
                  username='lynxadmin',
                  password='Greenlynx',
                  database='lynx',
                  ssl=True,
                  verify_ssl=True)

  # Load data from Lynx (!! Warning: This will be slow !!)
  tic1 = tm.perf_counter()
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
  toc1 = tm.perf_counter()
  print("Operation Time: {} secs".format(int(toc1 - tic1)))

  # Load label data from Lynx
  label_sampled = pd.DataFrame(data=label)
  label_sampled = label_sampled.resample(duration).fillna(method="ffill")
  label_sampled = label_sampled.fillna(0)
  label_sampled = label_sampled.drop(label_sampled.index[0])

  # Consolidate the data
  time = pd.DataFrame(data={"time": label_sampled.index.to_perioddelta(duration).seconds},
                      index=label_sampled.index)
  df = pd.concat([label_sampled, pos, motion, door, opration_status, watt, time],
                 axis=1, join_axes=[label_sampled.index]).fillna(0)
  df.index = df.index.tz_convert('Asia/Tokyo')

  # Obtain the label list
  label_list = lynx.label_list()
  X = df.drop(np.append(label_list, "user"), axis=1).values

  # Apply the scaler
  scaler = MinMaxScaler()       # TODO Save for later
  X_scaled = scaler.fit_transform(X)

  # Apply PCA
  pca = PCA(n_components=0.95)  # TODO Save for later!
  X_trans = pca.fit_transform(X_scaled)

  # Save as CSV
  csv_label_data = label_sampled.to_csv()
  csv_data = pd.DataFrame(X_trans).to_csv()

  # Train with the backend, launching up to N instances
  task_ids = []
  seq_id = 0

  mpq = multiprocessing.Queue()
  processes = []
  for i in range(0, node_count):
    p = multiprocessing.Process(target=enqueue_train_task,
                                args=(mpq, unique_id, seq_id, csv_label_data, tree_count, csv_data))

    processes.append(p)
    p.start()

    # Update the sequence id for the next
    seq_id += 1

  for p in processes:
    task = mpq.get()
    task_ids.append(task)

  for p in processes:
    p.join()

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
def enqueue_train_task(queue, unique_id, seq_id, label_data, tree_count, csv_data):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')

    #task = q.enqueue('DistRF_Tasks.train_classifier', unique_id, seq_id, tree_count, csv_data)
    task = q.enqueue('SMARTHOME_Tasks.train_classifier', label_data, tree_count, csv_data)

  queue.put(task.get_id())
  return

def enqueue_classify_task(queue, unique_id, seq_id, label, csv_data):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')

    task = q.enqueue('SMARTHOME_Tasks.classify_data', unique_id, seq_id, label, csv_data)

  queue.put(task.get_id())
  return


