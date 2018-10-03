#Readme, generates X features and y labels for training/classifications from a single chunk of data
#http://ubi-dl1.naist.jp:8888/notebooks/jp/Random_NPY_100Chunk_Generator.ipynb
#used in tandem with this

import os
import glob
import numpy as np
import pandas as pd
from numpy import mean, absolute
from scipy.stats import iqr, kurtosis
import scipy.fftpack
from scipy import signal
from scipy.ndimage.filters import gaussian_filter
import scipy.fftpack
from scipy.stats import entropy
import pickle
from rq import Queue, Connection
from rq import get_current_job
import socket
import time
import datetime
from dateutil import tz
import redis

import NUTS_Funcs

NODE_COUNT = '_node_count'
DONE_NODE_COUNT = '_done_node_count'
NODES = '_nodes'

TARGET_NAMES = ["still", "walk",  "run",  "bike",  "car",  "bus",  "train",  "subway"]
NUM_CLASSES = len(TARGET_NAMES)
window_size = 100
N = 100
feat_name_list = ["acc_x", "acc_y", "acc_z", "acc_comp", 
                  "lacc_x", "lacc_y", "lacc_z", "lacc_comp", 
                  "gra_x", "gra_y", "gra_z", "gra_comp", 
                  "gyr_x", "gyr_y", "gyr_z", "gyr_comp", 
                  "mag_x", "mag_y", "mag_z", "mag_comp", 
                  "ori_w", "ori_x", "ori_y", "ori_z", 
                  "pre"]
comp_ind_pos = [3, 7, 11, 15, 19]

def get_current_time():
  HERE = tz.gettz('Asia/Tokyo')
  UTC = tz.gettz('UTC')

  ts = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(HERE)
  local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z')
  return local_time

def feat_Extract_And_Classify(feat_list, test_list, unique_ID):


  tic = time.clock()
  job = get_current_job()

  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = get_current_time()
  job.save_meta()
  
  redis_connection = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  NUTS_Funcs.appendToListK(redis_connection, unique_ID + NODES, job.id)


  number_of_chunks = 15
  single_chunk = np.random.randint(1, number_of_chunks)

  y = np.fromstring(test_list, dtype='float64')
  
  raw_data_arrays = []
  for feat in feat_list:
    raw_data_arrays.append(np.fromstring(feat, dtype='float64'))

  time_features_array = []
  temp = []
  for raw_data_array in raw_data_arrays:
      time_features_array.append(NUTS_Funcs.time_features_extract(raw_data_array, window_size))

  #Indices of composite features in feat_list

  raw_comp_data_arrays = [ raw_data_arrays[i] for i in comp_ind_pos ]
  raw_comp_name_arrays = [ feat_name_list[i] for i in comp_ind_pos ]

  #From 1000 data points, fft becomes (10, 50) and time becomes (10, 7)
  freq_features_array = []
  for i, raw_comp_data_array in enumerate(raw_comp_data_arrays):
    temp_fft = NUTS_Funcs.filtered_fft(raw_comp_data_arrays[i], N)
    freq_features_array.append(NUTS_Funcs.freq_features_extract(temp_fft))

  X = np.hstack((
          time_features_array[0], time_features_array[1], time_features_array[2],
          time_features_array[3], time_features_array[4], time_features_array[5],
          time_features_array[6], time_features_array[7], time_features_array[8],
          time_features_array[9], time_features_array[10], time_features_array[11],
          time_features_array[12], time_features_array[13], time_features_array[14],
          time_features_array[15], time_features_array[16], time_features_array[17],
          time_features_array[18], time_features_array[19], time_features_array[20],
          time_features_array[21], time_features_array[22], time_features_array[23],
          time_features_array[24], freq_features_array[0], freq_features_array[1],
          freq_features_array[2], freq_features_array[3], freq_features_array[4]
  ))

  #TODO: Support choosing model from UI
  # clf_name = 'models/01_ANN_all_label_all_features_model_100.sav'
  clf_name = 'models/01_SVM_all_label_all_features_model_100.sav'
  loaded_clf = pickle.load(open(clf_name, 'rb'))
  y_pred = loaded_clf.predict(X)

  toc = time.clock()
  job.meta['progress'] = toc - tic
  job.save_meta()

  y_pred_int = [int(i) for i in y_pred.tolist()]
  output = [TARGET_NAMES[x - 1] for x in y_pred_int]

  classification = ''
  for pred in y_pred:
    classification += TARGET_NAMES[int(pred) - 1] + ','

  NUTS_Funcs.incrRedisKV(redis_connection, unique_ID + DONE_NODE_COUNT)
  node_count = NUTS_Funcs.getRedisV(redis_connection, unique_ID + NODE_COUNT)
  done_node_count = NUTS_Funcs.getRedisV(redis_connection, unique_ID + DONE_NODE_COUNT)

  #TODO: Check if result is not yet done before aggregation
  #http://python-rq.org/docs/ --> need to wait a while until the worker is finished
  if node_count == done_node_count:
    NUTS_Funcs.setRedisKV(redis_connection, unique_ID, "finished")
    with Connection(redis_connection):
      #Maybe add a differnetname?
      q = Queue('aggregator')
      t = q.enqueue('tasks.aggregate_nuts_data', unique_ID, depends_on=job.id) #job is this current job

  return { 'sequence_ID': unique_ID, 'output': output, 'outsize': len(output)}

