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

#this might be slower, and i should probably put the tasks inside the worker.py
#or can I just preload the libraries there?

def get_current_time():
  HERE = tz.gettz('Asia/Tokyo')
  UTC = tz.gettz('UTC')

  ts = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(HERE)
  # local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
  local_time = ts.strftime('%Y-%m-%d %H:%M:%S %Z')
  return local_time

def create_task(task_type):
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
  d = '123123134131234  erawdadadafaqerq23131e3'
  return d

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

def classify_iris_dist(input_data, nodes):
  tic = time.clock()

  input_dataIO = StringIO(input_data.decode("utf-8"))
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
