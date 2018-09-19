import time
from rq import get_current_job
import socket

import pandas as pd
import numpy as np

from sklearn import svm
from sklearn.externals import joblib

from io import StringIO

import time

def create_task(task_type):
  job = get_current_job()
  timer = int(task_type) * 10
  job.meta['handled_by'] = socket.gethostname()
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

  input_dataIO = StringIO(input_data)
  job = get_current_job()
  job.meta['handled_by'] = 'Distributed nodes'
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
