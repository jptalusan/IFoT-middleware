import subprocess
import csv
import time
import socket
# from pandas.compat 

from io import StringIO
import json
import pandas as pd
import redis
import rq
from rq import Queue, Connection
import NUTS_Funcs

class Defs():
  TASK_COUNT = '_task_count'
  DONE_TASK_COUNT = '_done_task_count'
  TASK_SUFFIX = '_task'
  EXEC_TIME_INFO  = 'exec_time_info'

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


def train_classifier(unique_id, sequence_id, labels, label_data, number_of_trees, feature_data):
  process_start_time = get_redis_server_time()
  tic = time.perf_counter()
  job = rq.get_current_job()

  # # Notify queue of job pick up
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())
  job.save_meta()

  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  job = rq.get_current_job()
  NUTS_Funcs.appendToListK(redis_conn, unique_id + Defs.TASK_SUFFIX, job.id )

  # if len(label) > 1:
  label_sampled = pd.read_csv(StringIO(label_data))
  df_Xtrans = pd.read_csv(StringIO(feature_data))

  #label_list = [ "Bathing","ReadingBook","UsingSmartphone","WorkingOnPC","CleaningRoom","CleaningBathroom","PlayingGame","WashingDishes","Eating","Sleeping","StayingAtHome","Dressing","WatchingTelevision","Laundry","PersonalHygiene","UsingToilet","Cooking" ]
  for label in labels:
    label_sample = label_sampled[label].values
    df_label_sample = pd.DataFrame(label_sample, columns = ['label'])
    df_new = pd.concat([df_label_sample, df_Xtrans], axis=1, join_axes=[df_Xtrans.index]).fillna(0)
    pd.DataFrame.to_csv(df_new , path_or_buf="SmartHome/" + label + ".csv", sep=',', na_rep=None, float_format=None, columns=None, header=False, index =False)

    args = ("./rf_exe", "train", label, "-t", str(number_of_trees), "-i", "SmartHome/" + label + ".csv")
    popen = subprocess.Popen(args, stdout=subprocess.PIPE)
    popen.wait()
    # output = popen.stdout.read()
    # print(output)

  # Update job progress
  toc = time.perf_counter()
  job.meta['progress'] = toc - tic
  job.save_meta()

  redis_conn.incr(unique_id + Defs.DONE_TASK_COUNT)
  task_count      = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.TASK_COUNT)
  done_task_count = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.DONE_TASK_COUNT)

  metas = {
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
  }

  # if task_count == done_task_count:
  #   NUTS_Funcs.setRedisKV(redis_conn, unique_id, "finished")
  #   with Connection(redis_conn):
  #     #Maybe add a differnetname?
  #     q = Queue('aggregator')
  #     t = q.enqueue('tasks.aggregate_decision_trees', unique_id, depends_on=job.id) #job is this current job
  #     metas['agg_task_id'] = t.id

  add_exec_time_info(unique_id, "processing-{}".format(sequence_id), process_start_time, get_redis_server_time())

  return { 'sequence_ID': sequence_id, 'metas' : metas, 'output': 'success', 'outsize': 0}

def classify_data(unique_id, sequence_id, label, input_csv_data):
  process_start_time = get_redis_server_time()
  tic = time.perf_counter()
  job = rq.get_current_job()

  # # Notify queue of job pick up
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())
  job.save_meta()

  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  job = rq.get_current_job()
  NUTS_Funcs.appendToListK(redis_conn, unique_id + Defs.TASK_SUFFIX, job.id )

  # ./rf_exe classify Bathing -i test.csv
  f = open('test.csv','w')
  f.write(input_csv_data) #Give your csv text here.
  f.write('\n')
  ## Python will convert \n to os.linesep
  f.close()

  args = ("./rf_exe", "classify", label, "-i", "test.csv")
  popen = subprocess.Popen(args, stdout=subprocess.PIPE)
  popen.wait()
  # output = popen.stdout.read()
  # print(output)

  with open('output.csv', 'r') as myfile:
    data = myfile.read().replace('\n', ';')

  results = {
      'aggregate_decision_trees' : data,
  }

  # Update job progress
  toc = time.perf_counter()
  job.meta['progress'] = toc - tic
  job.save_meta()

  redis_conn.incr(unique_id + Defs.DONE_TASK_COUNT)
  task_count      = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.TASK_COUNT)
  done_task_count = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.DONE_TASK_COUNT)

  metas = {
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
  }

  if task_count == done_task_count:
    NUTS_Funcs.setRedisKV(redis_conn, unique_id, "finished")
    with Connection(redis_conn):
      #Maybe add a differnetname?
      q = Queue('aggregator')
      t = q.enqueue('tasks.aggregate_decision_trees', unique_id, depends_on=job.id) #job is this current job
      metas['agg_task_id'] = t.id

  add_exec_time_info(unique_id, "processing-{}".format(sequence_id), process_start_time, get_redis_server_time())

  print(results)

  return { 'sequence_ID': sequence_id, 'metas' : metas, 'output': results, 'outsize': len(results['aggregate_decision_trees'])}

