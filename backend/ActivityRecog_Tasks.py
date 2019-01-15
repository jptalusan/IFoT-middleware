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
from collections import OrderedDict

from sklearn.externals import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from rq import Queue, Connection

class Defs():
  TASK_COUNT = '_task_count'
  DONE_TASK_COUNT = '_done_task_count'
  TASK_SUFFIX = '_task'
  EXEC_TIME_INFO  = 'exec_time_info'
  MODEL_PATHS = {
    'DEFAULT' : 'models/NN-ActivityRecog.sav',
    'random_forest'   : 'models/random_forest__ActivityRecog.sav',
    'neural_network'  : 'models/neural_network__ActivityRecog.sav',
    'svc'             : 'models/svc__ActivityRecog.sav',
    'k_neighbors'     : 'models/k_neighbors__ActivityRecog.sav',
  }
  SCALER_PATHS = {
    'DEFAULT' : 'models/NN-ActivityRecog-Scaler.sav',
  }
  CLASS_MAPPINGS = {
    'DEFAULT' : ['Medium Use', 'No Use', 'Low Use', 'High Use'],
  }
  CLASSIFIER_MAP = {
    "random_forest"  : { "name" : "Random Forest",
                         "clf" : RandomForestClassifier(n_estimators = 1000) },
    "neural_network" : { "name" : "MLP Neural Net I",
                         "clf" : MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(4, 2), max_iter=1000) },
    "svc"            : { "name" : "SVM SVC",
                         "clf" : SVC(kernel='rbf', gamma='auto', C = 1.0) },
    "k_neighbors"    : { "name" : "K-Neighbors",
                         "clf" : KNeighborsClassifier(4) }
  }

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

def classify(unique_id, sequence_id, model_type, sensor_list, param_list, columns, values):
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
  prune_columns = ['gateway', 'sensor_type', 'battery', 'di', 'distance', 'accel_x', 'accel_y', 'accel_z', 'pressure', 'uv', 'heat' ]
  pre_df = pd.DataFrame(values, columns=columns).drop(prune_columns, axis=1)
  pre_df['time'] = pre_df['time'].astype(np.int64)
  ### Remove other address columns
  pre_df = pre_df[pre_df['bt_address'].isin(sensor_list)]
  pre_df.insert(0, 'activity_id', 0)
  ### Construct the merged feature list
  ndf_list = create_merged_feature_list(pre_df, sensor_list, param_list)
  ### Then convert it into a dataframe
  df = pd.DataFrame(ndf_list, columns=generate_columns(sensor_list, param_list))
  # prune_columns = ['time', 'bt_address', 'gateway', 'sensor_type', 'battery', 'di', 'distance', 'accel_x', 'accel_y', 'accel_z', 'pressure', 'uv', 'heat' ]
  # df = pd.DataFrame(values, columns=columns).drop(prune_columns, axis=1)
  ts_list = list(df['time'])

  # Check if model path is valid
  model_path = model_type
  if model_type in Defs.MODEL_PATHS:
    model_path = Defs.MODEL_PATHS[model_type]

  if not os.path.isfile(model_path):
    error = { 'type' : 'ERROR', 'value' : 'Invalid model path: {}'.format(model_path) }
    return { 'sequence_ID': unique_id, 'output': [error], 'outsize': 1, 'model_type': model_type}

  # Load the classifier from the model
  clf = joblib.load(model_path)

  # Load the scaler (if needed)
  # scaler = None
  # scaler_path = Defs.SCALER_PATHS[model_type]
  # if not scaler_path == None:
  #   scaler = joblib.load(scaler_path)
  #   # Apply the scaler onto the data to be classified
  #   df = scaler.transform(df)
  # scaler = StandardScaler()
  # scaler.fit(df)
  # df = scaler.transform(df)

  # Use the classifier to predict

  # Labels are the values we want to predict
  labels = np.array(df['activity_id'])
  features = df.drop('activity_id', axis = 1)
  feature_list = list(features.columns)
  features = np.array(features)

  predictions = pd.DataFrame(clf.predict(features))

  # tdf.insert(0, 'hr_time', pd.to_datetime(temp_df['time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo'))
  predictions['class'] = predictions[0].apply(lambda x: Defs.CLASS_MAPPINGS['DEFAULT'][x])
  pred_val_list = list(predictions[0])
  pred_list = list(predictions['class'])
  # output.insert(0, 'time', tdf)
  #output = output['class'].tolist()

  results = []
  for i in range(0, len(pred_list)):
    results.append( {
      'ts'        : ts_list[i],
      'ts_hr_utc' : dt.datetime.utcfromtimestamp(int(ts_list[i]) / 1000.0).strftime('%Y-%m-%d %H:%M:%S'),
      'ts_hr'     : (dt.datetime.fromtimestamp(int(ts_list[i]) / 1000.0) + dt.timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S'),
      'class'     : pred_list[i],
      'class_val' : pred_val_list[i]
    })

  # Update job progress
  toc = time.clock()
  job.meta['progress'] = toc - tic
  job.save_meta()

  # Increment finished nodes counter
  redis_conn.incr(unique_id + Defs.DONE_TASK_COUNT)
  task_count      = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.TASK_COUNT)
  done_task_count = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.DONE_TASK_COUNT)

  metas = {
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
    'feature_list' : feature_list,
  }

  #TODO: Check if result is not yet done before aggregation
  #http://python-rq.org/docs/ --> need to wait a while until the worker is finished
  if task_count == done_task_count:
    NUTS_Funcs.setRedisKV(redis_conn, unique_id, "finished")
    with Connection(redis_conn):
      #Maybe add a differnetname?
      q = Queue('aggregator')
      t = q.enqueue('tasks.aggregate_activity_classifs', unique_id, depends_on=job.id) #job is this current job
      metas['agg_task_id'] = t.id

  # Log execution time info to redis
  add_exec_time_info(unique_id, "processing-{}".format(sequence_id), process_start_time, get_redis_server_time())

  return { 'sequence_ID': sequence_id, 'metas' : metas, 'output': results, 'outsize': len(results), 'model_type': model_type}

def train(unique_id, sequence_id, model_type, sensor_list, param_list, columns, values, labels_url, upload_url):
  process_start_time = get_redis_server_time()
  tic = time.perf_counter()
  job = rq.get_current_job()

  # Notify queue of job pick up
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())
  job.save_meta()

  results = []

  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Push handled job to the redis execution list for this worker
  NUTS_Funcs.appendToListK(redis_conn, unique_id + Defs.TASK_SUFFIX, job.id )

  # Load the data
  load_data_start_time = get_redis_server_time()

  prune_columns = ['gateway', 'sensor_type', 'battery', 'di', 'distance', 'accel_x', 'accel_y', 'accel_z', 'pressure', 'uv', 'heat' ]
  pre_df = pd.DataFrame(values, columns=columns).drop(prune_columns, axis=1)
  pre_df['time'] = pre_df['time'].apply(lambda x: x[0:10]).astype(int)
  ### Remove other address columns
  pre_df = pre_df[pre_df['bt_address'].isin(sensor_list)]
  ### Label the data based on our label intervals
  label_intervals = request_label_intervals(labels_url)
  pre_df = add_feature_labels(pre_df, label_intervals)
  ### Construct the merged feature list
  ndf_list = create_merged_feature_list(pre_df, sensor_list, param_list)
  ### Then convert it into a dataframe
  df = pd.DataFrame(ndf_list, columns=generate_columns(sensor_list, param_list)).drop(['time'], axis=1)

  add_exec_time_info(unique_id, "processing-load_data-{}".format(sequence_id), load_data_start_time, get_redis_server_time())

  # tdf = pd.DataFrame(values, columns=columns)[['time']]
  # ts_list = list(tdf['time'])

  data_prep_start_time = get_redis_server_time()
  # Create the labels and features
  labels = np.array(df['activity_id'])
  features = df.drop('activity_id', axis = 1)
  feature_list = list(features.columns)
  features = np.array(features)

  # Check if the model type exists
  clf = None
  if not model_type in Defs.CLASSIFIER_MAP.keys():
    error = { 'type' : 'ERROR', 'value' : 'Invalid model type: {}'.format(model_type) }
    return { 'sequence_ID': unique_id, 'output': [error], 'outsize': 1, 'model_type': model_type}

  # TODO Divide the data
  train_features = features
  train_labels = labels
  # train_features, test_features, train_labels, test_labels = train_test_split(features, labels, test_size = 0.25, random_state = 42)

  # Load the classifier
  clf = Defs.CLASSIFIER_MAP[model_type]['clf']

  # Load the scaler
  scaler = StandardScaler()
  scaler.fit(features)
  features = scaler.transform(features)

  add_exec_time_info(unique_id, "processing-data_preparation-{}".format(sequence_id), data_prep_start_time, get_redis_server_time())

  # Use the classifier to predict
  training_start_time = get_redis_server_time()
  clf.fit(train_features, train_labels)
  add_exec_time_info(unique_id, "processing-training-{}".format(sequence_id), training_start_time, get_redis_server_time())

  # Save the classifier to file
  which_sensors = "all"
  if len(sensor_list) == 1:
    which_sensors = sensor_list[0].lower()

  clf_filename = "{}_{}__{}.sav".format(which_sensors,
                                        model_type,
                                        "ActivityRecog")
  joblib.dump(clf, "models/{}".format(clf_filename))

  # Upload the file
  ret, msg = upload_classifier(clf_filename, upload_url)
  if ret != True:
    results.append( { "status" : "failed",
                      "error" : "Failed to upload classifier file: {}".format(msg) } )

  results.append({
    'status'          : 'success',
    'model_type'      : model_type,
    'train_data_size' : len(train_features),
    'filename'        : clf_filename,
    'device'          : os.uname().nodename,
  })
  # for i in range(0, len(pred_list)):
  #   results.append( {
  #     'ts'        : ts_list[i],
  #     'ts_hr_utc' : dt.datetime.utcfromtimestamp(int(ts_list[i]) / 1000).strftime('%Y-%m-%d %H:%M:%S'),
  #     'ts_hr'     : (dt.datetime.fromtimestamp(int(ts_list[i]) / 1000) + dt.timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S'),
  #     'class'     : pred_list[i],
  #     'class_val' : pred_val_list[i]
  #   })

  # Update job progress
  toc = time.clock()
  job.meta['progress'] = toc - tic
  job.save_meta()

  # Increment finished nodes counter
  redis_conn.incr(unique_id + Defs.DONE_TASK_COUNT)
  task_count      = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.TASK_COUNT)
  done_task_count = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.DONE_TASK_COUNT)

  #TODO: Check if result is not yet done before aggregation
  #http://python-rq.org/docs/ --> need to wait a while until the worker is finished
  # if task_count == done_task_count:
  #   NUTS_Funcs.setRedisKV(redis_conn, unique_id, "finished")
  #   with Connection(redis_conn):
  #     #Maybe add a differnetname?
  #     q = Queue('aggregator')
  #     t = q.enqueue('tasks.aggregate_activity_classifs', unique_id, depends_on=job.id) #job is this current job

  metas = {
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
  }

  # Log execution time info to redis
  add_exec_time_info(unique_id, "processing-{}".format(sequence_id), process_start_time, get_redis_server_time())

  return { 'sequence_ID': sequence_id, 'metas' : metas, 'output': results, 'outsize': len(results), 'model_type': model_type}

def download_classifier(unique_id, sequence_id, download_url, remote_file_changed):
  download_start_time = get_redis_server_time()
  tic = time.perf_counter()
  job = rq.get_current_job()

  # Notify queue of job pick up
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())
  job.save_meta()

  results = []

  # Connect to Redis
  redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Push handled job to the redis execution list for this worker
  NUTS_Funcs.appendToListK(redis_conn, unique_id + Defs.TASK_SUFFIX, job.id )

  results = []
  should_download = True
  outfile_name = download_url.rsplit('/', 1)[1]

  try:
    # Check first if we already have the file
    if os.path.isfile('models/{}'.format(outfile_name)):
      local_file_changed = os.stat('models/{}'.format(outfile_name)).st_mtime

      # If remote file changes are older than local file changes,
      #   then we can safely ignore them
      if remote_file_changed < local_file_changed + 10:
        should_download = False

    if should_download:
      # Get the file from the download URL
      resp = requests.get(download_url)

      with open('models/{}'.format(outfile_name), 'wb') as f:
        f.write(resp.content)

    results.append({"status" : "success", "skipped" : str(not should_download)})

  except Exception as e:
    results.append({"status" : "failed", "skipped" : str(not should_download), "error" : str(e)})

  # Update job progress
  toc = time.clock()
  job.meta['progress'] = toc - tic
  job.save_meta()

  # Increment finished nodes counter
  redis_conn.incr(unique_id + Defs.DONE_TASK_COUNT)
  task_count      = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.TASK_COUNT)
  done_task_count = NUTS_Funcs.getRedisV(redis_conn, unique_id + Defs.DONE_TASK_COUNT)

  metas = {
    'unique_id' : unique_id,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
  }

  # Log execution time info to redis
  add_exec_time_info(unique_id, "downloading-{}".format(sequence_id), download_start_time, get_redis_server_time())

  time.sleep(10.0)

  return { 'sequence_ID': sequence_id, 'metas' : metas, 'output': results, 'outsize': len(results) }

###############################################################################
##
##  Utility Functions
##
###############################################################################
def create_blank_row_map(addresses, sensor_columns):
  temp = {}
  for a in addresses:
    temp[a] = {}
    temp[a]['time'] = None
    for c in sensor_columns:
      temp[a][c] = None

  return temp

def row_to_dict(row):
  temp = {}
  temp['activity_id'] = row[0]
  temp['time'] = row[1]
  temp['bt_address'] = row[2]
  temp['humidity'] = row[3]
  temp['light'] = row[4]
  temp['noise'] = row[5]
  temp['rssi'] = row[6]
  temp['temperature'] = row[7]
  return temp

def generate_columns(addresses, sensor_columns):
  cols = []
  print(addresses)
  cols.append('activity_id')
  cols.append('time')
  for a in addresses:
    for s in sensor_columns:
      cols.append("{}_{}".format(a, s))

  return cols

def is_row_complete(row, addresses):
  for addr in addresses:
    for key, value in row[addr].items():
      if value == None:
        is_complete = False
        return False

  return True

def create_merged_feature_list(base_df, addresses, sensor_columns):
  # Define 
  MAX_TIME_DIFF_SECONDS = 300
  MAX_TIME_DIFF_MINUTES = 5

  # Declare loop control variables
  ref_address = None
  ref_ts = None
  ref_actv_id = None
  new_row = create_blank_row_map(addresses, sensor_columns)

  d_stats_rows_ignored = 0
  d_stats_rows_total = 0
  data_row_count = 0

  ndf_list = []

  for r in base_df.values:
    row_data = row_to_dict(r)
    bt_address = row_data['bt_address']
    row_time = dt.datetime.utcfromtimestamp(row_data['time'] / 1000)

    if bt_address not in addresses:
      continue

    # Check if our new row's timestamp is already too far from our 
    #  reference timestamp. If so, we will use this as our new reference 
    #  row.
    if ref_ts != None:
      time_diff = row_time - ref_ts

      if (time_diff.seconds >= MAX_TIME_DIFF_SECONDS):
  #       print("Exceeds time difference limit: {}".format(time_diff))
        ref_address = None
        new_row = create_blank_row_map(addresses, sensor_columns)
        d_stats_rows_ignored += data_row_count - 1

    # Check if we can should use this row as the new reference row
    if (ref_address == None) or (bt_address == ref_address):
      if (bt_address == ref_address):
  #       print("Address rollover")
        d_stats_rows_ignored += data_row_count - 1

      # Set the reference address and timestamps
      ref_address = bt_address
      ref_ts = row_time
      ref_actv_id = row_data['activity_id']

      # Adjust the minute value down to the nearest minute divisible by 5
      adj_minute = int(ref_ts.minute / MAX_TIME_DIFF_MINUTES) * MAX_TIME_DIFF_MINUTES
      ref_ts = ref_ts.replace(minute=adj_minute)

      new_row[bt_address]['time'] = ref_ts

      data_row_count = 0

    new_row[bt_address]['time'] = ref_ts
    # Cycle through all types of sensor column data we are interested in
    for c in sensor_columns:
      # If the new row we are creating does not yet have a value
      #  for this sensor column then we should create it
      if new_row[bt_address][c] == None:
        new_row[bt_address][c] = row_data[c]

    data_row_count += 1
    d_stats_rows_total += 1

    # Check if any column values are unfilled before we save this row
    #  to our new data frame
    if is_row_complete(new_row, addresses):
      # TODO Append to the new data frame
      ndf_row = {}
      for addr in addresses:
        for key, value in new_row[addr].items():
          data_key = "{}_{}".format(addr, key)

          if key == 'time':
  #           value = int(value.timestamp() * 1000)
            continue

          ndf_row[data_key] = value

      ndf_row = OrderedDict(sorted(ndf_row.items(), key=lambda t: t[0]))
      ndf_row = list(ndf_row.values())
      ndf_row.insert(0, int(ref_ts.timestamp() * 1000))
      ndf_row.insert(0, ref_actv_id)

      ndf_list.append(ndf_row)

      ref_address = None
      ref_ts = None
      ref_actv_id = None
      new_row = create_blank_row_map(addresses, sensor_columns)

  print("Done")
  print("Rows Total: {}".format(d_stats_rows_total))
  print("Rows Ignored: {}".format(d_stats_rows_ignored))
  return ndf_list

def get_timestamp_label(ts, labels):
  timestamp = dt.datetime.fromtimestamp(float(ts))
  for l in labels:
    if (l['Start'] <= timestamp) and (l['End'] >= timestamp):
      return l['ActivityId']

  return 'Unknown'

def add_feature_labels(df, labels):
  df.insert(0, 'activity_id', df['time'].apply(get_timestamp_label, args=([labels])))
  return df

def request_label_intervals(url):
  resp = requests.get(url)
  labels = json.loads(resp.text)

  get_local_date = lambda d: dt.datetime.fromtimestamp(d) + dt.timedelta(hours=9)

  for l in labels:
    l['Start'] = get_local_date(l['Start'])
    l['End'] = get_local_date(l['End'])

  return labels

def upload_classifier(filename, url):
  resp = None
  try:
    with open("models/{}".format(filename), "rb") as classifier_file:
      resp = requests.post( url, files={ 'file' : ( filename, classifier_file ) } )

  except Exception as e:
    return False, str(e)

  if resp.status_code != 200:
    return False, "{} : {}".format(str(resp.status_code), resp.text)

  return True, ""

