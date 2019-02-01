from flask import render_template, jsonify, request, current_app, Blueprint
from flask import send_from_directory, url_for, redirect
from werkzeug.utils import secure_filename
from ..models.models import Node
from ..forms.upload_form import UploadForm
import redis
from rq import Queue, Connection
from rq.registry import StartedJobRegistry, FinishedJobRegistry

import json
import urllib.request
import requests

# from bs4 import BeautifulSoup

from ..forms.upload_form import TextForm, Nuts2Form

import numpy as np

import os
import csv
import time
import datetime
from dateutil import tz

import multiprocessing

from ..api import metas, utils
from ..services import vas
from ..services import actv_reg
from ..services import dist_rf
# from ..services.defs import *

from ...common.mqtt_utils import MqttLog
from ...common.defs import *

import random
from influxdb import DataFrameClient
from math import radians

api = Blueprint('api', __name__,)

feat_name_list = [
    "acc_x", "acc_y", "acc_z", "acc_comp",
    "lacc_x", "lacc_y", "lacc_z", "lacc_comp",
    "gra_x", "gra_y", "gra_z", "gra_comp",
    "gyr_x", "gyr_y", "gyr_z", "gyr_comp",
    "mag_x", "mag_y", "mag_z", "mag_comp",
    "ori_w", "ori_x", "ori_y", "ori_z", "pre"]

@api.route('/', methods=['GET'])
def home():
  return "{'hello':'world'}"

@api.route('/getallqueues', methods=['POST'])
def getallqeueues():
    return jsonify(metas.get_all_task_ids(current_app.config))

@api.route('/getqueuecount', methods=['POST'])
def getqueuecount():
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue()
  if q:
    return jsonify({'length': len(q)})
  else:
    return jsonify({'length': -1})

@api.route('/checkqueue', methods=['GET', 'POST'])
def checkqueue():
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue()
    if q:
      data = {}
      data['status'] = 'success'
      data['jobs_count'] = len(q)
      data['jobs'] = []
      for job_id in q.job_ids:
        job = q.fetch_job(job_id)
        task_status = job.get_status()
        task_result = job.result
        task_obj = {'task_id': job_id, \
                    'task_status': task_status, \
                    'task_result': task_result}
        data['jobs'].append(task_obj)

      return json.dumps({'response': data})

  return jsonify({'status': 'error'})

#https://stackoverflow.com/questions/15182696/multiple-parameters-in-in-flask-approute

@api.route('/task/<queue>/<task_id>', methods=['GET','POST'])
def get_status(queue = None, task_id = None):
  return jsonify(metas.get_task_status(queue, task_id, current_app.config))


@api.route('/queue_count', methods=['GET', 'POST'])#, 'OPTIONS'])
def queue_count():
  queue_out = {}
  csv_out = str(int(time.time()))
  csv_out += ','
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')
    f_registry = FinishedJobRegistry('default')
    csv_out += str(len(q))
    csv_out += ','
    csv_out += str(len(f_registry.get_job_ids()))
  csv_out += '\n'
  return csv_out

@api.route('/getmetas', methods=['GET', 'POST'])
def getmetas():
  return jsonify(metas.get_meta_info(current_app.config))

@api.route('/get_exec_times', methods=['GET', 'POST'])
def get_exec_times():
  data = {}
  # Get the execution timing info
  data['exec_time_logs'] = metas.get_all_exec_time_logs()
  return jsonify(data)

@api.route('/get_exec_time/<unique_id>', methods=['GET', 'POST'])
def get_exec_time(unique_id):
  data = {}
  data['exec_time_logs'] = { unique_id : metas.get_exec_time_log(unique_id) }
  return jsonify(data)

@api.route('/set_redis', methods=['GET','POST'])
def set_redis():
  try:
    r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.set("msg:hello", "Hello Redis!!!")
    print(r.get("msg:hello"))
    return "Redis inserted"
  except Exception as e:
    print(e)

@api.route('/check_redis/<key>', methods=['GET','POST'])
def check_redis(key):
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  status = r.get(key)
  node_count = r.get(key + NODE_COUNT)
  done_node_count = r.get(key + DONE_NODE_COUNT)

  d = {'id': key, 'status': status, 'node_count': node_count, 'done_node_count': done_node_count}
  if status is not None:
    return jsonify(d)
  else:
    return "Empty"

@api.route('/get_redis', methods=['GET','POST'])
def get_redis():
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  output = r.get("msg:hello")
  if output is not None:
    return output
  else:
    return "Empty"

@api.route('/flush_redis', methods=['GET','POST'])
def flush_redis():
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    queues = ['default', 'aggregator']

    for queue in queues:
      q = Queue(queue)
      f_registry = FinishedJobRegistry(queue)
      finished_job_ids = f_registry.get_job_ids()

      for f_job_id in finished_job_ids:
        j = q.fetch_job(f_job_id)
        j.cleanup(0)

  return "Flushed"

def fix_labels(label):
  #Labels
  #For feature extraction i think
  # %%time
  reshape_label = label.reshape(len(label)//window_size, window_size)

  correct_label = []

  for i in range(reshape_label.shape[0]):
      rows = reshape_label[i, :]
      unique, counts = np.unique(rows, return_counts=True)
      out = np.asarray((unique, counts)).T
      if out[0][1] != float(window_size):
          max_ind = np.argmax(np.max(out, axis=1))
          correct_label.append(out[max_ind, 0])
      elif out[0][1] == float(window_size):
          correct_label.append(out[0][0])

  y = np.array(correct_label)
  print(y.shape)
  return y

TARGET_NAMES = ["still", "walk",  "run",  "bike",  "car",  "bus",  "train",  "subway"]
NUM_CLASSES = len(TARGET_NAMES)
window_size = 100

def enqueue_npy_files(unique_ID, model_type, chunk_list, mp_q):
  label_list = []
  data_list = []
  np_data_arrays = []
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')
    print("Handling chunks: ", chunk_list)
    for chunk in chunk_list:
      filename = 'labels_' + str(chunk) + '.npy'
      path = os.path.join(current_app.instance_path, 'htmlfi/Chunks', filename)
      label = np.load(path)
      label_list.append(label)

      for i, name in enumerate(feat_name_list):
        raw_filename = name + '_' + str(chunk) + '.npy'
        raw_path = os.path.join(current_app.instance_path, 'htmlfi/Chunks', raw_filename)
        np_temp = np.load(raw_path)
        if len(np_data_arrays) == len(feat_name_list):
          np_data_arrays[i].extend([np_temp])
        else:
          np_data_arrays.append([np_temp])

    label_all = np.concatenate(label_list)
    y = fix_labels(label_all)
    y_str = y.tostring()

    for np_data_array in np_data_arrays:
      np_all = np.concatenate(np_data_array)
      all_str = np_all.tostring()
      data_list.append(all_str)

    task = q.enqueue('NUTS_Tasks.feat_Extract_And_Classify', data_list, y_str, model_type, unique_ID)
    response_object = {
        'status': 'success',
        'unique_ID': 'NUTS FEAT EXTRACT',
        'data_list_len': len(data_list[0]),
        'y_str_len': len(y_str),
        'model_type': model_type,
        'chunk_list': chunk_list,
        'data': {
          'task_id': task.get_id()
      }
    }
    mp_q.put(response_object)

#TODO: Fix for moren odes above 3
@api.route('/nuts_classify', methods=['GET', 'POST'])#, 'OPTIONS'])
def nuts_classify():
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')
    #form = TextForm(meta={'csrf_context': request.remote_addr})
    form = TextForm(meta={'csrf_context': 'secret_key'})
    if form.validate_on_submit():
      tic = time.clock()
      node_count = form.node_count.data
      number_of_chunks = form.chunk_count.data
      model_type = form.model_type.data

      if node_count > number_of_chunks:
        #Maybe i should just divide the chunks, no! haha 
        return jsonify({'status':'node_count is greater than chunk_count'}), 616

      unique_ID = utils.initialize_query(node_count)
      total_chunks = 200 #hard coded because i only saved 15 chunks in the server
      #Now need to make everything into lists
      chunks = []
      while len(chunks) != number_of_chunks:
        single_chunk = np.random.randint(0, total_chunks)
        if single_chunk in chunks:
          continue
        else:
          chunks.append(single_chunk)

      div, mod = divmod(number_of_chunks, node_count)

      index = 0
      chunk_lists = []
      node_data_list = []
      node_label_list = []

      for i in range(node_count):
        slice = chunks[index: index + div]
        index += div
        chunk_lists.append(slice)

      for i in range(mod):
        slice = chunks[index: index + div]
        chunk_lists[i].extend(slice)
        index += div

      #For debugging, need 202 return no.
      # return jsonify(chunk_lists[0]), 202

      json_response = {}
      json_response['tasks'] = []
      json_response['query_ID'] = unique_ID
      #json_response['query_received'] = int(time.time())
      json_response['query_received'] = utils.get_current_time()

      out_q = multiprocessing.Queue()
      procs = []
      for chunk_list in chunk_lists:
        p = multiprocessing.Process(target=enqueue_npy_files, args=(unique_ID, model_type, chunk_list, out_q))
        procs.append(p)
        p.start()

      for chunk_list in chunk_lists:
        json_response['tasks'].append(out_q.get())

      for p in procs:
        p.join()

      toc = time.clock()
      json_response['progress'] = toc - tic
      return jsonify(json_response), 202
    elif form.csrf_token.errors:
      return jsonify({'status':'csrf_token_errors'})
    else:
      return jsonify({'status':'error'})

@api.route('/nuts2_classify', methods=['GET', 'POST'])#, 'OPTIONS'])
def nuts2_classify():
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')
    form = Nuts2Form()
    if form.validate_on_submit():
      req = request.json
      tic = time.clock()
      node_count = req['node_count']
      number_of_chunks = req['chunk_count']
      model_type = req['model_type']

      if node_count > number_of_chunks:
        #Maybe i should just divide the chunks, no! haha 
        return jsonify({'status':'node_count is greater than chunk_count'}), 616

      unique_ID = utils.initialize_query(node_count)
      total_chunks = 200 #hard coded because i only saved 15 chunks in the server
      #Now need to make everything into lists
      chunks = []
      while len(chunks) != number_of_chunks:
        single_chunk = np.random.randint(0, total_chunks)
        if single_chunk in chunks:
          continue
        else:
          chunks.append(single_chunk)

      div, mod = divmod(number_of_chunks, node_count)

      index = 0
      chunk_lists = []
      node_data_list = []
      node_label_list = []

      for i in range(node_count):
        slice = chunks[index: index + div]
        index += div
        chunk_lists.append(slice)

      for i in range(mod):
        slice = chunks[index: index + div]
        chunk_lists[i].extend(slice)
        index += div

      #For debugging, need 202 return no.
      # return jsonify(chunk_lists[0]), 202

      json_response = {}
      json_response['tasks'] = []
      json_response['query_ID'] = unique_ID
      #json_response['query_received'] = int(time.time())
      json_response['query_received'] = utils.get_current_time()

      out_q = multiprocessing.Queue()
      procs = []
      for chunk_list in chunk_lists:
        p = multiprocessing.Process(target=enqueue_npy_files, args=(unique_ID, model_type, chunk_list, out_q))
        procs.append(p)
        p.start()

      for chunk_list in chunk_lists:
        json_response['tasks'].append(out_q.get())

      for p in procs:
        p.join()

      toc = time.clock()
      json_response['progress'] = toc - tic
      return jsonify(json_response), 202
    else:
      return jsonify({'status':'error'})

def convertIntToLocalTime(input):
    # METHOD 1: Hardcode zones:
    # from_zone = tz.gettz('UTC')
    # to_zone = tz.gettz('America/New_York')

    # METHOD 2: Auto-detect zones:
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    # 1537323289425,1533650065704
    # 1540263839999000064
    ts = int(input)
    ts /= 1000

    utc = datetime.datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    local = datetime.datetime.strptime(utc, '%Y-%m-%d %H:%M:%S')#.replace(tzinfo=from_zone).astimezone(to_zone)
    return local.strftime('%Y-%m-%d %H:%M:%S')

@api.route('/dt_get_readable', methods=['GET', 'POST'])#, 'OPTIONS'])
def dt_get_readable():
  req = request.get_json(force=True)
  influx_ip = req['influx_ip']

  if influx_ip == '163.221.68.206':
    headers: { 'Accept': 'application/csv' }
    payload = {
      "db": 'bl01_db',
      "pretty": True,
      "epoch": 'ms',
      "q": 'SELECT last(accel_y) from "bl01_db"."autogen"."meas_1"'
    }
    r = requests.get('http://' + influx_ip +':8086/query', params=payload)
    last = json.loads(r.text)
    last_time_int = last["results"][0]["series"][0]["values"][0][0]

    payload = {
      "db": 'bl01_db',
      "pretty": True,
      "epoch": 'ms',
      "q": 'SELECT first(accel_y) from "bl01_db"."autogen"."meas_1"'
    }
    r = requests.get('http://163.221.68.206:8086/query', params=payload)
    first = json.loads(r.text)
    first_time_int = first["results"][0]["series"][0]["values"][0][0]
    return str(convertIntToLocalTime(first_time_int)) + ';' + str(convertIntToLocalTime(last_time_int))
    # return str(first_time_int) + ',' + str(last_time_int)
  elif influx_ip == '163.221.68.191':
    DB = 'IFoT-GW2'
    query = "SELECT first(accel_y) from \"autogen\".\"" + DB + "-Meas\""
    payload = {
    "db": DB,
    "pretty": True,
    "epoch": 'ms',
    "q": query
    }
    #curl -G 'http://163.221.68.191:8086/query?db=IFoT-GW1' --data-urlencode 'q=SELECT last(accel_y) from "autogen"."IFoT-GW1-Meas"'
    r = requests.get('http://163.221.68.191:8086/query?', params=payload)
    #FOR SOPICHA
    print(r.text)
    first = json.loads(r.text)
    first_t = first["results"][0]["series"][0]["values"][0][0]

    #EARLIEST-LATEST date getter
    query = "SELECT last(accel_y) from \"autogen\".\"" + DB + "-Meas\""
    payload = {
    "db": DB,
    "pretty": True,
    "epoch": 'ms',
    "q": query
    }
    #curl -G 'http://163.221.68.191:8086/query?db=IFoT-GW1' --data-urlencode 'q=SELECT last(accel_y) from "autogen"."IFoT-GW1-Meas"'
    r = requests.get('http://163.221.68.191:8086/query?', params=payload)
    last = json.loads(r.text)
    last_t = last["results"][0]["series"][0]["values"][0][0]
    return str(convertIntToLocalTime(first_t)) + ';' + str(convertIntToLocalTime(last_t))
    # return str(first_t) + ',' + str(last_t)

@api.route('/dt_get', methods=['GET', 'POST'])#, 'OPTIONS'])
def dt_get():
  req = request.get_json(force=True)
  influx_ip = req['influx_ip']

  if influx_ip == '163.221.68.206':
    headers: { 'Accept': 'application/csv' }
    payload = {
      "db": 'bl01_db',
      "pretty": True,
      "epoch": 'ms',
      "q": 'SELECT last(accel_y) from "bl01_db"."autogen"."meas_1"'
    }
    r = requests.get('http://' + influx_ip +':8086/query', params=payload)
    last = json.loads(r.text)
    last_time_int = last["results"][0]["series"][0]["values"][0][0]

    payload = {
      "db": 'bl01_db',
      "pretty": True,
      "epoch": 'ms',
      "q": 'SELECT first(accel_y) from "bl01_db"."autogen"."meas_1"'
    }
    r = requests.get('http://163.221.68.206:8086/query', params=payload)
    first = json.loads(r.text)
    first_time_int = first["results"][0]["series"][0]["values"][0][0]
    # return str(convertIntToLocalTime(first_time_int)) + ',' + str(convertIntToLocalTime(last_time_int))
    return str(first_time_int) + ';' + str(last_time_int)
  elif influx_ip == '163.221.68.191':
    DB = 'IFoT-GW2'
    query = "SELECT first(accel_y) from \"autogen\".\"" + DB + "-Meas\""
    payload = {
    "db": DB,
    "pretty": True,
    "epoch": 'ms',
    "q": query
    }
    #curl -G 'http://163.221.68.191:8086/query?db=IFoT-GW1' --data-urlencode 'q=SELECT last(accel_y) from "autogen"."IFoT-GW1-Meas"'
    r = requests.get('http://163.221.68.191:8086/query?', params=payload)
    #FOR SOPICHA
    print(r.text)
    first = json.loads(r.text)
    first_t = first["results"][0]["series"][0]["values"][0][0]

    #EARLIEST-LATEST date getter
    query = "SELECT last(accel_y) from \"autogen\".\"" + DB + "-Meas\""
    payload = {
    "db": DB,
    "pretty": True,
    "epoch": 'ms',
    "q": query
    }
    #curl -G 'http://163.221.68.191:8086/query?db=IFoT-GW1' --data-urlencode 'q=SELECT last(accel_y) from "autogen"."IFoT-GW1-Meas"'
    r = requests.get('http://163.221.68.191:8086/query?', params=payload)
    last = json.loads(r.text)
    last_t = last["results"][0]["series"][0]["values"][0][0]
    # return str(convertIntToLocalTime(first_t)) + ',' + str(convertIntToLocalTime(last_t))
    return str(first_t) + ';' + str(last_t)

def convert_utc_to_epoch(timestamp_string):
    '''Use this function to convert utc to epoch'''
    timestamp = datetime.datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S')
    epoch = int(calendar.timegm(timestamp.utctimetuple()))
    print(epoch)
    return int(epoch) * 1000

def enqueue_heatmap_queries(start_time, end_time, feature, mp_q):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')


    mp_q.put(response_object)

#TODO: Breakdown the time/duration and then send the unix times to the various nodes.
@api.route('/heatmap_trigger', methods=['POST'])#, 'OPTIONS'])
def heatmap_trigger():
  req = request.get_json(force=True)
  influx_ip = req['influx_ip']
  start_time = req['start_time']
  end_time = req['end_time']
  feature = req['feature']

  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')
    json_response = {}
    json_response['query_ID'] = 'unique_ID'
    json_response['query_received'] = utils.get_current_time()

    task = q.enqueue('HEATMAP_Tasks.readSVG', influx_ip, start_time, end_time, feature)
    params = str(start_time) + " to " + str(end_time) + " of feature: " + feature
    response_object = {
      'status': 'success',
      'unique_ID': 'HEATMAP READ SVG',
      'params': params,
      'data': {
        'task_id': task.get_id()
      }
    }
    json_response['response_object'] = response_object

    return jsonify(json_response), 202

@api.route('/get_raw_labels', methods=['GET'])#, 'OPTIONS'])
def get_raw_labels():
  resp = ""
  try:
    f = open("static/raw_labels.csv", "r")
    resp = {"status" : "success", "contents" : f.read()}

  except Exception as e:
    resp = {"status" : "failed", "error" : str(e)}

  return jsonify(resp), 200

@api.route('/get_training_labels', methods=['GET'])#, 'OPTIONS'])
def get_training_labels():
  labels = []

  try:
    # See if we have any files cached
    if os.path.isfile("static/raw_labels-cached.json"):

      cached_last_mod = os.stat("static/raw_labels-cached.json").st_mtime
      ref_last_mod =  os.stat("static/raw_labels.csv").st_mtime

      # If cached file is newer than the reference file, we can
      #  still reuse it
      if cached_last_mod > ref_last_mod:
        resp = ""
        with open("static/raw_labels-cached.json", "r") as f:
          resp = f.read()

        return resp, 200

    fmt_date = "%Y-%m-%d %H:%M.%S"
    date_convert = lambda d: datetime.datetime.strptime(d, fmt_date) - \
                             datetime.timedelta(hours=9)

    with open("static/raw_labels.csv", newline='') as csvf:
      reader =  csv.DictReader(csvf)
      activity_list = []
      for row in reader:
        if not row['Activity'] in activity_list:
          activity_list.append(row['Activity'])

        proper_row = {
          "Activity" : row['Activity'],
          "ActivityId" : activity_list.index(row["Activity"]),
          "End" : date_convert(row['End']).timestamp(),
          "Start" : date_convert(row['Start']).timestamp(),
        }
        labels.append(proper_row)

    # Cache the file
    with open("static/raw_labels-cached.json", "w") as f:
      f.write(json.dumps(labels))

  except Exception as e:
    labels.append({"status" : "failed", "error" : str(e)})

  return json.dumps(labels), 200

@api.route('/uploads/<filename>')
def uploaded_file(filename):
  return send_from_directory("uploads", filename)

@api.route('/upload_classifier', methods=['POST'])#, 'OPTIONS'])
def upload_classifier():
  if 'file' not in request.files:
    return "File not found", 400

  file = request.files['file']
  if file.filename == '':
    return "No filename", 400

  if file:
    filename = secure_filename(file.filename)
    file.save(os.path.join("uploads", filename))

    # TODO Move to a separate thread?
    # Notify other S-Workers of the new classifier file
    last_changed = os.stat("uploads/{}".format(filename)).st_mtime
    unique_id = '{}'.format( int(datetime.datetime.now().timestamp()) )
    download_url = "http://163.221.68.242:5001{}".format( url_for("api.uploaded_file", filename=filename) )
    task_ids = []
    for seq_id in range(0, 16):
      with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('default')

        task = q.enqueue('ActivityRecog_Tasks.download_classifier',
                            unique_id, seq_id,
                            download_url,
                            last_changed )

        task_ids.append( task.get_id() )

    return "Success: {}".format(url_for("api.uploaded_file",
                                        filename=filename)), 200

  return "Failed", 404

@api.route('/get_distance', methods=['POST'])
def get_distance():
    client = DataFrameClient('163.221.68.206', 8086, "rsu_locations")
    result = client.query('select * from "rsu_id_location"."autogen"."rsu_locations";')
    df = result['rsu_locations']
    
    list_of_rsus = request.json['rsu_list']
    aggregator_node = random.choice(list_of_rsus)
    agg_row = df.loc[df['rsu-id'] == aggregator_node]
    agg_lat = radians(float(agg_row['lat'].values[0]))
    agg_lon = radians(float(agg_row['lon'].values[0]))
    agg_point1 = np.asarray([agg_lat, agg_lon])
    response = {}
    distances = []
    for rsu in list_of_rsus:
        row = df.loc[df['rsu-id'] == rsu]
        lat = radians(float(row['lat'].values[0]))
        lon = radians(float(row['lon'].values[0]))
        point2 = np.asarray([lat, lon])
        distance = utils.distance_between_two_points(agg_point1, point2)
        rsu_distance = {}
        rsu_distance[rsu] = distance
        distances.append(rsu_distance)
    response['distances'] = distances
    response['agg_node'] = aggregator_node
    return jsonify(response)
##
##  IFoT Middleware Service APIs
##  - Add APIs for new middleware services here
##  - However, all actual service code should be in the '../services' folder
##
@api.route('/actv_reg/classify', methods=['POST'])
def activity_reg_classify():
  return call_service(actv_reg.classify, request)

@api.route('/actv_reg/train', methods=['POST'])
def activity_reg_train():
  return call_service(actv_reg.train, request)

@api.route('/dist_rf/classify', methods=['POST'])
def dist_rf_classify():
  return call_service(dist_rf.classify, request)

@api.route('/dist_rf/train', methods=['POST'])
def dist_rf_train():
  return call_service(dist_rf.train, request)

@api.route('/vas/get_average_speeds', methods=['GET'])
def get_average_speeds():
  return call_service(vas.get_average_speeds, request)

@api.route('/vas/request_rsu_list', methods=['GET'])
def request_rsu_list():
  return jsonify(vas.get_rsu_list())

##
##  IFoT Middleware Service Management Utility Functions
##
def call_service(service_func, request, split_count=1):
  # # Log the collect start time
  # collect_start_time = utils.get_redis_server_time()

  # Execute the request
  api_resp = service_func(request)

  # Log execution time info to redis
  # collect_end_time = utils.get_redis_server_time()

  # unique_id = api_resp['response_object']['unique_ID']
  # log = MqttLog("SVW-001", unique_id)
  # log.exec_time('collection', collect_start_time, collect_end_time)
  # metas.add_exec_time_info(unique_id, "collection", collect_start_time, collect_end_time)

  return jsonify(api_resp), 202


