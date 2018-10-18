from flask import render_template, jsonify, request, current_app, Blueprint
from flask import send_from_directory, url_for, redirect
from werkzeug.utils import secure_filename
from ..models.models import Node
from ..forms.upload_form import UploadForm
from rq import Queue, Connection
import redis
import os
import json
from rq.registry import StartedJobRegistry, FinishedJobRegistry
import urllib.request, json 
import requests

from ..forms.upload_form import TextForm, Nuts2Form

from ..main import funcs
import numpy as np

import time
import datetime
from dateutil import tz

import multiprocessing

NODE_COUNT = '_node_count'
DONE_NODE_COUNT = '_done_node_count'

api = Blueprint('api', __name__,)

@api.route('/', methods=['GET'])
def home():
  return "{'hello':'world'}"

def get_all_finished_tasks_from(queue_name):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
      # q = Queue(queue_name)
      f_registry = FinishedJobRegistry(queue_name)
      data = {}

      finished_job_ids = f_registry.get_job_ids()
      data['status'] = 'success'
      data['queue_name'] = queue_name
      data['finished'] = {}

      data['finished']['count'] = len(finished_job_ids)
      data['finished']['finished_tasks_ids'] = []
      for finished_job_id in finished_job_ids:
        data['finished']['finished_tasks_ids'].append(finished_job_id)

      return jsonify(data)

def get_all_queued_tasks_from(queue_name):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
      q = Queue(queue_name)
      queued_job_ids = q.job_ids
      data = {}

      data['status'] = 'success'
      data['queue_name'] = queue_name
      data['queued'] = {}

      data['queued']['count'] = len(queued_job_ids)
      data['queued']['queued_tasks_ids'] = []
      for queued_job_id in queued_job_ids:
        data['queued']['queued_tasks_ids'].append(queued_job_id)

      return jsonify(data)

def get_all_running_tasks_from(queue_name):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
      # q = Queue(queue_name)
      registry = StartedJobRegistry(queue_name)
      data = {}

      running_job_ids = registry.get_job_ids()
      data['status'] = 'success'
      data['queue_name'] = queue_name
      data['running'] = {}

      data['running']['count'] = len(running_job_ids)
      data['running']['running_tasks_ids'] = []
      for running_job_id in running_job_ids:
        data['running']['running_tasks_ids'].append(running_job_id)

      return jsonify(data)

def getalltasksID():
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')
    registry = StartedJobRegistry('default')
    f_registry = FinishedJobRegistry('default')
  if q:
    data = {}
    running_job_ids = registry.get_job_ids()
    expired_job_ids = registry.get_expired_job_ids()
    finished_job_ids = f_registry.get_job_ids()
    queued_job_ids = q.job_ids
    data['status'] = 'success'
    data['queue_name'] = 'default' #Make dynamic or parameterized?
    data['running'] = {}
    data['queued'] = {}
    data['expired'] = {}
    data['finished'] = {}

    data['running']['count'] = len(running_job_ids)
    data['running']['running_tasks_ids'] = []
    for running_job_id in running_job_ids:
      data['running']['running_tasks_ids'].append(running_job_id)

    data['queued']['count'] = len(queued_job_ids)
    data['queued']['queued_tasks_ids'] = []
    for queued_job_id in queued_job_ids:
      data['queued']['queued_tasks_ids'].append(queued_job_id)

    data['expired']['count'] = len(expired_job_ids)
    data['expired']['expired_tasks_ids'] = []
    for expired_job_id in expired_job_ids:
      data['expired']['expired_tasks_ids'].append(expired_job_id)

    data['finished']['count'] = len(finished_job_ids)
    data['finished']['finished_tasks_ids'] = []
    for finished_job_id in finished_job_ids:
      data['finished']['finished_tasks_ids'].append(finished_job_id)

    return jsonify(data)
  else:
    return jsonify({'status': 'error'})

@api.route('/getallqueues', methods=['POST'])
def getallqeueues():
    return getalltasksID()

def get_task_status(queue, task_id):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue(queue)
    task = q.fetch_job(task_id)
  if task is not None:
    response_object = {
      'status': 'success',
      'data': {
          'task_id': task.get_id(),
          'task_status': task.get_status(),
          'task_result': task.result,
      }
    }
  else:
    response_object = {'status': 'error, task is None'}
  return jsonify(response_object)

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
  else:
    return jsonify({'status': 'error'})

#https://stackoverflow.com/questions/15182696/multiple-parameters-in-in-flask-approute

@api.route('/task/<queue>/<task_id>', methods=['GET','POST'])
def get_status(queue = None, task_id = None):
  return get_task_status(queue, task_id)


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
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('default')
    registry = StartedJobRegistry('default')
    f_registry = FinishedJobRegistry('default')

    all_task_ids = getalltasksID()
    # if request.method == 'GET':
    response_text = all_task_ids.get_data(as_text=True)

    response_json = all_task_ids.get_json()
    # data = json.load(response_json)
    running_tasks_ids = response_json["running"]["running_tasks_ids"]
    finished_tasks_ids = response_json["finished"]["finished_tasks_ids"]
    queued_tasks_ids = response_json["queued"]["queued_tasks_ids"]

    data = {}

    data['running_tasks'] = []
    for task_id in running_tasks_ids:
      d = {}
      job = q.fetch_job(task_id)
      job.refresh()
      job.meta['result'] = 'null'
      d[task_id] = job.meta
      data['running_tasks'].append(d)

    data['queued_tasks'] = []
    for task_id in queued_tasks_ids:
      d = {}
      job = q.fetch_job(task_id)
      job.refresh()
      job.meta['result'] = 'null'
      d[task_id] = job.meta
      data['queued_tasks'].append(d)

    data['finished_tasks'] = []
    for task_id in finished_tasks_ids:
      d = {}
      job = q.fetch_job(task_id)
      job.refresh()
      job.meta['result'] = job.result
      d[task_id] = job.meta
      data['finished_tasks'].append(d)

  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue('aggregator')

    agg_fin_task_ids = get_all_finished_tasks_from('aggregator')
    temp = agg_fin_task_ids.get_json()
    agg_fin_tasks_ids = temp["finished"]["finished_tasks_ids"]

    agg_que_task_ids = get_all_queued_tasks_from('aggregator')
    temp = agg_que_task_ids.get_json()
    agg_que_tasks_ids = temp["queued"]["queued_tasks_ids"]

    agg_run_task_ids = get_all_running_tasks_from('aggregator')
    temp = agg_run_task_ids.get_json()
    agg_run_tasks_ids = temp["running"]["running_tasks_ids"]

    data['agg_finished_tasks'] = []
    for task_id in agg_fin_tasks_ids:
      d = {}
      job = q.fetch_job(task_id)
      if job is not None:
        job.refresh()
        job.meta['result'] = job.result
        d[task_id] = job.meta
        data['agg_finished_tasks'].append(d)

    data['agg_queued_tasks'] = []
    for task_id in agg_que_tasks_ids:
      d = {}
      job = q.fetch_job(task_id)
      if job is not None:
        job.refresh()
        job.meta['result'] = job.result
        d[task_id] = job.meta
        data['agg_queued_tasks'].append(d)

    data['agg_running_tasks'] = []
    for task_id in agg_run_tasks_ids:
      d = {}
      job = q.fetch_job(task_id)
      if job is not None:
        job.refresh()
        job.meta['result'] = job.result
        d[task_id] = job.meta
        data['agg_running_tasks'].append(d)

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
'''
  try:
    r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.set("msg:hello", "Hello Redis!!!")
    output = r.flushall()
    return "Flushed"
  except Exception as e:
    print(e)
    return e
'''

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

def get_current_time():
  HERE = tz.gettz('Asia/Tokyo')
  UTC = tz.gettz('UTC')

  ts = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(HERE)
  # local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
  local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z')[:-3]
  return local_time

def intializeQuery(total_nodes):
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  u_ID = funcs.generate_unique_ID()
  funcs.setRedisKV(r, u_ID, 'ongoing')
  funcs.setRedisKV(r, u_ID + NODE_COUNT, total_nodes)
  funcs.setRedisKV(r, u_ID + DONE_NODE_COUNT, 0)

  return u_ID

feat_name_list = [
    "acc_x", "acc_y", "acc_z", "acc_comp",
    "lacc_x", "lacc_y", "lacc_z", "lacc_comp",
    "gra_x", "gra_y", "gra_z", "gra_comp",
    "gyr_x", "gyr_y", "gyr_z", "gyr_comp",
    "mag_x", "mag_y", "mag_z", "mag_comp",
    "ori_w", "ori_x", "ori_y", "ori_z", "pre"]

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

      unique_ID = intializeQuery(node_count)
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
      json_response['query_received'] = get_current_time()

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

      unique_ID = intializeQuery(node_count)
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
      json_response['query_received'] = get_current_time()

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
