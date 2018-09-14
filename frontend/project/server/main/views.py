from flask import render_template, jsonify, request, current_app, Blueprint
from flask import send_from_directory, url_for, redirect
from werkzeug.utils import secure_filename
from ..models.models import Node
from ..forms.upload_form import UploadForm
from rq import Queue, Connection
import redis
import os
from project.server.main.tasks import create_task
import json
from rq.registry import StartedJobRegistry, FinishedJobRegistry
import urllib.request, json 
import requests

main_blueprint = Blueprint('main', __name__,)

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'jpg'])

def allowed_file(filename):
  return '.' in filename and \
          filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@main_blueprint.route('/', methods=['GET'])
def home():
  n = Node(name='Master')
  print(n)
  return render_template('main/home.html', debug=n)

@main_blueprint.route('/upload', methods=['GET', 'POST'])
def upload():
  form = UploadForm()
  if form.validate_on_submit():
    file = form.file.data
    if file and allowed_file(file.filename):
      filename = secure_filename(file.filename)

      #os.makedirs(os.path.join(current_app.config['UPLOAD_FOLDER']), exist_ok=True)
      #file.save(os.path.join(current_app.config['UPLOAD_FOLDER'], filename))
      os.makedirs(os.path.join(current_app.instance_path, 'htmlfi'), exist_ok=True)
      file.save(os.path.join(current_app.instance_path, 'htmlfi', filename))

      return redirect(url_for('main.uploaded_file', filename=filename))
  return render_template('main/upload.html', form=form, result=None)

@main_blueprint.route('/uploads/<filename>')
def uploaded_file(filename):
  return send_from_directory(os.path.join(current_app.instance_path, 'htmlfi'), filename)
  #return send_from_directory(current_app.config['UPLOAD_FOLDER'], filename)

@main_blueprint.route('/tasks', methods=['POST'])
def run_task():
  print(dir())
  task_type = request.form['type']
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue()
    task = q.enqueue('tasks.create_task', task_type)
  response_object = {
      'status': 'success',
      'data': {
        'task_id': task.get_id()
    }
  }
  return jsonify(response_object), 202

@main_blueprint.route('/getqueuecount', methods=['POST'])
def getqueuecount():
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue()
  if q:
    return jsonify({'length': len(q)})
  else:
    return jsonify({'length': -1})

@main_blueprint.route('/checkqueue', methods=['GET', 'POST'])
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

@main_blueprint.route('/getallqueues', methods=['POST'])
def getallqeueues():
    return getalltasksID()

def get_task_status(task_id):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue()
    task = q.fetch_job(task_id)
  if task:
    response_object = {
      'status': 'success',
      'data': {
          'task_id': task.get_id(),
          'task_status': task.get_status(),
          'jp': 'test',
          'task_result': task.result,
      }
    }
  else:
    response_object = {'status': 'error'}
  return jsonify(response_object)

@main_blueprint.route('/task/<task_id>', methods=['GET','POST'])
def get_status(task_id):
  return get_task_status(task_id)

@main_blueprint.route('/getmetas', methods=['GET', 'POST'])
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

  return jsonify(data)
  # else:
    # response_json = all_task_ids.get_json()
    # data = json.load(response_json)
    # return 

  #python3
  # with urllib.request.urlopen("http://maps.googleapis.com/maps/api/geocode/json?address=google") as url:
  #   data = json.loads(url.read().decode())
  #   print(data)

'''
Aggregator, add key or something to notify service broker
'''