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
import csv
from . import funcs

#Move to constants file.
NODE_COUNT = '_node_count'
DONE_NODE_COUNT = '_done_node_count'
NODES = '_nodes'

main_blueprint = Blueprint('main', __name__,)

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
    if file and funcs.allowed_file(file.filename):
      filename = secure_filename(file.filename)
      os.makedirs(os.path.join(current_app.instance_path, 'htmlfi'), exist_ok=True)
      file.save(os.path.join(current_app.instance_path, 'htmlfi', filename))

      return redirect(url_for('main.uploaded_file', filename=filename))
  return render_template('main/upload.html', form=form, result=None)

@main_blueprint.route('/jp')
def jp():
  return redirect(url_for('api.home'))

#https://stackoverflow.com/questions/19794695/flask-python-buttons
#https://stackoverflow.com/questions/43811779/use-many-submit-buttons-in-the-same-form
@main_blueprint.route('/iris_classifier', methods=['GET', 'POST'])
def iris_classifier():
  form = UploadForm()
  if request.method == 'POST':
    if form.validate_on_submit():
      file = form.file.data
      if file and funcs.allowed_file(file.filename):
        filename = secure_filename(file.filename)
        os.makedirs(os.path.join(current_app.instance_path, 'htmlfi'), exist_ok=True)
        file.save(os.path.join(current_app.instance_path, 'htmlfi', filename))
        print(filename)
        submit_val = request.form['action']
        print(submit_val)
        if submit_val == 'Single':
          return redirect(url_for('main.start_iris_process', filename=filename))
        elif submit_val == 'Distributed':
          return redirect(url_for('main.start_iris_dist_process', filename=filename))
        else:
          pass # unknown
  elif request.method == 'GET':
      return render_template('main/upload.html', form=form, result=None)

#Dist
@main_blueprint.route('/iris_dist_process', methods=['GET', 'POST'])
def start_iris_dist_process():
  # filename = secure_filename(request.args.get("filename"))
  # split(filename, 3)

  filename = secure_filename(request.args.get("filename"))
  redis_conn = redis.from_url(current_app.config['REDIS_URL'])
  print(filename)
  try:
    if filename and funcs.allowed_file(filename):
      u_ID = funcs.generate_unique_ID()
      with open(os.path.join(current_app.instance_path, 'htmlfi', filename)) as f:
        q = Queue(connection = redis_conn)
        nodes = 3
        #Split file into 3
        files = funcs.split(filename, 3)
        
        funcs.setRedisKV(redis_conn, u_ID, 'ongoing')
        funcs.setRedisKV(redis_conn, u_ID + NODE_COUNT, nodes)
        funcs.setRedisKV(redis_conn, u_ID + DONE_NODE_COUNT, 0)

        for file in files:
          data = file.read()
          #Need to decode?
          #Race condition i think.
          task = q.enqueue('tasks.classify_iris_dist', data, nodes, u_ID)

      return 'Classifying Distributedly...'
  except IOError:
    pass
  return "Unable to read file"


@main_blueprint.route('/iris_process', methods=['GET', 'POST'])
def start_iris_process():
  filename = secure_filename(request.args.get("filename"))
  redis_conn = redis.from_url(current_app.config['REDIS_URL'])
  print(filename)
  try:
    if filename and funcs.allowed_file(filename):
      with open(os.path.join(current_app.instance_path, 'htmlfi', filename)) as f:
        data = f.read()
        q = Queue(connection = redis_conn)
        task = q.enqueue('tasks.classify_iris', data)
      return 'Classifying...'
  except IOError:
    pass
  return "Unable to read file"

@main_blueprint.route('/uploads/<filename>')
def uploaded_file(filename):
  return send_from_directory(os.path.join(current_app.instance_path, 'htmlfi'), filename)

@main_blueprint.route('/tasks', methods=['POST'])
def run_task():
  print(dir())
  task_type = request.form['type']
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  u_ID = funcs.generate_unique_ID()
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue()
    funcs.setRedisKV(r, u_ID, 'ongoing')
    funcs.setRedisKV(r, u_ID + NODE_COUNT, 1)
    funcs.setRedisKV(r, u_ID + DONE_NODE_COUNT, 0)

    task = q.enqueue('tasks.create_task', task_type, u_ID)
  response_object = {
      'status': 'success',
      'unique_ID': u_ID,
      'data': {
        'task_id': task.get_id()
    }
  }
  return jsonify(response_object), 202
