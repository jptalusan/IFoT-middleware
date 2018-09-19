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
import multiprocessing
import tempfile


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
      if file and allowed_file(file.filename):
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

#If you read the files, it will be in bytes
def split(infilename, num_chunks=multiprocessing.cpu_count()):
    READ_BUFFER = 2**13
    in_file_size = os.path.getsize(os.path.join(current_app.instance_path, 'htmlfi', infilename))
    print('in_file_size:', in_file_size)
    chunk_size = in_file_size // num_chunks
    print('target chunk_size:', chunk_size)
    files = []
    with open(os.path.join(current_app.instance_path, 'htmlfi', infilename), 'rb', READ_BUFFER) as infile:
      for _ in range(num_chunks):
        temp_file = tempfile.TemporaryFile()
        while temp_file.tell() < chunk_size:
          try:
            temp_file.write(infile.readline())
          except StopIteration:  # end of infile
            break
        temp_file.seek(0)  # rewind
        files.append(temp_file)
    return files

#Dist
@main_blueprint.route('/iris_dist_process', methods=['GET', 'POST'])
def start_iris_dist_process():
  # filename = secure_filename(request.args.get("filename"))
  # split(filename, 3)

  filename = secure_filename(request.args.get("filename"))
  redis_conn = redis.from_url(current_app.config['REDIS_URL'])
  print(filename)
  try:
    if filename and allowed_file(filename):
      with open(os.path.join(current_app.instance_path, 'htmlfi', filename)) as f:
        q = Queue(connection = redis_conn)
        nodes = 3
        #Split file into 3
        files = split(filename, 3)

        for file in files:
          data = file.read()
          q.enqueue('tasks.classify_iris_dist', data, nodes)

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
    if filename and allowed_file(filename):
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
