import redis
import datetime
import os
import multiprocessing
import tempfile
from flask import current_app

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'jpg'])

def generate_unique_ID():
  return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')

def allowed_file(filename):
  return '.' in filename and \
          filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def setRedisKV(r, K, V):
  try:
    # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.set(K, V)
    return True
  except:
    return False

def getRedisV(r, K):
  # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  output = r.get(K)
  if output is not None:
    return output
  else:
    return "Empty"

def appendToListK(r, K, V):
  try:
    # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.rpush(K, V)
    return True
  except:
    return False

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