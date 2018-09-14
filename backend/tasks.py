import time
from rq import get_current_job
import socket

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
