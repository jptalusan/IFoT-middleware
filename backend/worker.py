import redis
from rq import Worker, Connection
import time

REDIS_URL = 'redis://redis:6379/0'
QUEUES = ['default']


redis_connection = redis.from_url(REDIS_URL) 
if __name__ == '__main__':
  with Connection(redis_connection):
    worker = Worker(QUEUES)
    worker.work()
