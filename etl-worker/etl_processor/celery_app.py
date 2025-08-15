import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# Create Celery instance
app = Celery(
    'etl_processor',
    broker=os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0'),
    backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/1'),
    include=['etl_processor.simple_tasks'] # Use simple tasks for testing
)

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    # Set worker hostname
    worker_hostname='etl-worker@%h'
)

if __name__ == '__main__':
    app.start()