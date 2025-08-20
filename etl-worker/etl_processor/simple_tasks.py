from etl_processor.celery_app import app
import logging

logger = logging.getLogger(__name__)

@app.task(name='hello_world')
def hello_world():
    logger.info("Hello World task executed!")
    return "Hello from Celery!"

@app.task(name='add_numbers')
def add_numbers(x, y):
    result = x + y
    logger.info(f"Adding {x} + {y} = {result}")
    return result