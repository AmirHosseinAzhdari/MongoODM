import psutil
import os
from base.db.frames_motor.frames import Frame
from aio_pika import connect

MEMORY_MIN = os.getenv('MEMORY_MIN')
BROKER_URL = os.getenv('BROKER_URL')


async def check_memory_status():
    global MEMORY_MIN
    if not MEMORY_MIN:
        MEMORY_MIN = 100
    else:
        MEMORY_MIN = int(MEMORY_MIN)
    memory = psutil.virtual_memory()
    if memory.available < (MEMORY_MIN * 1024 * 1024):
        avail = '{:n}'.format(int(memory.available / 1024 / 1024))
        threshold = '{:n}'.format(MEMORY_MIN)
        raise Exception(
            "{avail} MB available RAM below {threshold} MB".format(
                avail=avail, threshold=threshold)
        )


async def health_check_minor():
    await connect(BROKER_URL)
    await Frame.get_db().list_collection_names({})
    await check_memory_status()
