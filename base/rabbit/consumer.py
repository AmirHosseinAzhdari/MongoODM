from aio_pika import *
import asyncio
import os
from CommodityMS.settings import QUEUE_NAME


async def consumer(exchange_name=None, exchange_type=None, queue_name=QUEUE_NAME, durable=False,
                   callback=None, broker_url=os.getenv('BROKER_URL'), loop=None):
    """
    :param exchange_name: optional to use but if no value sets its will be declare default exchange
    :param exchange_type: optional to use
    :param queue_name:if no value sets its will be declare queue with name default
    :param callback: the callback method
    :param durable: durable queue
    :param broker_url: if not given, the default broker url is selected
    :param loop: its optional to use if its not set its will be create it self (asyncio.get_event_loop())
    """
    if not loop:
        loop = asyncio.get_running_loop()
    if queue_name is None:
        raise NotImplementedError('Queue name cant be empty')
    if not callback:
        raise NotImplementedError('callback cant be empty')

    connection = await connect_robust(
        broker_url, loop=loop
    )
    channel = await connection.channel()
    queue = await channel.declare_queue(queue_name, durable=durable)

    if exchange_name and exchange_type:
        exchange = await channel.declare_exchange(exchange_name, exchange_type, durable=True)
        await queue.bind(exchange)
    await queue.consume(callback=callback)
