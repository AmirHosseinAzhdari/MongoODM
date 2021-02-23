from aio_pika import *
import asyncio


async def consumer(exchange_name=None, exchange_type=None, queue_name=None, loop=None):
    """
    :param exchange_name: optional to use but if no value sets its will be declare default exchange
    :param exchange_type: optional to use
    :param queue_name:if no value sets its will be declare queue with name default
    :param loop: its optional to use if its not set its will be create it self (asyncio.get_event_loop())
    :return: its return one by one
    """
    # -------------------------------------------------------------------------------------------------
    if not loop:
        loop = asyncio.get_event_loop()
    if queue_name is None:
        return "Queue name can not be null"
    # -------------------------------------------------------------------------------------------------
    connection = await connect_robust(
        "amqp://user:example@10.10.10.20:5672/", loop=loop
    )
    channel = await connection.channel()
    # -------------------------------------------------------------------------------------------------
    if exchange_name and exchange_type:
        exchange = await channel.declare_exchange(exchange_name, exchange_type, durable=True)
        if queue_name is None:
            queue = await channel.declare_queue('default')
        else:
            queue = await channel.declare_queue(queue_name)

        await queue.bind(exchange)

        incoming_message = await queue.get(timeout=1)
        await incoming_message.ack()
        return incoming_message
    # -------------------------------------------------------------------------------------------------

    else:
        exchange = channel.default_exchange

        if queue_name is None:
            queue = await channel.declare_queue('default')
        else:
            queue = await channel.declare_queue(queue_name)
    # -------------------------------------------------------------------------------------------------
        await queue.bind(exchange)

        incoming_message = await queue.get(timeout=1)
        await incoming_message.ack()
        return incoming_message
    # -------------------------------------------------------------------------------------------------
