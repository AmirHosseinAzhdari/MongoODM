from aio_pika import *
import asyncio


# queue = await channel.declare_queue(queue_name)
#
# await queue.bind(exchange, routing_key)

# ---------------------------------------------------

async def publisher(message, queue_name=None, routing_key=None, exchange_type=None, exchange_name=None,
                    correlation_id=None, loop=None):
    """

    """
    if not loop:
        loop = asyncio.get_event_loop()
    connection = await connect_robust(
        "amqp://user:example@10.10.10.20:5672/", loop=loop
    )
    channel = await connection.channel()
    if exchange_type:
        exchange = await channel.declare_exchange(exchange_name, exchange_type, durable=True)
        # queue = await channel.declare_queue(queue_name)
        #
        # await queue.bind(exchange)

    else:
        exchange = channel.default_exchange
        # queue = await channel.declare_queue(queue_name, routing_key=routing_key)
        #
        # await queue.bind(exchange)

    await exchange.publish(Message(
        bytes(message.encode()),
        correlation_id=correlation_id,
    ), routing_key=routing_key)

    await connection.close()