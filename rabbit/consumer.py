from aio_pika import *
import asyncio


async def on_Message(message: IncomingMessage):
    async with message.process():
        print(message.body)
        await asyncio.sleep(1)


async def consumer(exchange_name=None, exchange_type=None, queue_name=None, call_back_method=None, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()

    connection = await connect_robust(
        "amqp://user:example@10.10.10.20:5672/", loop=loop
    )
    channel = await connection.channel()

    if exchange_name and exchange_type:
        exchange = await channel.declare_exchange('publisher', exchange_type, durable=True)

        if queue_name is None:
            queue = await channel.declare_queue('default')

        queue = await channel.declare_queue(queue_name)

        await queue.bind(exchange)

        await queue.consume(callback=on_Message)

    else:
        queue = await channel.declare_queue(queue_name)

        await queue.consume(callback=on_Message)
