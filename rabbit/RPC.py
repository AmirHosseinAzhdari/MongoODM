import uuid
from functools import partial

from aio_pika import *
import asyncio

class RPCClass:

    @staticmethod
    async def rpc_consumer(exchange_type=None, queue_name=None, call_back_method=None, loop=None):
        connection = await connect(
            "amqp://user:example@10.10.10.20:5672/", loop=loop
        )

        channel = await connection.channel()

        queue = await channel.declare_queue(queue_name)

        await queue.consume(partial(
            call_back_method, channel.default_exchange)
        )

    @staticmethod
    async def rpc_client(message_body,routing_key=None, callback_queue=None, loop=None):
        connection = await connect(
            "amqp://user:example@10.10.10.20:5672/", loop=loop
        )

        channel = await connection.channel()

        correlation_id = str(uuid.uuid4())

        await channel.default_exchange.publish(
            Message(
                body=str(message_body).encode(),
                content_type="application/json",
                correlation_id=correlation_id,
                reply_to=callback_queue,
                content_encoding='utf-8',
                delivery_mode=DeliveryMode.NOT_PERSISTENT,

            ),
            routing_key=routing_key,
        )

        await connection.close()
