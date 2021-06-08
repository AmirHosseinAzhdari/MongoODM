import asyncio
import json
import uuid
import os
import datetime

from aio_pika.message import ReturnedMessage, IncomingMessage

from .consumer import consumer
from .publisher import publisher


class RPCClass:

    def __init__(self, loop=None, broker_url=os.getenv('BROKER_URL'), exchange_type=None, exchange_name=None,
                 queue_name=os.getenv('QUEUE_NAME'), routing_key=None, durable=False):
        self.connection = None
        self.channel = None
        self.queue_name = queue_name
        if not self.queue_name.__contains__('rpc'):
            self.queue_name += '_rpc'
        self.exchange_type = exchange_type
        self.broker_url = broker_url
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.durable = durable
        self.futures = {}
        self.loop = loop
        self.connected = False

    async def connect(self, loop=None):
        if self.connected:
            return
        if not self.loop and not loop:
            raise Exception()
        if not self.loop:
            self.loop = loop
        self.connection = await consumer(loop=self.loop, callback=self.on_response, queue_name=self.queue_name,
                                         durable=True,
                                         exchange_name=self.exchange_name, exchange_type=self.exchange_type,
                                         broker_url=self.broker_url)
        self.connected = True

    async def on_response(self, message: IncomingMessage):
        try:
            future = self.futures.pop(message.correlation_id, None)
            if not future or future.done():
                return
            async with message.process():
                future.set_result(message.body)
        except:
            message.nack()

    async def wait_response(self, future, correlation_id):
        count = 1
        while count < 11 and not future.done():
            await asyncio.sleep(0.1 * count)
            count += 1
        if not future.done():
            self.futures.pop(correlation_id, None)
            future.set_result('{"op": "error", "code": 1}')
            return '{"op": "error", "code": 1}'
        else:
            return future.result()

    async def call(self, target, op='get', model=None, key=None, value=None, query=None):
        correlation_id = str(uuid.uuid4())
        future = self.loop.create_future()
        self.futures[correlation_id] = future
        data = {
            'op': op,
            'model': model,
            'key': key,
            'value': value,
            'query': query
        }
        data = json.dumps(data)
        expiration = datetime.datetime.now() + datetime.timedelta(seconds=5)
        await publisher(data, routing_key=target, reply_to=self.queue_name, loop=self.loop,
                        exchange_type=self.exchange_type, exchange_name=self.exchange_name,
                        expiration=expiration, correlation_id=correlation_id, broker_url=self.broker_url)

        return json.loads(await self.wait_response(future, correlation_id))
