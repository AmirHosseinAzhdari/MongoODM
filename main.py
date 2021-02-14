import asyncio

from aio_pika import *

from rabbit.publisher import publisher

from rabbit.consumer import consumer

# async def on_Message(message: IncomingMessage):
#     async with message.process():
#         print(message.body)
#         await asyncio.sleep(1)


# if __name__ == '__main__':
loop = asyncio.get_event_loop()
loop.run_until_complete(
    publisher(message='Hello Babe :) ;)',
              exchange_name='publisher',
              exchange_type=ExchangeType.DIRECT,
              routing_key="Hello", loop=loop))
# loop.run_until_complete(
#     consumer(exchange_name="publisher", exchange_type=ExchangeType.DIRECT, queue_name="Hello", loop=loop))
# loop.run_until_complete(
#     consumer(queue_name="Hello", loop=loop))
loop.run_forever()
