# import asyncio
#
# from aio_pika import *
#
# from rabbit.publisher import publisher
#
# from rabbit.consumer import consumer
# from rabbit.RPC import RPCClass
#
#
# async def on_Message(message: IncomingMessage):
#     async with message.process():
#         print(message.body)
#         print(message.correlation_id)
#         await asyncio.sleep(1)
#
#
# async def on_message(exchange: Exchange, message: IncomingMessage):
#     async with message.process():
#         print(message)
#         print(message.body)
#         response = "Answer From Rpc"
#
#         await exchange.publish(
#             Message(
#                 body=bytes(response.encode()),
#                 correlation_id=message.correlation_id
#             ), routing_key="Hello World!",
#         )
#
#
# if __name__ == '__main__':
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(
#         RPCClass.rpc_client(message_body="Message From Client", callback_queue="Hello World!", routing_key="Hello",
#                             loop=loop))
#     # loop.run_until_complete(
#     #     publisher(message='Hello Babe :) ;)',
#     #               exchange_name='publisher',
#     #               exchange_type=ExchangeType.DIRECT,
#     #               correlation_id=1234566,
#     #               routing_key="Hello", loop=loop))
#     # loop.run_until_complete(
#     #     consumer(exchange_name="publisher", exchange_type=ExchangeType.DIRECT, queue_name="Hello",
#     #              call_back_method=on_Message, loop=loop))
#     # loop.run_until_complete(
#     #     consumer(exchange_name='publisher', queue_name="Hello", call_back_method=on_Message, loop=loop))
#
#     loop.create_task(RPCClass.rpc_consumer(call_back_method=on_message, queue_name='Hello', loop=loop))
#     loop.run_forever()
from rest_framework import status
from rest_framework.response import Response

from rf.views import APIView


class Test(APIView):

    @staticmethod
    def post(request):
        return Response(status=status.HTTP_200_OK)