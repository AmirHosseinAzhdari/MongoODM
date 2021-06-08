from aio_pika import *
import asyncio
import os


async def publisher(message, routing_key=None, exchange_type=None, exchange_name=None,
                    correlation_id=None, loop=None, reply_to=None, broker_url=os.getenv('BROKER_URL'), expiration=None):
    """
    if exchange type is set then declare exchange

    but if its not exchange will be default

    in this method message converts to bytes and encode
    :param message Should be string
    :param routing_key that exchange name should be inserted
    :param correlation_id that if its None it will be generate uuid and send with message
    :param loop its optional to give it loop or not
    :param exchange_type if exchange_type is set exchange name should be set too else it will be default exchange
    :param exchange_name that particular exchange name that you want to declare
    :param broker_url that particular broker_url that you want to declare
    :param reply_to the rpc replay to
    :param expiration message expiration time
    """
    if not loop:
        loop = asyncio.get_event_loop()
    if routing_key is None:
        raise NotImplementedError("Routing Key Can not be null")

    connection = await connect_robust(
        broker_url, loop=loop
    )
    channel = await connection.channel()
    if exchange_type and exchange_name:
        exchange = await channel.declare_exchange(exchange_name, exchange_type, durable=True)
    else:
        exchange = channel.default_exchange
    send_message = Message(
        bytes(message.encode()),
    )
    if expiration is not None:
        send_message.expiration = expiration
    if correlation_id:
        send_message.correlation_id = correlation_id
    if reply_to:
        send_message.reply_to = reply_to
    await exchange.publish(send_message, routing_key=routing_key)
    await connection.close()
