import uuid

from aio_pika import *
import asyncio


async def publisher(message, routing_key=None, exchange_type=None, exchange_name=None,
                    correlation_id=None, loop=None):
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
    :return None if have problem return error
    """
    if not loop:
        loop = asyncio.get_event_loop()
    if routing_key is None:
        return "Routing Key Can not be null"
    # -------------------------------------------------------------------------------------------------
    connection = await connect_robust(
        "amqp://user:example@10.10.10.20:5672/", loop=loop
    )
    channel = await connection.channel()
    # -------------------------------------------------------------------------------------------------
    if exchange_type and exchange_name:
        exchange = await channel.declare_exchange(exchange_name, exchange_type, durable=True)
    else:
        exchange = channel.default_exchange
    # -------------------------------------------------------------------------------------------------
    if correlation_id is None:
        correlation_id_default = uuid.uuid4()
        await exchange.publish(Message(
            bytes(message.encode()),
            correlation_id=correlation_id_default,
        ), routing_key=routing_key)
    else:
        await exchange.publish(Message(
            bytes(message.encode()),
            correlation_id=correlation_id,
        ), routing_key=routing_key)
    # -------------------------------------------------------------------------------------------------
    await connection.close()
