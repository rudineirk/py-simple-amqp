import asyncio

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.asyncio import AsyncioAmqpConnection


async def consumer(msg: AmqpMsg):
    payload = msg.payload
    payload = payload.decode()
    print('msg received: {}'.format(payload))

    # acknowledge that the message was received correctly
    return True


async def main():
    conn = AsyncioAmqpConnection(AmqpParameters())
    channel = conn.channel()
    exchange = channel.exchange('events.exchange', type='topic')
    queue = channel.queue('events.queue')
    queue.bind(exchange, 'logs.topic')
    queue.consume(consumer)

    await conn.start()

    while True:
        # wait for messages
        await asyncio.sleep(1)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
