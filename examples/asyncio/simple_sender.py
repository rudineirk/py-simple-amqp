import asyncio

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.asyncio import AsyncioAmqpConnection


async def main():
    conn = AsyncioAmqpConnection(AmqpParameters())
    channel = conn.channel()
    channel.exchange('events.exchange', type='topic')

    await conn.start()

    while True:
        await asyncio.sleep(1)
        await conn.publish(channel, AmqpMsg(
            exchange='events.exchange',
            topic='logs.topic',
            payload=b'hello world',
        ))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
