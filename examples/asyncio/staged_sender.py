import asyncio

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.asyncio import AsyncioAmqpConnection


async def main():
    conn = AsyncioAmqpConnection(AmqpParameters())
    channel = conn.channel()
    channel.exchange('events.first', type='topic')

    second_stage = conn.stage('2:second')
    channel.exchange(
        'events.second',
        type='topic',
        stage=second_stage,
    )

    await conn.start()

    for _ in range(3):
        await asyncio.sleep(1)
        await conn.publish(channel, AmqpMsg(
            exchange='events.first',
            topic='msg',
            payload=b'first',
        ))

    await conn.next_stage()

    for _ in range(3):
        await asyncio.sleep(1)
        await conn.publish(channel, AmqpMsg(
            exchange='events.second',
            topic='msg',
            payload=b'second',
        ))

    await conn.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
