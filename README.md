# Simple AMQP

A simple AMQP lib for Python

## Installation

```bash
# Asyncio
pip install simple-amqp[asyncio]

# Gevent
pip install simple-amqp[gevent]
```

## Example usage

```python
# Asyncio
from asyncio import get_event_loop, sleep

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.asyncio import AsyncioAmqpConnection

conn = AsyncioAmqpConnection(AmqpParameters(host='localhost'))
channel = conn.channel()

exchange = channel.exchange('exchange.name', 'topic', durable=True)


async def msg_received(msg: AmqpMsg):
    if msg.payload == b'ok':
        return True # ack msg

    return False # nack msg


channel \
  .queue('queue.name', durable=True) \
  .bind(exchange, 'topic.name') \
  .consume(msg_received)


async def main():
    await conn.start()
    await channel.publish(AmqpMsg(
        exchange='exchange.name',
        topic='topic.name',
        payload=b'ok',
    ))

    await sleep(1)
    await conn.stop()

loop = get_event_loop()
loop.run_until_complete(main)
```

```python
# Gevent
from gevent import monkey
monkey.patch_all()

from time import sleep

from simple_amqp import AmqpMsg, AmqpParameters
from simple_amqp.gevent import GeventAmqpConnection

conn = GeventAmqpConnection(AmqpParameters(host='localhost'))
channel = conn.channel()

exchange = channel.exchange('exchange.name', 'topic', durable=True)


def msg_received(msg: AmqpMsg):
    if msg.payload == b'ok':
        return True # ack msg

    return False # nack msg


channel \
  .queue('queue.name', durable=True) \
  .bind(exchange, 'topic.name') \
  .consume(msg_received)

conn.start()

channel.publish(AmqpMsg(
    exchange='exchange.name',
    topic='topic.name',
    payload=b'ok',
))

sleep(1)
conn.stop()
```
