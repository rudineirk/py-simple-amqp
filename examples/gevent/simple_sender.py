from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

from time import sleep  # noqa: E402

from simple_amqp import (  # noqa: E402
    AmqpConnectionNotOpen,
    AmqpMsg,
    AmqpParameters
)
from simple_amqp.gevent import GeventAmqpConnection  # noqa: E402


def main():
    conn = GeventAmqpConnection(AmqpParameters())
    channel = conn.channel()
    channel.exchange('events.exchange', type='topic')

    conn.start()

    while True:
        sleep(1)
        try:
            conn.publish(channel, AmqpMsg(
                exchange='events.exchange',
                topic='logs.topic',
                payload=b'hello world',
            ))
        except AmqpConnectionNotOpen:
            print('Not connected to an AMQP server...')
            pass


if __name__ == '__main__':
    main()
