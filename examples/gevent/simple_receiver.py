from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

from time import sleep  # noqa: E402

from simple_amqp import AmqpMsg, AmqpParameters  # noqa: E402
from simple_amqp.gevent import GeventAmqpConnection  # noqa: E402


def consumer(msg: AmqpMsg):
    payload = msg.payload
    payload = payload.decode()
    print('msg received: {}'.format(payload))

    # acknowledge that the message was received correctly
    return True


def main():
    conn = GeventAmqpConnection(AmqpParameters())
    channel = conn.channel()
    exchange = channel.exchange('events.exchange', type='topic')
    queue = channel.queue('events.queue')
    queue.bind(exchange, 'logs.topic')
    queue.consume(consumer)

    conn.start()

    while True:
        # wait for messages
        sleep(1)


if __name__ == '__main__':
    main()
