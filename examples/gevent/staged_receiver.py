from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

from time import sleep  # noqa: E402

from simple_amqp import AmqpMsg, AmqpParameters  # noqa: E402
from simple_amqp.gevent import GeventAmqpConnection  # noqa: E402

FIRST_COUNT = 0
SECOND_COUNT = 0


def consumer(msg: AmqpMsg):
    global FIRST_COUNT
    global SECOND_COUNT
    payload = msg.payload
    payload = payload.decode()
    if payload == 'first':
        FIRST_COUNT += 1
    if payload == 'second':
        SECOND_COUNT += 1

    print('msg received: {}'.format(payload))

    # acknowledge that the message was received correctly
    return True


def main():
    conn = GeventAmqpConnection(AmqpParameters())
    channel = conn.channel()

    first_exchange = channel.exchange('events.first', type='topic')
    first_queue = channel.queue('events.first')
    first_queue.bind(first_exchange, 'msg')
    first_queue.consume(consumer)

    second_stage = conn.stage('2:second')
    second_exchange = channel.exchange(
        'events.second',
        type='topic',
        stage=second_stage,
    )
    second_queue = channel.queue(
        'events.second',
        stage=second_stage,
    )
    second_queue.bind(
        second_exchange,
        'msg',
        stage=second_stage,
    )
    second_queue.consume(
        consumer,
        stage=second_stage,
    )

    conn.start()

    while FIRST_COUNT < 3:
        # wait for messages
        sleep(.1)

    conn.next_stage()

    while SECOND_COUNT < 3:
        # wait for messages
        sleep(.1)

    conn.stop()


if __name__ == '__main__':
    main()
