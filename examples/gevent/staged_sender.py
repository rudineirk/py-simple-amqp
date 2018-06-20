from gevent import monkey  # isort:skip
monkey.patch_all()  # isort:skip

from time import sleep  # noqa: E402

from simple_amqp import AmqpMsg, AmqpParameters  # noqa: E402
from simple_amqp.gevent import GeventAmqpConnection  # noqa: E402


def main():
    conn = GeventAmqpConnection(AmqpParameters())
    channel = conn.channel()
    channel.exchange('events.first', type='topic')

    second_stage = conn.stage('2:second')
    channel.exchange(
        'events.second',
        type='topic',
        stage=second_stage,
    )

    conn.start()

    for _ in range(3):
        sleep(1)
        conn.publish(channel, AmqpMsg(
            exchange='events.first',
            topic='msg',
            payload=b'first',
        ))

    conn.next_stage()

    for _ in range(3):
        sleep(1)
        conn.publish(channel, AmqpMsg(
            exchange='events.second',
            topic='msg',
            payload=b'second',
        ))

    conn.stop()


if __name__ == '__main__':
    main()
