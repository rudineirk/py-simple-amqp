import traceback
from asyncio import ensure_future, get_event_loop, sleep

from aio_pika import ExchangeType as PikaExchangeType
from aio_pika import IncomingMessage as PikaIncomingMessage
from aio_pika import Message as PikaMessage
from aio_pika import connect as pika_connect
from aio_pika.exchange import Exchange
from aio_pika.queue import Queue

from .actions import (
    BindConsumer,
    BindExchange,
    BindQueue,
    CreateChannel,
    CreateConnection,
    DeclareExchange,
    DeclareQueue
)
from .base import AmqpChannel, AmqpConnection, AmqpConsumer
from .data import AmqpMsg, AmqpParameters
from .log import logger

EXCHANGE_TYPES_MAP = {
    'direct': PikaExchangeType.DIRECT,
    'topic': PikaExchangeType.TOPIC,
    'fanout': PikaExchangeType.FANOUT,
}


class AsyncioAmqpConnection(AmqpConnection):
    def __init__(self, params: AmqpParameters):
        super().__init__(params)
        self._conn = None
        self._channels = {}
        self._consumers = {}
        self._consumer_queues = {}
        self._queues = {}
        self._exchanges = {}

        self._closing = False
        self._auto_reconnect = False
        self.reconnect_delay = 1

        self._conn_error_handler = None
        self._consumer_error_handler = None
        self.log = logger

    async def start(self, auto_reconnect=True, wait=True):
        super().start()
        self._closing = False
        self._auto_reconnect = auto_reconnect
        if wait:
            await self._action_processor()
        else:
            ensure_future(self._action_processor())

    async def stop(self):
        self._closing = True

        await self._stop_consuming()
        await self._close_channels()
        await self._close_connection()

    def cancel_consumer(
        self,
        channel: AmqpChannel,
        consumer: AmqpConsumer,
    ):
        real_channel = self._get_channel(channel.number)
        queue = self._consumer_queues[consumer.tag]
        return self._cancel_consumer(real_channel, queue, consumer.tag)

    async def publish(self, channel: AmqpChannel, msg: AmqpMsg):
        self.log.info(
            'publishing message on channel {}'
            .format(channel.number)
        )
        self.log.debug('publishing message: {}'.format(str(msg)))
        real_channel = self._get_channel(channel.number)
        if msg.exchange:
            exchange = self._exchanges[channel.number][msg.exchange]
        else:
            real_channel = self._get_channel(channel.number)
            exchange = real_channel.default_exchange

        expiration = msg.expiration
        if expiration is not None and expiration < 0:
            expiration = None

        pika_msg = PikaMessage(
            body=msg.payload,
            content_type=msg.content_type,
            content_encoding=msg.encoding,
            correlation_id=msg.correlation_id,
            reply_to=msg.reply_to,
            expiration=expiration,
            headers=msg.headers,
        )
        await exchange.publish(
            pika_msg,
            msg.topic,
        )

    async def _action_processor(self):
        self.log.info('Starting action processor')
        while True:
            ok = True
            try:
                await self._run_actions()
            except Exception as e:
                self.log.error('an error ocurred when processing actions')
                ok = False
                if self._conn_error_handlers:
                    for handler in self._conn_error_handlers:
                        handler(e)
                else:
                    traceback.print_exc()

            if ok:
                self.log.info('Action processor done')

            if ok:
                return
            elif not self._auto_reconnect:
                return

            await sleep(self.reconnect_delay)
            self.log.info('retrying to process actions')

    async def _run_actions(self):
        for action in self.actions:
            self.log.debug('action: {}'.format(str(action)))
            if action.TYPE == CreateConnection.TYPE:
                await self._connect(action)
            elif action.TYPE == CreateChannel.TYPE:
                await self._create_channel(action)
            elif action.TYPE == DeclareExchange.TYPE:
                await self._declare_exchange(action)
            elif action.TYPE == DeclareQueue.TYPE:
                await self._declare_queue(action)
            elif action.TYPE == BindExchange.TYPE:
                await self._bind_exchange(action)
            elif action.TYPE == BindQueue.TYPE:
                await self._bind_queue(action)
            elif action.TYPE == BindConsumer.TYPE:
                await self._bind_consumer(action)

    def _get_channel(self, number: int):
        return self._channels[number]

    def _set_channel(self, number: int, channel):
        self._channels[number] = channel
        self._consumers[number] = set()

    def _remove_channel(self, number):
        self._channels.pop(number)
        self._consumers.pop(number)

    def _clear_channels(self):
        self._channels = {}
        self._consumers = {}
        self._consumer_queues = {}
        self._queues = {}
        self._exchanges = {}

    def _get_queue(self, channel_num, queue_name):
        channel = self._get_channel(channel_num)
        if channel_num not in self._queues:
            self._queues[channel_num] = {}
        if queue_name not in self._queues[channel_num]:
            loop = get_event_loop()
            self._queues[channel_num][queue_name] = Queue(
                loop=loop,
                channel=channel._channel,
                future_store=channel._futures.get_child(),
                name=queue_name,
                durable=False,
                exclusive=False,
                auto_delete=False,
                arguments={},
            )

        return self._queues[channel_num][queue_name]

    def _get_exchange(self, channel_num, exchange_name):
        channel = self._get_channel(channel_num)
        if channel_num not in self._exchanges:
            self._exchanges[channel_num] = {}
        if exchange_name not in self._exchanges[channel_num]:
            loop = get_event_loop()
            self._exchanges[channel_num][exchange_name] = Exchange(
                loop=loop,
                channel=channel._channel,
                publish_method=channel._publish,
                future_store=channel._futures.get_child(),
                name=exchange_name,
                type=EXCHANGE_TYPES_MAP['topic'],
                durable=False,
                internal=False,
                auto_delete=False,
                passive=False,
                arguments={},
            )

        return self._exchanges[channel_num][exchange_name]

    async def _connect(self, action: CreateConnection):
        self.log.info('starting connection')
        self._conn = await pika_connect(
            host=action.host,
            port=action.port,
            virtualhost=action.vhost,
            login=action.username,
            password=action.password,
        )
        self._conn.add_close_callback(
            lambda *args: ensure_future(self._on_connection_close(*args))
        )

    async def _stop_consuming(self):
        for channel_number, consumer_tags in self._consumers.items():
            for consumer_tag in list(consumer_tags):
                await self._cancel_consumer(channel_number, consumer_tag)

    async def _cancel_consumer(self, channel_number, consumer_tag):
        queue = self._consumer_queues[consumer_tag]
        await queue.cancel(consumer_tag=consumer_tag)
        self._consumers[channel_number].remove(consumer_tag)
        self._consumer_queues.pop(consumer_tag)

    async def _close_channels(self):
        for channel in self._channels.values():
            await channel.close()

    async def _close_connection(self):
        self._closing = True
        await self._conn.close()
        self._conn = None

    async def _on_connection_close(self, _):
        self.log.info('connection closed')
        self._clear_channels()
        if not self._closing and self._auto_reconnect:
            await sleep(self.reconnect_delay)
            ensure_future(self._action_processor())

    async def _create_channel(self, action: CreateChannel):
        self.log.info('creating channel {}'.format(action.number))
        channel = await self._conn.channel(action.number)
        self.log.debug('channel {} opened'.format(action.number))
        self._set_channel(action.number, channel)

    async def _declare_queue(self, action: DeclareQueue):
        self.log.info('declaring queue {}'.format(action.name))
        channel = self._get_channel(action.channel)
        if action.channel not in self._queues:
            self._queues[action.channel] = {}

        self._queues[action.channel][action.name] = await channel \
            .declare_queue(
                name=action.name,
                durable=action.durable,
                exclusive=action.exclusive,
                auto_delete=action.auto_delete,
                arguments=action.props,
        )
        self.log.debug('queue {} declared'.format(action.name))

    async def _declare_exchange(self, action: DeclareExchange):
        self.log.info('declaring exchange {}'.format(action.name))
        channel = self._get_channel(action.channel)
        if action.channel not in self._exchanges:
            self._exchanges[action.channel] = {}

        exchange_type = EXCHANGE_TYPES_MAP[action.type]

        self._exchanges[action.channel][action.name] = await channel \
            .declare_exchange(
                name=action.name,
                type=exchange_type,
                durable=action.durable,
                auto_delete=action.auto_delete,
                internal=action.internal,
                arguments=action.props,
        )
        self.log.debug('exchange {} declared'.format(action.name))

    async def _bind_queue(self, action: BindQueue):
        self.log.info('binding queue {} to exchange {}'.format(
            action.queue,
            action.exchange,
        ))
        queue = self._get_queue(action.channel, action.queue)
        exchange = self._get_exchange(action.channel, action.exchange)

        await queue.bind(
            exchange=exchange,
            routing_key=action.routing_key,
            arguments=action.props,
        )
        self.log.debug('bound queue {} to exchange {}'.format(
            action.queue,
            action.exchange,
        ))

    async def _bind_exchange(self, action: BindExchange):
        self.log.info('binding exchange {} to exchange {}'.format(
            action.src_exchange,
            action.dst_exchange,
        ))
        src_exchange = self._get_exchange(action.channel, action.src_exchange)
        dst_exchange = self._get_exchange(action.channel, action.dst_exchange)

        await dst_exchange.bind(
            src_exchange,
            routing_key=action.routing_key,
            props=action.props,
        )
        self.log.debug('bound exchange {} to exchange {}'.format(
            action.src_exchange,
            action.dst_exchange,
        ))

    async def _bind_consumer(self, action: BindConsumer):
        self.log.info('binding consumer to queue {}'.format(
            action.queue,
        ))
        queue = self._get_queue(action.channel, action.queue)
        self._consumers[action.channel].add(action.tag)
        self._consumer_queues[action.tag] = queue

        def consumer(pika_msg: PikaIncomingMessage):
            msg = AmqpMsg(
                payload=pika_msg.body,
                content_type=pika_msg.content_type,
                encoding=pika_msg.content_encoding,
                exchange=pika_msg.exchange,
                topic=pika_msg.routing_key,
                correlation_id=pika_msg.correlation_id.decode(),
                reply_to=pika_msg.reply_to,
                expiration=pika_msg.expiration,
                headers=pika_msg.headers,
            )
            ensure_future(self._handle_msg(
                action,
                pika_msg,
                msg,
            ))

        await queue.consume(
            callback=consumer,
            no_ack=action.auto_ack,
            exclusive=action.exclusive,
            consumer_tag=action.tag,
            arguments=action.props,
        )

    async def _handle_msg(
            self,
            action: BindConsumer,
            pika_msg: PikaIncomingMessage,
            msg: AmqpMsg,
    ):
        self.log.info(
            'received msg {} in queue {} '
            'from exchange {} topic {}'.format(
                pika_msg.delivery_tag,
                action.queue,
                msg.exchange,
                msg.topic,
            )
        )
        self.log.debug('msg received: {}'.format(str(msg)))

        result = True
        try:
            result = await action.callback(msg)
            self.log.debug('msg processed')
        except Exception as e:
            self.log.error('an error occurred when processing message')
            result = False
            if self._consumer_error_handlers:
                for handler in self._consumer_error_handlers:
                    handler(e)
            else:
                traceback.print_exc()

        if not action.auto_ack and result:
            self.log.debug(
                'sending ack for message {}'
                .format(pika_msg.delivery_tag)
            )
            pika_msg.ack()
        elif not action.auto_ack:
            self.log.debug(
                'sending nack for message {}'
                .format(pika_msg.delivery_tag)
            )
            pika_msg.nack(requeue=action.nack_requeue)
