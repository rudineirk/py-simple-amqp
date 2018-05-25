import logging
import traceback
from os import environ
from time import sleep

import pika
from gevent import spawn
from gevent.event import AsyncResult

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
from .log import setup_logger

NEXT_ACTION = 1
BREAK_ACTION = 0


class PikaConnection(pika.SelectConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_disconnect_handlers = []

    def add_on_disconnect_error_callback(self, callback):
        self._on_disconnect_handlers.append(callback)

    def _adapter_disconnect(self):
        error = None
        try:
            super()._adapter_disconnect()
        except Exception as e:
            error = e

        if error:
            for handler in self._on_disconnect_handlers:
                spawn(handler, error)

            raise error


class GeventAmqpConnection(AmqpConnection):
    def __init__(self, params: AmqpParameters, logger=None):
        super().__init__(params)
        self._pika_conn = None
        self._pika_channels = {}
        self._pika_consumers = {}

        self._closing = False
        self._closing_fut = None
        self._consumer_cancel_fut = None
        self._auto_reconnect = False
        self.reconnect_delay = 1

        self._processor_fut = None
        self._conn_error_handler = None
        self._consumer_error_handler = None
        self.log = logger if logger is not None else setup_logger()

    def start(self, auto_reconnect=True, wait=True):
        super().start()
        self._closing = False
        self._auto_reconnect = auto_reconnect
        if wait:
            self._action_processor()
        else:
            spawn(self._action_processor)

    def stop(self):
        self._closing = True
        self._closing_fut = AsyncResult()

        self._stop_consuming()
        sleep(2)
        self._close_channels()
        self._closing_fut.get()
        self._closing_fut = None

    def cancel_consumer(self, channel: AmqpChannel, consumer: AmqpConsumer):
        real_channel = self._get_channel(channel.number)
        self._cancel_consumer(real_channel, consumer.tag)

    def publish(self, channel: AmqpChannel, msg: AmqpMsg):
        self.log.info(
            'publishing message on channel {}'
            .format(channel.number)
        )
        self.log.debug('publishing message: {}'.format(str(msg)))
        real_channel = self._get_channel(channel.number)

        expiration = msg.expiration
        if expiration is not None and expiration < 0:
            expiration = None
        if expiration is not None:
            expiration = str(expiration)

        properties = pika.BasicProperties(
            content_type=msg.content_type,
            content_encoding=msg.encoding,
            correlation_id=msg.correlation_id,
            reply_to=msg.reply_to,
            expiration=expiration,
            headers=msg.headers,
        )
        real_channel.basic_publish(
            msg.exchange,
            msg.topic,
            msg.payload,
            properties,
        )

    def _action_processor(self):
        self.log.info('Starting action processor')
        while True:
            ok = True
            try:
                ok = self._run_actions()
            except Exception as e:
                self.log.error('an error ocurred when processing actions')
                self._processor_fut = None
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

            sleep(self.reconnect_delay)
            self.log.info('retrying to process actions')

    def _run_actions(self):
        for action in self.actions:
            self.log.debug('action: {}'.format(str(action)))
            self._processor_fut = AsyncResult()
            if action.TYPE == CreateConnection.TYPE:
                self._connect(action)
            elif action.TYPE == CreateChannel.TYPE:
                self._create_channel(action)
            elif action.TYPE == DeclareExchange.TYPE:
                self._declare_exchange(action)
            elif action.TYPE == DeclareQueue.TYPE:
                self._declare_queue(action)
            elif action.TYPE == BindExchange.TYPE:
                self._bind_exchange(action)
            elif action.TYPE == BindQueue.TYPE:
                self._bind_queue(action)
            elif action.TYPE == BindConsumer.TYPE:
                self._bind_consumer(action)

            res = self._processor_fut.get()
            if res != NEXT_ACTION:
                return False

        return True

    def _next_action(self, status=NEXT_ACTION):
        self._processor_fut.set(status)

    def _action_error(self, exc):
        self._processor_fut.set_exception(exc)

    def _get_channel(self, number: int):
        return self._pika_channels[number]

    def _set_channel(self, number: int, channel):
        self._pika_channels[number] = channel
        self._pika_consumers[number] = set()

    def _remove_channel(self, number):
        self._pika_channels.pop(number)
        self._pika_consumers.pop(number)

    def _clear_channels(self):
        self._pika_channels = {}
        self._pika_consumers = {}

    def _connect(self, action: CreateConnection):
        self.log.info('starting connection')
        self._pika_conn = PikaConnection(
            pika.ConnectionParameters(
                host=action.host,
                port=action.port,
                virtual_host=action.vhost,
                credentials=pika.credentials.PlainCredentials(
                    username=action.username,
                    password=action.password,
                )
            ),
            self._on_connection_open,
            self._on_connection_error,
            self._on_connection_close,
            stop_ioloop_on_close=False,
        )
        self._pika_conn.add_on_disconnect_error_callback(self._on_disconnect)
        spawn(self._pika_conn.ioloop.start)

    def _stop_consuming(self):
        for channel_number, consumer_tags in self._pika_consumers.items():
            channel = self._get_channel(channel_number)
            for consumer_tag in list(consumer_tags):
                self._cancel_consumer(channel, consumer_tag)

    def _cancel_consumer(self, channel, consumer_tag):
        fut = AsyncResult()
        self._consumer_cancel_fut = fut
        channel.basic_cancel(
            consumer_tag=consumer_tag,
            callback=lambda _: fut.set(True),
        )
        fut.get()
        self._consumer_cancel_fut = None
        self._pika_consumers[channel.channel_number].remove(consumer_tag)

    def _close_channels(self):
        for channel in self._pika_channels.values():
            channel.close()

    def _close_connection(self):
        self._pika_conn.close()

    def _on_connection_open(self, *_):
        self.log.info('connection opened')
        self._next_action()

    def _on_connection_error(self, *_):
        self.log.info('connection error')
        self._next_action(BREAK_ACTION)

    def _on_disconnect(self, error):
        self.log.info('connection error')
        self._next_action(BREAK_ACTION)

    def _on_connection_close(self, *_):
        self.log.info('connection closed')
        if self._processor_fut:
            self._processor_fut.set_exception(
                ConnectionAbortedError('amqp connection closed'),
            )
            self._processor_fut = None
        if self._consumer_cancel_fut:
            self._consumer_cancel_fut.set_exception(
                ConnectionAbortedError('amqp connection closed'),
            )
            self._consumer_cancel_fut = None

        self._clear_channels()
        self._pika_conn = None
        if not self._closing and self._auto_reconnect:
            sleep(self.reconnect_delay)
            spawn(self._action_processor)

        if self._closing_fut:
            self._closing_fut.set(True)

    def _create_channel(self, action: CreateChannel):
        self.log.info('creating channel {}'.format(action.number))
        self._pika_conn.channel(
            lambda channel: self._on_channel_open(channel, action.number),
        )

    def _on_channel_open(self, channel, number):
        self.log.debug('channel {} opened'.format(number))
        self._set_channel(number, channel)
        channel.add_on_close_callback(self._on_channel_closed)
        self._next_action()

    def _on_channel_closed(self, channel, *_):
        for key, value in list(self._pika_channels.items()):
            if value == channel:
                self.log.info('channel {} closed'.format(key))
                self._remove_channel(key)

        if not self._pika_channels and not self._closing and self._auto_reconnect:
            spawn(self._action_processor)

        if not self._pika_channels and self._closing:
            self._close_connection()

    def _declare_queue(self, action: DeclareQueue):
        self.log.info('declaring queue {}'.format(action.name))
        channel = self._get_channel(action.channel)
        channel.queue_declare(
            queue=action.name,
            durable=action.durable,
            exclusive=action.exclusive,
            auto_delete=action.auto_delete,
            arguments=action.props,
            callback=lambda *_: self._on_queue_declare(action),
        )

    def _on_queue_declare(self, action: DeclareQueue):
        self.log.debug('queue {} declared'.format(action.name))
        self._next_action()

    def _declare_exchange(self, action: DeclareExchange):
        self.log.info('declaring exchange {}'.format(action.name))
        channel = self._get_channel(action.channel)
        channel.exchange_declare(
            exchange=action.name,
            exchange_type=action.type,
            durable=action.durable,
            auto_delete=action.auto_delete,
            internal=action.internal,
            arguments=action.props,
            callback=lambda *_: self._on_exchange_declare(action),
        )

    def _on_exchange_declare(self, action: DeclareExchange):
        self.log.debug('exchange {} declared'.format(action.name))
        self._next_action()

    def _bind_queue(self, action: BindQueue):
        self.log.info('binding queue {} to exchange {}'.format(
            action.queue,
            action.exchange,
        ))
        channel = self._get_channel(action.channel)
        channel.queue_bind(
            queue=action.queue,
            exchange=action.exchange,
            routing_key=action.routing_key,
            arguments=action.props,
            callback=lambda *_: self._on_bind_queue(action),
        )

    def _on_bind_queue(self, action: BindQueue):
        self.log.debug('bound queue {} to exchange {}'.format(
            action.queue,
            action.exchange,
        ))
        self._next_action()

    def _bind_exchange(self, action: BindExchange):
        self.log.info('binding exchange {} to exchange {}'.format(
            action.src_exchange,
            action.dst_exchange,
        ))
        channel = self._get_channel(action.channel)
        channel.exchange_bind(
            source=action.src_exchange,
            destination=action.dst_exchange,
            routing_key=action.routing_key,
            props=action.props,
            callback=lambda *_: self._on_bind_exchange(action),
        )

    def _on_bind_exchange(self, action: BindExchange):
        self.log.debug('bound exchange {} to exchange {}'.format(
            action.src_exchange,
            action.dst_exchange,
        ))
        self._next_action()

    def _bind_consumer(self, action: BindConsumer):
        self.log.info('binding consumer to queue {}'.format(
            action.queue,
        ))
        channel = self._get_channel(action.channel)
        self._pika_consumers[action.channel].add(action.tag)

        def consumer_callback(channel, deliver, props, payload):
            delivery_tag = deliver.delivery_tag
            msg = AmqpMsg(
                payload=payload,
                content_type=props.content_type,
                encoding=props.content_encoding,
                exchange=deliver.exchange,
                topic=deliver.routing_key,
                correlation_id=props.correlation_id,
                reply_to=props.reply_to,
                expiration=props.expiration,
                headers=props.headers,
            )
            spawn(self._handle_msg, action, channel, delivery_tag, msg)

        channel.basic_consume(
            queue=action.queue,
            no_ack=action.auto_ack,
            exclusive=action.exclusive,
            consumer_tag=action.tag,
            arguments=action.props,
            consumer_callback=consumer_callback,
        )
        self._next_action()

    def _handle_msg(
            self,
            action: BindConsumer,
            channel,
            delivery_tag,
            msg,
    ):
        self.log.info(
            'received msg {} in queue {} '
            'from exchange {} topic {}'.format(
                delivery_tag,
                action.queue,
                msg.exchange,
                msg.topic,
            )
        )
        self.log.debug('msg received: {}'.format(str(msg)))

        result = True
        try:
            result = action.callback(msg)
            self.log.debug('msg processed')
        except Exception as e:
            self.log.error('an error occurred when processing message')
            result = False
            if self._consumer_error_handlers:
                for handler in self._conn_error_handlers:
                    handler(e)
            else:
                traceback.print_exc()

        if not action.auto_ack and result:
            self.log.debug(
                'sending ack for message {}'
                .format(delivery_tag)
            )
            channel.basic_ack(delivery_tag)
        elif not action.auto_ack:
            self.log.debug(
                'sending nack for message {}'
                .format(delivery_tag)
            )
            channel.basic_nack(
                delivery_tag,
                requeue=action.nack_requeue,
            )
