from abc import ABCMeta
from typing import Dict
from uuid import uuid4

from dataclasses import dataclass

from .actions import (
    BindConsumer,
    BindExchange,
    BindQueue,
    CreateChannel,
    CreateConnection,
    DeclareExchange,
    DeclareQueue
)
from .data import AmqpConsumerCallback, AmqpMsg, AmqpParameters

DEFAULT_STAGE_NAME = '0:init'


def create_name(name):
    if not name:
        name = 'private.{}'.format(str(uuid4()))

    return name


def sort_actions(actions):
    connections = []
    channels = []
    exchange_declarations = []
    queue_declarations = []
    exchange_bindings = []
    queue_bindings = []
    consumer_bindings = []

    for action in actions:
        if action.TYPE == CreateConnection.TYPE:
            connections.append(action)
        elif action.TYPE == CreateChannel.TYPE:
            channels.append(action)
        elif action.TYPE == DeclareExchange.TYPE:
            exchange_declarations.append(action)
        elif action.TYPE == DeclareQueue.TYPE:
            queue_declarations.append(action)
        elif action.TYPE == BindExchange.TYPE:
            exchange_bindings.append(action)
        elif action.TYPE == BindQueue.TYPE:
            exchange_bindings.append(action)
        elif action.TYPE == BindConsumer.TYPE:
            consumer_bindings.append(action)

    return [
        *connections,
        *channels,
        *exchange_declarations,
        *queue_declarations,
        *exchange_bindings,
        *queue_bindings,
        *consumer_bindings,
    ]


class AmqpConnection(metaclass=ABCMeta):
    def __init__(self, params: AmqpParameters, default_stage=DEFAULT_STAGE_NAME):
        self.actions = {}
        self._default_stage = self.stage(default_stage)
        self._current_stage = None

        self.add_action(CreateConnection(
            host=params.host,
            port=params.port,
            username=params.username,
            password=params.password,
            vhost=params.vhost,
        ))

        self._channel_number = 1
        self._conn_error_handlers = set()
        self._consumer_error_handlers = set()

    @property
    def stages(self):
        return sorted(self.actions.keys())

    def add_conn_error_handler(self, handler):
        self._conn_error_handlers.add(handler)

    def add_consumer_error_handler(self, handler):
        self._consumer_error_handlers.add(handler)

    def start(self):
        self._sort_actions()
        self._current_stage = None
        stage = self.stages[0]
        return stage

    def next_stage(self):
        if self._current_stage is None:
            raise ConnectionError('Connection has not been started')

        stage = None
        try:
            pos = self.stages.index(self._current_stage)
            stage = self.stages[pos + 1]
        except (IndexError, ValueError):
            pass

        if stage is None:
            raise IndexError('No more stages')

        return stage

    def stop(self):
        raise NotImplementedError

    def cancel_consumer(
            self,
            channel: 'AmqpChannel',
            consumer: 'AmqpConsumer',
    ):
        raise NotImplementedError

    def publish(self, channel: 'AmqpChannel', msg: AmqpMsg):
        raise NotImplementedError

    def channel(self, stage: 'AmqpStage' = None) -> 'AmqpChannel':
        number = self._channel_number
        self._channel_number += 1
        return AmqpChannel(self, number, stage=stage)

    def stage(self, name: str=DEFAULT_STAGE_NAME) -> 'AmqpStage':
        stage = AmqpStage(name)
        if stage.name not in self.actions:
            self.actions[stage.name] = []

        return stage

    def add_action(self, action, stage: 'AmqpStage' = None):
        if stage is None:
            stage = self._default_stage

        self.actions[stage.name].append(action)

    def _sort_actions(self):
        for stage, actions in self.actions.items():
            self.actions[stage] = sort_actions(actions)


@dataclass(frozen=True)
class AmqpStage:
    name: str = DEFAULT_STAGE_NAME


class AmqpChannel:
    def __init__(self, conn, number=-1, stage: AmqpStage = None):
        self.conn = conn
        self.number = number
        self._queue_cache = {}
        self._exchange_cache = {}

        self.conn.add_action(CreateChannel(
            number=number,
        ), stage=stage)

    def queue(
            self,
            name: str='',
            durable: bool=False,
            exclusive: bool=False,
            auto_delete: bool=False,
            props: Dict[str, str]=None,
            stage: AmqpStage = None,
    ) -> 'AmqpQueue':
        try:
            if name:
                return self._queue_cache[name]
        except KeyError:
            pass

        queue = AmqpQueue(
            self.conn, self,
            name=name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            props=props,
            stage=stage,
        )
        if not name:
            return queue

        self._queue_cache[name] = queue
        return queue

    def exchange(
            self,
            name: str ='',
            type: str='direct',
            durable: bool=False,
            auto_delete: bool=False,
            internal: bool=False,
            props: Dict[str, str]=None,
            stage: AmqpStage = None,
    ) -> 'AmqpExchange':
        try:
            if name:
                return self._exchange_cache[name]
        except KeyError:
            pass

        exchange = AmqpExchange(
            self.conn, self,
            name=name,
            type=type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            props=props,
            stage=stage,
        )
        if not name:
            return exchange

        self._exchange_cache[name] = exchange
        return exchange

    def publish(self, msg: AmqpMsg):
        return self.conn.publish(self, msg)

    def cancel_consumer(self, consumer: 'AmqpConsumer'):
        self.conn.cancel_consumer(self, consumer)


class AmqpQueue:
    def __init__(
            self,
            conn: AmqpConnection,
            channel: AmqpChannel,
            name: str='',
            durable: bool=False,
            exclusive: bool=False,
            auto_delete: bool=False,
            props: Dict[str, str]=None,
            stage: AmqpStage = None,
    ):
        self.conn = conn
        self.channel = channel
        self.name = create_name(name)

        if props is None:
            props = {}

        self.conn.add_action(DeclareQueue(
            channel=channel.number,
            name=self.name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            props=props,
        ), stage=stage)

    def bind(
            self,
            exchange: 'AmqpExchange',
            routing_key: str,
            props: Dict[str, str] = None,
            stage: AmqpStage = None,
    ):
        if props is None:
            props = {}

        self.conn.add_action(BindQueue(
            channel=self.channel.number,
            queue=self.name,
            exchange=exchange.name,
            routing_key=routing_key,
            props=props,
        ), stage=stage)
        return self

    def consume(
            self,
            callback: AmqpConsumerCallback,
            auto_ack: bool=False,
            exclusive: bool=False,
            nack_requeue: bool=True,
            props: Dict[str, str] = None,
            stage: AmqpStage = None,
    ) -> 'AmqpConsumer':
        return AmqpConsumer(
            self.conn, self.channel, self,
            callback=callback,
            auto_ack=auto_ack,
            exclusive=exclusive,
            nack_requeue=nack_requeue,
            stage=stage,
        )


class AmqpExchange:
    def __init__(
            self,
            conn: AmqpConnection,
            channel: AmqpChannel,
            name: str = '',
            type: str = 'direct',
            durable: bool = False,
            auto_delete: bool = False,
            internal: bool = False,
            props: Dict[str, str]=None,
            stage: AmqpStage = None,
    ):
        self.conn = conn
        self.channel = channel
        self.name = create_name(name)

        if props is None:
            props = {}

        self.conn.add_action(DeclareExchange(
            channel=channel.number,
            name=self.name,
            type=type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            props=props,
        ), stage=stage)

    def bind(
            self,
            exchange: 'AmqpExchange',
            routing_key: str,
            props: Dict[str, str] = None,
            stage: AmqpStage = None,
    ):
        if props is None:
            props = {}

        self.conn.add_action(BindExchange(
            channel=self.channel.number,
            src_exchange=exchange.name,
            dst_exchange=self.name,
            routing_key=routing_key,
            props=props,
        ), stage=stage)
        return self


class AmqpConsumer:
    def __init__(
            self,
            conn: AmqpConnection,
            channel: AmqpChannel,
            queue: AmqpQueue,
            callback: AmqpConsumerCallback,
            auto_ack: bool = False,
            exclusive: bool = False,
            nack_requeue: bool = True,
            props: Dict[str, str] = None,
            stage: AmqpStage = None,
    ):
        self.conn = conn
        self.channel = channel
        self.tag = 'consumer.{}'.format(str(uuid4()))

        if props is None:
            props = {}

        conn.add_action(BindConsumer(
            channel=channel.number,
            queue=queue.name,
            tag=self.tag,
            callback=callback,
            auto_ack=auto_ack,
            exclusive=exclusive,
            props=props,
        ), stage=stage)

    def cancel(self):
        self.channel.cancel_consumer(self)
