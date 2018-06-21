from abc import ABCMeta
from typing import Dict
from uuid import uuid4

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

ZERO_STAGE_NAME = '0:init'


class AmqpConnectionNotOpen(Exception):
    pass


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
    def __init__(self, params: AmqpParameters, zero_stage=ZERO_STAGE_NAME):
        self.stages = []
        self._stage_zero = self.stage(zero_stage)
        self._stage_zero.add_action(CreateConnection(
            host=params.host,
            port=params.port,
            username=params.username,
            password=params.password,
            vhost=params.vhost,
        ))
        self.stages.append(self._stage_zero)

        self._channel_number = 1
        self._conn_error_handlers = set()
        self._consumer_error_handlers = set()

    def add_conn_error_handler(self, handler):
        self._conn_error_handlers.add(handler)

    def add_consumer_error_handler(self, handler):
        self._consumer_error_handlers.add(handler)

    def run_stage(self, stage: 'AmqpStage'):
        raise NotImplementedError

    def add_stage(self, stage: 'AmqpStage'):
        stage.sort_actions()
        self.stages.append(stage)

    def start(self):
        raise NotImplementedError

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

    def stage(self, name: str=None) -> 'AmqpStage':
        if name is None:
            return self._stage_zero

        return AmqpStage(name)


class AmqpStage:
    def __init__(self, name: str):
        self.name = name
        self.actions = []

    def add_action(self, action):
        self.actions.append(action)

    def sort_actions(self):
        self.actions = sort_actions(self.actions)


class AmqpChannel:
    def __init__(self, conn, number=-1, stage: AmqpStage = None):
        self.conn = conn
        self.stage = stage
        self.number = number
        self._queue_cache = {}
        self._exchange_cache = {}

        stage = stage if stage else self.conn.stage()
        stage.add_action(CreateChannel(
            number=number,
        ))

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

        stage = stage if stage else self.conn.stage()
        stage.add_action(DeclareQueue(
            channel=channel.number,
            name=self.name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            props=props,
        ))

    def bind(
            self,
            exchange: 'AmqpExchange',
            routing_key: str,
            props: Dict[str, str] = None,
            stage: AmqpStage = None,
    ):
        if props is None:
            props = {}

        stage = stage if stage else self.conn.stage()
        stage.add_action(BindQueue(
            channel=self.channel.number,
            queue=self.name,
            exchange=exchange.name,
            routing_key=routing_key,
            props=props,
        ))
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

        stage = stage if stage else self.conn.stage()
        stage.add_action(DeclareExchange(
            channel=channel.number,
            name=self.name,
            type=type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            props=props,
        ))

    def bind(
            self,
            exchange: 'AmqpExchange',
            routing_key: str,
            props: Dict[str, str] = None,
            stage: AmqpStage = None,
    ):
        if props is None:
            props = {}

        stage = stage if stage else self.conn.stage()
        stage.add_action(BindExchange(
            channel=self.channel.number,
            src_exchange=exchange.name,
            dst_exchange=self.name,
            routing_key=routing_key,
            props=props,
        ))
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

        stage = stage if stage else self.conn.stage()
        stage.add_action(BindConsumer(
            channel=channel.number,
            queue=queue.name,
            tag=self.tag,
            callback=callback,
            auto_ack=auto_ack,
            exclusive=exclusive,
            props=props,
        ))

    def cancel(self):
        self.channel.cancel_consumer(self)
