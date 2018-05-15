from typing import Callable, Dict

from dataclasses import dataclass, field, replace


class Action:
    def replace(self, **kwargs):
        return replace(self, **kwargs)


@dataclass(frozen=True)
class CreateConnection(Action):
    host: str = 'localhost'
    port: int = 5672
    username: str = 'guest'
    password: str = 'guest'
    vhost: str = '/'
    TYPE: str = 'conn.create'


@dataclass(frozen=True)
class CreateChannel(Action):
    number: int = -1
    TYPE: str = 'chann.create'


@dataclass(frozen=True)
class DeclareQueue(Action):
    channel: int = -1
    name: str = ''
    durable: bool = False
    exclusive: bool = False
    auto_delete: bool = False
    props: Dict[str, str] = field(default_factory=dict)
    TYPE: str = 'queue.declare'


@dataclass(frozen=True)
class DeclareExchange(Action):
    channel: int = -1
    name: str = ''
    type: str = ''
    durable: bool = False
    auto_delete: bool = False
    internal: bool = False
    props: Dict[str, str] = field(default_factory=dict)
    TYPE: str = 'exchange.declare'


@dataclass(frozen=True)
class BindQueue(Action):
    channel: int = -1
    queue: str = ''
    exchange: str = ''
    routing_key: str = ''
    props: Dict[str, str] = field(default_factory=dict)
    TYPE: str = 'queue.bind'


@dataclass(frozen=True)
class BindExchange(Action):
    channel: int = -1
    src_exchange: str = ''
    dst_exchange: str = ''
    routing_key: str = ''
    props: Dict[str, str] = field(default_factory=dict)
    TYPE: str = 'exchange.bind'


@dataclass(frozen=True)
class BindConsumer(Action):
    channel: int = -1
    queue: str = ''
    tag: str = ''
    callback: Callable = None
    auto_ack: bool = False
    exclusive: bool = False
    nack_requeue: bool = True
    props: Dict[str, str] = field(default_factory=dict)
    TYPE: str = 'consumer.bind'
