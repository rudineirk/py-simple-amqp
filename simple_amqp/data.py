from typing import Callable, Dict

from dataclasses import dataclass, field, replace

CONTENT_TYPE_TEXT_PLAIN = 'text/plain'
ENCODING_UTF8 = 'utf8'


class Data:
    def replace(self, **kwargs):
        return replace(self, **kwargs)


@dataclass(frozen=True)
class AmqpParameters(Data):
    host: str = 'localhost'
    port: int = 5672
    username: str = 'guest'
    password: str = 'guest'
    vhost: str = '/'


@dataclass(frozen=True)
class AmqpMsg(Data):
    payload: bytes
    exchange: str = ''
    topic: str = ''
    correlation_id: str = ''
    content_type: str = CONTENT_TYPE_TEXT_PLAIN
    encoding: str = ENCODING_UTF8
    reply_to: str = None
    expiration: int = None
    headers: Dict[str, str] = field(default_factory=dict)


AmqpConsumerCallback = Callable[[AmqpMsg], None]
