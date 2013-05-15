import logging
from time import mktime
import kombu
from kombu.mixins import ConsumerMixin
from ddd_common import fullname


def to_timestamp(timestamp):
    return int(mktime(timestamp.timetuple()))


class BrokerChannel(object):
    def __init__(self, channel, *args, **kwargs):
        self._channel = channel
        self._initialize(*args, **kwargs)

    @property
    def channel(self):
        return self._channel

    def close(self):
        self.channel.close()

    def _initialize(self, *args, **kwargs):
        raise NotImplementedError(
            '_initialize is not implemented in %r' % self.__class__)


class Exchange(BrokerChannel):
    @classmethod
    def direct_instance(cls, channel, name, durable):
        return Exchange(channel, name, 'direct', durable)

    @classmethod
    def fanout_instance(cls, channel, name, durable):
        return Exchange(channel, name, 'fanout', durable)

    @classmethod
    def headers_instance(cls, channel, name, durable):
        return Exchange(channel, name, 'headers', durable)

    @classmethod
    def topic_instance(cls, channel, name, durable):
        return Exchange(channel, name, 'topic', durable)

    def _initialize(self, name, type, is_durable):
        self._name = name
        self._type = type
        self._is_durable = is_durable

        self._exchange = kombu.Exchange(self._name, self._type, durable=self._is_durable)
        self._producer = kombu.Producer(self.channel, self._exchange)

    @property
    def exchange(self):
        return self._exchange

    @property
    def exchange_name(self):
        return self._name

    @property
    def is_durable(self):
        return self._is_durable

    def send(self, routing_key, text_message, headers=None):
        self._producer.publish(text_message, routing_key, headers=headers, content_type='application/json')


class Queue(BrokerChannel):
    @classmethod
    def durable_instance(cls, channel, name):
        return Queue(channel, name, durable=True)

    @classmethod
    def durable_exclusive_instance(cls, channel, name):
        return Queue(channel, name, durable=True, exclusive=True)

    @classmethod
    def exchange_subscriber_instance(cls, exchange, routing_keys=None,
                                     durable=False, exclusive=True,
                                     auto_delete=True):
        queue = Queue(exchange.channel, '', durable=durable,
                      exclusive=exclusive, auto_delete=auto_delete)
        queue.bind(exchange, *(routing_keys or []))
        return queue

    @classmethod
    def individual_exchange_subscriber_instance(cls, exchange, name,
                                                routing_keys):
        queue = Queue(exchange.channel, name, durable=True)

        queue.bind(exchange, *routing_keys)
        return queue

    def _initialize(self, name, durable=False, exclusive=False,
                    auto_delete=False):
        self._is_durable = durable

        self._name = name
        self._exclusive = exclusive
        self._auto_delete = auto_delete

        self._queues = []
        self._bindings = []

    @property
    def queue_name(self):
        return self._name

    @property
    def is_durable(self):
        return self._is_durable

    def bind(self, exchange, *routing_keys):
        if routing_keys:
            for routing_key in routing_keys:
                self._bindings.append(self._bind(exchange, routing_key))
        else:
            self._bindings.append([self._bind(exchange)])

    @property
    def bindings(self):
        return self._bindings

    def _bind(self, exchange, routing_key=''):
        queue = kombu.Queue(self.queue_name, exchange=exchange.exchange,
                            routing_key=routing_key,
                            durable=self.is_durable, exclusive=self._exclusive,
                            auto_delete=self._auto_delete,
                            channel=self.channel)
        queue.declare()
        queue.queue_bind()
        return queue


class ExchangeListener(object):
    def __init__(self, channel, listener):
        exchange = Exchange.direct_instance(channel, listener.exchange_name, True)
        self._listener = listener
        queue_name = '%s.%s' % (listener.exchange_name, fullname(listener))

        listens_to = listener.listens_to
        if not isinstance(listens_to, (tuple, list)):
            listens_to = [listens_to]

        listens_to = [a.type_name if not isinstance(a, basestring) else a for a in listens_to]

        self._queue = Queue.individual_exchange_subscriber_instance(
            exchange, queue_name, listens_to
        )
        bindings = self._queue.bindings

        self.consumer = kombu.Consumer(channel, queues=bindings, callbacks=[self._handle_delivery])

    def _handle_delivery(self, body, message):
        try:
            self._listener.dispatch(body)
            message.ack()
        except Exception, e:
            logging.exception(e)
            raise


class ListenerMixin(object):
    @property
    def listens_to(self):
        raise NotImplementedError('listens_to is not implemented in %r' % self.__class__)

    @property
    def exchange_name(self):
        raise NotImplementedError('exchange_name is not implemented in %r' % self.__class__)

    def dispatch(self, message):
        pass

    def can_dispatch(self, type_name):
        if self.listens_to == type_name:
            return True

        if isinstance(self.listens_to, (list, tuple)):
            for event in self.listens_to:
                if getattr(event, 'type_name', event) == type_name:
                    return True

        return False


class MqListener(object):
    def __init__(self, exchange_name, listens_to):
        assert isinstance(listens_to, list)
        self._exchange_name = exchange_name
        self._listens_to = listens_to

    @property
    def exchange_name(self):
        return self._exchange_name

    @property
    def listens_to(self):
        return self._listens_to

    def dispatch(self, message):
        raise NotImplementedError('dispatch is not implemented in %r' % self.__class__)

    def can_dispatch(self, exchange, type_name):
        if self.exchange_name != exchange:
            return False

        if self.listens_to == type_name:
            return True

        if isinstance(self.listens_to, (list, tuple)):
            for event in self.listens_to:
                if getattr(event, 'type_name', event) == type_name:
                    return True

        return False


class Worker(ConsumerMixin):
    def __init__(self, connection, worker_consumer):
        self.connection = connection
        self._worker_consumer = worker_consumer

    def get_consumers(self, Consumer, channel):
        return self._worker_consumer.consumers(channel)


class ConnectionFactory(object):
    def __enter__(self):
        return kombu.Connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            raise