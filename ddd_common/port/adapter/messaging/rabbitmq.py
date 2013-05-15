from time import mktime
import functools
import logging
import kombu
from kombu.mixins import ConsumerMixin

from ddd_common import fullname


def to_timestamp(timestamp):
    return int(mktime(timestamp.timetuple()))

class Exchange(object):
    @classmethod
    def direct_instance(cls, exchange_name, durable=True):
        return Exchange(exchange_name, 'direct', durable)

    @classmethod
    def fanout_instance(cls, exchange_name, durable=True):
        return Exchange(exchange_name, 'fanout', durable)

    @classmethod
    def headers_instance(cls, exchange_name, durable=True):
        return Exchange(exchange_name, 'headers', durable)

    @classmethod
    def topic_instance(cls, exchange_name, durable=True):
        return Exchange(exchange_name, 'topic', durable)

    def __init__(self, exchange_name, type, durable):
        self._exchange_name = exchange_name
        self._type = type
        self._durable = durable

    @property
    def exchange_name(self):
        return self._exchange_name

    def exchange(self, connection):
        return kombu.Exchange(channel=connection, name=self._exchange_name,
                              type=self._type, durable=self._durable)

    def declare(self, connection):
        exchange = self.exchange(connection)
        exchange.declare()


class WorkerConsumer(object):
    def __init__(self, listeners):
        self._listeners = listeners

    def declare(self, channel, declare=True):
        consumers = []
        for listener in self._listeners:
            exchange = kombu.Exchange(channel=channel, name=listener.exchange_name, type='direct', durable=True)
            queue_name = '%s.%s' % (listener.exchange_name, fullname(listener))

            bindings = []
            for event in listener.listens_to:
                routing_key = event
                queue = kombu.Queue(channel=channel, name=queue_name, exchange=exchange, routing_key=routing_key, durable=True)

                if declare:
                    queue.declare()

                bindings.append(queue)

            consumer = kombu.Consumer(channel, queues=bindings,
                           callbacks=[functools.partial(self._handle_delivery, listener)])

            consumers.append(consumer)

        return consumers

    def _handle_delivery(self, listener, body, message):
        try:
            listener.dispatch(body)
            message.ack()
        except Exception, e:
            logging.exception(e)
            raise


    def consumers(self, channel):
        return self.declare(channel, declare=False)

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