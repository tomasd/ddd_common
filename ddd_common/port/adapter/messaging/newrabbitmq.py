import functools
import logging
import kombu
from ddd_common import fullname


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