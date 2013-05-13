from datetime import datetime
import time
import functools
from hamcrest import assert_that, has_length
import pika
import sqlalchemy as sa
from ddd_common.event import JsonSerializableMixin
from ddd_common.port.adapter.messaging.rabbitmq import Exchange, Queue, ListenerMixin, ExchangeListener

from ddd_common.port.adapter.notification.rabbitmq import RabbitMQNotificationPublisher
from ddd_common.port.adapter.persistence import mapping
from ddd_common.port.adapter.persistence.sa import SaEventStore, SaPublishedNotificationTrackerStore


def session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        engine = sa.create_engine('sqlite://')
        mapping.create_schema(engine)

        Session = sa.orm.sessionmaker(bind=engine)
        kwargs['session'] = Session()
        return func(*args, **kwargs)

    return wrapper

class MyEvent(JsonSerializableMixin):
    def __init__(self, body):
        self.event_version = 1
        self.occured_on = datetime.now()
        self.body = body

    def to_json(self):
        return {
            'event_version':self.event_version,
            'occured_on':self._serialize_datetime(self.occured_on),
            'body': self.body
        }

    @classmethod
    def from_json(self, value):
        event = MyEvent(value['body'])
        event.occured_on = self._deserialize_datatime(value['occured_on'])
        return event


class MyListener(ListenerMixin):
    def __init__(self):
        self.messages = []

    @property
    def queue_name(self):
        return 'test_queue'

    @property
    def listens_to(self):
        return 'integration_tests.test_notification.MyEvent'

    def dispatch(self, message):
        self.messages.append(message)


@session
def test_rabbitmqnotification(session):
    event_store = SaEventStore(session)
    tracker_store = SaPublishedNotificationTrackerStore(session, 'unit.test')
    connection = pika.BlockingConnection()

    exchange = Exchange.fanout_instance(connection, 'ddd_common_unit_test', False)

    listener = MyListener()
    ExchangeListener(exchange, listener)

    publisher = RabbitMQNotificationPublisher(event_store, tracker_store, exchange)

    event_store.append(MyEvent('message1'))

    publisher.publish_notifications()
