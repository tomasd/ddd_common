import threading


class Entity(object):
    def __init__(self, id, *args, **kwargs):
        self._id = id
        self._initialize(*args, **kwargs)

    @property
    def id(self):
        return self._id

    def _initialize(self, *args, **kwargs):
        pass
        # raise NotImplementedError(
        #     '_initialize not implemented in entity %r' % self.__class__)


_storage = threading.local()


class PublisherLifecycle(object):
    def __init__(self, event_publisher, subscriber):
        self._event_publisher = event_publisher
        self._subscriber = subscriber

    def __enter__(self):
        self._event_publisher.reset()

        self._event_publisher.subscribe(self._subscriber)

    def __exit__(self, exc_type, exc_val, exc_tb):

        if exc_val:
            raise


def publisher():
    class DomainEventPublisher(object):
        def __init__(self):
            self._publishing = False
            self._subscribers = []

        @property
        def is_publishing(self):
            return self._publishing

        @property
        def has_subscribers(self):
            return self._subscribers is not None

        def _ensure_subscribers_list(self):
            if not self.has_subscribers:
                self._subscribers = []

        def publish(self, event):
            if not self.is_publishing and self.has_subscribers:
                try:
                    self._publishing = True

                    event_type = event.__class__

                    for subscriber in self._subscribers:
                        subscribed_event_type = subscriber.subscribed_event_type

                        is_subscribed = event_type == subscribed_event_type or subscribed_event_type is None

                        if is_subscribed:
                            subscriber.handle_event(event)

                finally:
                    self._publishing = False

        def publish_all(self, events):
            for event in events:
                self.publish(event)

        def reset(self):
            if not self.is_publishing:
                self._subscribers = []

        def subscribe(self, subscriber):
            if not self.is_publishing:
                self._ensure_subscribers_list()

                self._subscribers.append(subscriber)


    if not hasattr(_storage, 'instance'):
        setattr(_storage, 'instance', DomainEventPublisher())

    return getattr(_storage, 'instance')


class EventsCollector(object):
    def __init__(self):
        self.events = []

    @property
    def subscribed_event_type(self):
        return None

    def handle_event(self, event):
        self.events.append(event)