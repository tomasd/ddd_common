import functools
from ddd_common.domain.model import publisher, PublisherLifecycle


def events(func):
    func.__store_events__ = True
    return func


def transactional(func):
    func.__transactional__ = True
    return func


def service(func):
    return events(transactional(func))


def store_all_events(event_store, obj):
    for key in dir(obj):
        value = getattr(obj, key)

        if callable(value) and hasattr(value, '__store_events__'):
            setattr(obj, key, _store_events(event_store)(value))
    return obj


def _store_events(event_store):
    def wrap_function(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with PublisherLifecycle(publisher(), EventStoreSubscriber(event_store)):
                return func(*args, **kwargs)

        return wrapper

    return wrap_function


class EventStoreSubscriber(object):
    def __init__(self, event_store):
        self._event_store = event_store

    @property
    def subscribed_event_type(self):
        return None

    def handle_event(self, event):
        self._event_store.append(event)
