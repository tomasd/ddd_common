import functools
from ddd_common import fullname
from ddd_common.application import store_all_events
from ddd_common.event import StoredEvent, serialize_event
from ddd_common.notification import PublishedNotificationTracker
from ddd_common.port.adapter.persistence import mapping
import sqlalchemy as sa


class SaRepository(object):
    def __init__(self, session):
        self._session = session

    @property
    def _entity_class(self):
        raise NotImplementedError('_entity_class not specified for %r' % self.__class__)

    @property
    def _table(self):
        raise NotImplementedError('_table not specified for %r' % self.__class__)

    @property
    def _query(self):
        return self._session.query(self._entity_class)

    def _object_of_id(self, id):
        return self._query.filter(self._table.c.id == id).first()

    def save(self, obj):
        self._session.add(obj)

    def add(self, obj):
        self.save(obj)


class SaEventStore(object):
    def __init__(self, session, testing=False):
        self._session = session
        self._testing = testing

        if testing:
            self.events = []

    @property
    def _query(self):
        query = self._session.query(StoredEvent)
        return query

    def all_stored_events_between(self, low_stored_event_id, high_stored_event_id):
        query = self._query.filter(
            mapping.stored_events.c.event_id.between(low_stored_event_id, high_stored_event_id)
        )
        query = query.order_by(sa.asc(mapping.stored_events.c.event_id))

        return query.all()

    def all_stored_events_since(self, low_stored_event_id):
        query = self._query

        if low_stored_event_id is not None:
            query = query.filter(mapping.stored_events.c.event_id>low_stored_event_id)

        query = query.order_by(sa.asc(mapping.stored_events.c.event_id))

        return query.all()

    def append(self, domain_event):
        if self._testing:
            self.events.append(domain_event)
        type_name = domain_event.type_name
        stored_event = StoredEvent(type_name, domain_event.occured_on, serialize_event(domain_event))
        self._session.add(stored_event)
        self._session.flush()

        return stored_event

    def close(self):
        pass

    def count_stored_events(self):
        return self._query.count()


class SaPublishedNotificationTrackerStore(object):
    def __init__(self, session, type_name):
        self._session = session
        self._type_name = type_name

    @property
    def _query(self):
        return self._session.query(PublishedNotificationTracker)

    def published_notification_tracker(self, type_name=None):
        type_name = type_name if type_name is not None else self._type_name

        query = self._query.filter(mapping.published_notification_tracker.c.type_name==type_name)

        tracker = query.first()

        if tracker is None:
            tracker = PublishedNotificationTracker(type_name)

        return tracker

    def track_most_recent_published_notification(self, tracker, notifications):
        if notifications:
            most_recent_id = notifications[-1].notification_id

            tracker.set_most_recent_published_notification_id(most_recent_id)
            self._session.add(tracker)


def wrap_function_in_transaction(session):
    def wrap_function(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return_value = func(*args, **kwargs)
                session.commit()
                return return_value
            except Exception, e:
                session.rollback()
                raise

        return wrapper
    return wrap_function


def wrap_object_in_transaction(session, obj):
    for key in dir(obj):
        value = getattr(obj, key)

        if callable(value) and hasattr(value, '__transactional__'):
            setattr(obj, key, wrap_function_in_transaction(session)(value))

    return obj

class TransactionalService(object):
    def __init__(self, session, event_store):
        self._session = session
        self._event_store = event_store

    def __call__(self, obj):
        obj = store_all_events(self._event_store, obj)
        return wrap_object_in_transaction(self._session, obj)
