from datetime import datetime
from decimal import Decimal
import simplejson
from ddd_common import fullname
from ddd_common.event import JsonSerializableMixin, serialize_event


class Notification(JsonSerializableMixin):
    def __init__(self, notification_id, serialized_event, occured_on, type_name, event_version):
        self.notification_id = notification_id
        self.event = serialized_event
        self.occured_on = occured_on
        self.type_name = type_name
        self.version = event_version

    @classmethod
    def from_stored_event(cls, notification_id, stored_event):
        reader = NotificationReader(stored_event.event)

        return Notification(
            notification_id, stored_event.event,
            stored_event.occured_on, stored_event.type_name,
            reader.version
        )

    def __eq__(self, other):
        if isinstance(other, Notification):
            return self.notification_id == other.notification_id

    def __hash__(self):
        return hash(self.notification_id)

    def __repr__(self):
        return ("Notification [event=" + repr(self.event) + ", notificationId=" + repr(self.notification_id)
        + ", occurredOn=" + repr(self.occured_on) + ", typeName="
        + repr(self.type_name) + ", version=" + repr(self.version) + "]")

    def to_json(self):
        return {
            'notification_id': self.notification_id,
            'event': self.event,
            'occured_on': self._serialize_datetime(self.occured_on),
            'type_name': self.type_name,
            'version': self.version
        }


class PublishedNotificationTracker(object):
    def __init__(self, type_name):
        self._type_name = type_name
        self._most_recent_published_notification_id = None

    @property
    def type_name(self):
        return self._type_name

    @property
    def most_recent_published_notification_id(self):
        return self._most_recent_published_notification_id

    def set_most_recent_published_notification_id(self, value):
        self._most_recent_published_notification_id = value

    def __eq__(self, other):
        if isinstance(other, PublishedNotificationTracker):
            return self.type_name == other.type_name

    def __repr__(self):
        return ("PublishedNotificationTracker [mostRecentPublishedNotificationId=" + self.most_recent_published_notification_id
        +", typeName=" + self.type_name + "]")


class NotificationReader(object):
    def __init__(self, json):
        self._json = simplejson.loads(json) if isinstance(json, basestring) else json
        self._event = self._json

    @property
    def version(self):
        return self._json['event_version']

    @property
    def occured_on(self):
        return self._json['occured_on']

    @property
    def notification_id(self):
        return self._json['notification_id']

    @property
    def occured_on(self):
        return self.to_datetime(self._json['occured_on'])

    @property
    def type_name(self):
        return self._json['type_name']

    def _get_value(self, key):
        original_key = key
        json = self._event
        original_json = json

        try:
            while '.' in key:
                parent, key = key.split('.', 1)
                json = json[parent]

            if json is None:
                raise KeyError('Item at %r is None' % original_key)
            return json[key]
        except KeyError:
            raise KeyError("Can't find %r in %r" % (key, json))

    def string(self, key):
        value = self._get_value(key)

        if value is True or value is False:
            return 'true' if value else 'false'
        return unicode(value) if value is not None else value

    def int(self, key):
        value = self._get_value(key)
        return int(value)if value is not None else value

    def to_datetime(self, value):
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S') if value is not None else value

    def datetime(self, key):
        return self.to_datetime(self._get_value(key))

    def date(self, key):
        value = self._get_value(key)
        return datetime.strptime(value, '%Y-%m-%d').date() if value is not None else value

    def bool(self, key):
        value = self._get_value(key)
        return bool(value) if value is not None else value

    def contains(self, key):
        try:
            self._get_value(key)
            return True
        except KeyError:
            return False

    def decimal(self, key):
        value = self._get_value(key)
        return Decimal(value) if value is not None else value