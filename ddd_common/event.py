from datetime import datetime, date
from brownie.importing import import_string
import simplejson

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'


class JsonSerializableMixin(object):
    def to_json(self):
        return self.__dict__


    def _serialize_datetime(self, value):
        return value.strftime(DATETIME_FORMAT)

    @classmethod
    def _deserialize_datatime(cls, value):
        return datetime.strptime(value, DATETIME_FORMAT)


class Event(JsonSerializableMixin):
    def __init__(self, event_version=1):
        self.event_version = event_version
        self.occured_on = datetime.now()

    def to_json(self):
        data = {key: getattr(self, key) for key in dir(self) if not key.startswith('_') and not callable(getattr(self, key))}

        child = self._to_json_child()
        if child:
            data.update(child)
        return data

    def _to_json_child(self):
        pass

    def __repr__(self):
        return '%s(%s)' % (self.__class__, ', '.join(['%s=%s' % a for a in self.to_json().iteritems()]))




class StoredEvent(object):
    def __init__(self, type_name, occured_on, event, event_id=None):
        self._event_id = event_id
        self._type_name = type_name
        self._occured_on = occured_on
        self._event = event

    @property
    def event_id(self):
        return self._event_id

    @property
    def type_name(self):
        return self._type_name

    @property
    def occured_on(self):
        return self._occured_on

    @property
    def event(self):
        return self._event

    def __repr__(self):
        return ("StoredEvent [eventBody=" + repr(self._event) + ", eventId=" + repr(self._event_id) + ", occurredOn=" + repr(self._occured_on) + ", typeName="
        + repr(self._type_name) + "]")


def serialize_event(event):
    def default(o):
        if isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(o, date):
            return o.strftime('%Y-%m-%d')
        elif hasattr(o, 'to_json'):
            return o.to_json()
        raise TypeError("Can't convert %r to json" % (
            o.__class__ if o is not None else 'None'))

    return simplejson.dumps(event.to_json(), default=default)
