from ddd_common import fullname
from ddd_common.event import StoredEvent, serialize_event


class MockEventStore(object):
    def __init__(self):
        self._stored_events = []
        self.events = []
        self.next_id = 1

    def all_stored_events_between(self, low_stored_event_id, high_stored_event_id):
        return [a for a in self._stored_events if low_stored_event_id <= a.event_id <= high_stored_event_id]

    def all_stored_events_since(self, low_stored_event_id):
        return [a for a in self._stored_events if a.event_id > low_stored_event_id]

    def append(self, domain_event):
        type_name = fullname(domain_event)
        stored_event = StoredEvent(type_name, domain_event.occured_on,
                                   serialize_event(domain_event), event_id=self.next_id)
        self.next_id += 1
        self._stored_events.append(stored_event)
        self.events.append(domain_event)


    def close(self):
        pass

    def count_stored_events(self):
        return len(self._stored_events)
