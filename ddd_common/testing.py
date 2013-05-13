from brownie.importing import import_string
from hamcrest import has_item
from hamcrest.core.base_matcher import BaseMatcher


class IsEventOfType(BaseMatcher):
    def __init__(self, event_class):
        self._event_class = event_class

    def _matches(self, item):
        return isinstance(item, self._event_class)

    def describe_to(self, description):
        description.append_text('event of type %s was not found' % self._event_class)

def has_event(event):
    if isinstance(event, basestring):
        event = import_string(event)

    return has_item(IsEventOfType(event))