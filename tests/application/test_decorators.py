from collections import namedtuple
from hamcrest import assert_that, is_
import mock
from ddd_common.application import events, store_all_events
from ddd_common.domain.model import publisher

MyEvent = namedtuple('MyEvent', 'value')


class MyService(object):
    def __init__(self, delegate):
        self.delegate = delegate

    @events
    def do_something(self, value):
        publisher().publish(MyEvent(value))
        return self.delegate.do_something(value)


def test_store_all_events():
    delegate = mock.MagicMock(name='delegate')
    delegate.do_something.return_value = 'new value'
    event_store = mock.MagicMock(name='event_store')

    service = store_all_events(event_store, MyService(delegate))

    result = service.do_something('value')

    assert_that(result, is_('new value'))

    event_store.append.assert_called_once_with(MyEvent('value'))