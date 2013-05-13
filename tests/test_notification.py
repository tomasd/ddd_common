from datetime import date, datetime
from hamcrest import assert_that, is_
import simplejson
from ddd_common.event import serialize_event, Event
from ddd_common.notification import NotificationReader


class MyEvent(Event):
    def __init__(self, param1_str, param2_int, param3_datetime, param4_date,
                 param5_bool, complex):
        super(MyEvent, self).__init__()
        self.param1_str = param1_str
        self.param2_int = param2_int
        self.param3_datetime = param3_datetime
        self.param4_date = param4_date
        self.param5_bool = param5_bool
        self.complex = complex


def test_notification_reader():
    json = serialize_event(
        MyEvent('value1', 1, datetime(2010, 1, 1), date(2010, 1, 1), True, {'param':1}))

    reader = NotificationReader(json)

    assert_that(reader.string('param1_str'), is_('value1'))
    assert_that(reader.int('param2_int'), is_(1))
    assert_that(reader.string('param2_int'), is_('1'))
    assert_that(reader.datetime('param3_datetime'), is_(datetime(2010, 1, 1)))
    assert_that(reader.string('param3_datetime'), is_('2010-01-01 00:00:00'))
    assert_that(reader.date('param4_date'), is_(date(2010, 1, 1)))
    assert_that(reader.string('param4_date'), is_('2010-01-01'))
    assert_that(reader.bool('param5_bool'), is_(True))
    assert_that(reader.string('param5_bool'), is_('true'))

    assert_that(reader.int('complex.param'), is_(1))