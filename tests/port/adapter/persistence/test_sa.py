from hamcrest import assert_that, is_
import mock
from ddd_common.application import transactional
from ddd_common.port.adapter.persistence.sa import wrap_object_in_transaction


class MyService(object):
    def __init__(self, delegate):
        self.delegate = delegate

    @transactional
    def do_something(self, param):
        return self.delegate.do_something(param)


def test_wrap_object_in_transaction_commit():
    session = mock.MagicMock(name='session')
    delegate = mock.MagicMock(name='delegate')
    delegate.do_something.return_value = 'new value'

    service = wrap_object_in_transaction(session, MyService(delegate))

    returned_value = service.do_something('value')
    assert_that(returned_value, is_('new value'))

    session.commit.assert_called_once_with()


def test_wrap_object_in_transaction_rollback():
    session = mock.MagicMock(name='session')
    delegate = mock.MagicMock(name='delegate')

    delegate.do_something.side_effect = Exception()

    service = wrap_object_in_transaction(session, MyService(delegate))

    try:
        service.do_something('value')
        assert_that(True, is_(False))
    except Exception:
        pass

    session.rollback.assert_called_once_with()