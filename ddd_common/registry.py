class BaseApplication(object):
    def __init__(self, application_registry, domain_registry,
                 infrastructure_registry, listeners):
        self.app = application_registry
        self.domain = domain_registry
        self.infrastructure = infrastructure_registry
        self.listeners = listeners


class TestingRegistryMixin(object):
    @property
    def events(self):
        return self.infrastructure.event_store.events

    def dispatch(self, exchange, type_name, message):
        for listener in self.listeners:
            if listener.can_dispatch(exchange, type_name):
                listener.dispatch(message)

    @property
    def client(self):
        return self.flask.test_client()

    def clear_events(self):
        while self.events:
            self.events.pop()
