from werkzeug.local import LocalProxy


class ApplicationWrapper(object):
    def __init__(self, application=None):
        self._application = application

    def init_app(self, flask_app, application):
        self._init(flask_app, application)

    def _init(self, flask_app, application):
        self._application = application

    def registry(self):
        return LocalProxy(lambda: self._application.application_registry)

    def application(self):
        return LocalProxy(lambda: self._application)