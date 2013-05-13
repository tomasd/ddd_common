class MessageException(Exception):
    def __init__(self, message, cause=None, retry=False):
        super(MessageException, self).__init__(message)
        self.cause = cause
        self.retry = retry