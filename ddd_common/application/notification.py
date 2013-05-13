from ddd_common.application import service


class NotificationService(object):
    def __init__(self, notification_publisher):
        self._notification_publisher = notification_publisher

    @service
    def publish_notifications(self):
        self._notification_publisher.publish_notifications()