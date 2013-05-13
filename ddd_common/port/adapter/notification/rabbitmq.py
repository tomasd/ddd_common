import simplejson
from ddd_common.notification import Notification
from ddd_common.port.adapter.messaging.rabbitmq import to_timestamp


class RabbitMQNotificationPublisher(object):
    def __init__(self, event_store, published_notification_tracker_store, exchange):
        self._event_store = event_store
        self._published_notification_tracker_store = published_notification_tracker_store
        self._exchange = exchange

    def publish_notifications(self):
        notification_tracker = self._published_notification_tracker_store.published_notification_tracker()

        notifications = self._list_unpublished_notifications(notification_tracker.most_recent_published_notification_id)

        message_producer = self._message_producer()

        with message_producer:
            for notification in notifications:
                self._publish(notification, message_producer)

            self._published_notification_tracker_store.track_most_recent_published_notification(notification_tracker,notifications)

    def _list_unpublished_notifications(self, most_recent_published_notification_id):
        stored_events = self._event_store.all_stored_events_since(most_recent_published_notification_id)

        notifications = self._notifications_from(stored_events)

        return list(notifications)

    def _notifications_from(self, stored_events):
        for event in stored_events:
            notification = Notification.from_stored_event(event.event_id, event)

            yield notification

    def _publish(self, notification, message_producer):
        headers = dict(
            message_id = str(notification.notification_id),
            timestamp = to_timestamp(notification.occured_on),
            type = notification.type_name
        )

        serialized_notification = notification.event

        message_producer.send(
            notification.type_name, serialized_notification, headers
        )

    def _message_producer(self):
        return self._exchange.message_producer()