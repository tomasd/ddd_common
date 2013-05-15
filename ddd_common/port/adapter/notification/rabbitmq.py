from ddd_common.notification import Notification
from ddd_common.port.adapter.messaging.rabbitmq import to_timestamp


class RabbitMQNotificationPublisher(object):
    def __init__(self, event_store, published_notification_tracker_store, exchange, connection_factory):
        self._event_store = event_store
        self._published_notification_tracker_store = published_notification_tracker_store
        self._exchange = exchange
        self._connection_factory = connection_factory

    def publish_notifications(self):
        notification_tracker = self._published_notification_tracker_store.published_notification_tracker()

        notifications = self._list_unpublished_notifications(notification_tracker.most_recent_published_notification_id)

        with self._connection_factory as connection:
            with connection.Producer(exchange=self._exchange.exchange(connection)) as producer:
                for notification in notifications:
                    self._publish(notification, producer)

        self._published_notification_tracker_store.track_most_recent_published_notification(notification_tracker, notifications)

    def _list_unpublished_notifications(self, most_recent_published_notification_id):
        stored_events = self._event_store.all_stored_events_since(most_recent_published_notification_id)

        notifications = self._notifications_from(stored_events)

        return list(notifications)

    def _notifications_from(self, stored_events):
        for event in stored_events:
            notification = Notification.from_stored_event(event.event_id, event)

            yield notification

    def _publish(self, notification, producer):
        headers = dict(
            message_id = str(notification.notification_id),
            timestamp = to_timestamp(notification.occured_on),
            type = notification.type_name
        )

        serialized_notification = notification.event

        producer.publish(
            routing_key=notification.type_name,
            body=serialized_notification,
            headers=headers,
            content_type='application/json'
        )