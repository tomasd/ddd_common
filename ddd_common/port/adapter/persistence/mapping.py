import sqlalchemy as sa
import sqlalchemy.orm
from ddd_common.event import StoredEvent
from ddd_common.notification import PublishedNotificationTracker

metadata = sa.MetaData()


def create_schema(bind):
    metadata.create_all(bind=bind)


stored_events = sa.Table(
    'stored_events',
    metadata,
    sa.Column('event_body', sa.String),
    sa.Column('event_id', sa.Integer, primary_key=True),
    sa.Column('occured_on', sa.DateTime, nullable=False),
    sa.Column('type_name', sa.String, nullable=False)
)
sa.orm.mapper(StoredEvent, stored_events, properties={
    '_event_id': stored_events.c.event_id,
    '_type_name': stored_events.c.type_name,
    '_occured_on': stored_events.c.occured_on,
    '_event': stored_events.c.event_body,
})

published_notification_tracker = sa.Table(
    'published_notification_tracker',
    metadata,
    sa.Column('type_name', sa.String, primary_key=True),
    sa.Column('most_recent_published_notification_id', sa.Integer),
    sa.Column('concurrency_version', sa.Integer)
)

sa.orm.mapper(
    PublishedNotificationTracker, published_notification_tracker,
    properties={
       '_type_name':published_notification_tracker.c.type_name,
       '_most_recent_published_notification_id':published_notification_tracker.c.most_recent_published_notification_id,
    },
    version_id_col=published_notification_tracker.c.concurrency_version
)