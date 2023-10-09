import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    task_event = sqlalchemy.Table('task_event', meta, autoload=True)
    event_index = sqlalchemy.Column(
        "index", sqlalchemy.Integer, default=0, nullable=False)
    task_event.create_column(event_index)

    task_progress_update = sqlalchemy.Table(
        'task_progress_update', meta, autoload=True)
    progress_index = sqlalchemy.Column(
        "index", sqlalchemy.Integer, default=0, nullable=False)
    task_progress_update.create_column(progress_index)
    task_progress_update.c.current_step.alter(type=sqlalchemy.BigInteger)
    task_progress_update.c.total_steps.alter(type=sqlalchemy.BigInteger)
