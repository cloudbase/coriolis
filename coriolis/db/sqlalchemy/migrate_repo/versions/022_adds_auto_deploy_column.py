import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    transfer_schedule = sqlalchemy.Table(
        'transfer_schedules', meta, autoload=True,
        mysql_engine="InnoDB",
        mysql_charset="utf8")
    auto_deploy = sqlalchemy.Column(
        'auto_deploy', sqlalchemy.Boolean, nullable=False, default=False)
    transfer_schedule.create_column(auto_deploy)
