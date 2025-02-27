import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    base_transfer = sqlalchemy.Table(
        'base_transfer_action', meta, autoload=True)
    clone_disks = sqlalchemy.Column(
        "clone_disks", sqlalchemy.Boolean, nullable=False, default=True)
    base_transfer.create_column(clone_disks)
    skip_os_morphing = sqlalchemy.Column(
        "skip_os_morphing", sqlalchemy.Boolean, nullable=False, default=False)
    base_transfer.create_column(skip_os_morphing)
