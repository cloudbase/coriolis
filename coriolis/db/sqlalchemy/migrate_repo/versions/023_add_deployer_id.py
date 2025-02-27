import sqlalchemy


def upgrade(migrate_engine):
    meta = sqlalchemy.MetaData()
    meta.bind = migrate_engine

    deployment = sqlalchemy.Table('deployment', meta, autoload=True)
    deployer_id = sqlalchemy.Column(
        'deployer_id', sqlalchemy.String(36), nullable=True)
    trust_id = sqlalchemy.Column(
        'trust_id', sqlalchemy.String(255), nullable=True)
    created_columns = []
    try:
        deployment.create_column(deployer_id)
        created_columns.append(deployer_id)
        deployment.create_column(trust_id)
        created_columns.append(trust_id)
    except Exception:
        for c in created_columns:
            deployment.drop_column(c)
        raise
