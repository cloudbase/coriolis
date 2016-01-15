from oslo_db.sqlalchemy import models
from sqlalchemy.ext import declarative
from sqlalchemy.orm import relationship, backref
from sqlalchemy import (Column, Index, Integer, BigInteger, Enum, String,
                        schema, Unicode)
from sqlalchemy import ForeignKey, DateTime, Boolean, Text, Float

BASE = declarative.declarative_base()


class Operation(BASE, models.TimestampMixin, models.ModelBase):
    __tablename__ = 'operation'

    id = Column(String(36), default=lambda: str(uuid.uuid4()),
                primary_key=True)
    migration_id = Column(Integer,
                          ForeignKey('migration.id'),
                          nullable=False)
    # migration = relationship("Migration",
    # backref=backref("operations"), lazy='joined')
    instance = Column(String(1024), nullable=False)
    host = Column(String(1024), nullable=True)
    status = Column(String(100), nullable=False)
    operation_type = Column(String(100), nullable=False)


class Migration(BASE, models.TimestampMixin, models.ModelBase):
    __tablename__ = 'migration'

    id = Column(Integer, primary_key=True)
    user_id = Column(String(255), nullable=False)
    origin = Column(String(1024), nullable=False)
    destination = Column(String(1024), nullable=False)
    status = Column(String(100), nullable=False)
    operations = relationship(Operation, cascade="all,delete",
                              backref=backref('migration'))
