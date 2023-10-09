# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.


#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import zlib

from oslo_serialization import jsonutils
from sqlalchemy.dialects import mysql
from sqlalchemy import types


class LongText(types.TypeDecorator):
    impl = types.Text

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(mysql.LONGTEXT())
        else:
            return self.impl


class Blob(types.TypeDecorator):
    impl = types.LargeBinary

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(mysql.BLOB(4294967295))
        else:
            return self.impl


class Json(LongText):

    def process_bind_param(self, value, dialect):
        return jsonutils.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return jsonutils.loads(value)


class Bson(Blob):

    def process_bind_param(self, value, dialect):
        return zlib.compress(
            jsonutils.dumps(value).encode('utf-8'))

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        data = None
        try:
            data = zlib.decompress(value)
        except Exception:
            data = value
        return jsonutils.loads(data)


class List(types.TypeDecorator):
    impl = types.Text

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(mysql.LONGTEXT())
        else:
            return self.impl

    def process_bind_param(self, value, dialect):
        return jsonutils.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return jsonutils.loads(value)
