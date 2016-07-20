import json
import jsonschema
import mock

import jinja2

from coriolis import schemas
from coriolis.tests import test_base


RENDERED_TEMPLATE_SENTINEL = mock.sentinel.some_string_schema


def _get_mock_template_env():
    temp = mock.MagicMock()
    temp.render.return_value = RENDERED_TEMPLATE_SENTINEL

    tempenv = mock.MagicMock()
    tempenv.get_template.return_value = temp

    return tempenv


class SchemasTestCase(test_base.CoriolisBaseTestCase):
    """ Collection of tests for the Coriolis schemas package. """

    def setUp(self):
        super(SchemasTestCase, self).setUp()

    def _assert_tempenv_calls(self, mock_tempenv, temp_name):
        mock_tempenv.get_template.assert_called_once_with(temp_name)
        mock_tempenv.get_template().render.assert_called_once_with()

    @mock.patch.object(jinja2, 'Environment')
    @mock.patch.object(jinja2, 'PackageLoader')
    @mock.patch.object(json, 'loads')
    def test_get_schema(self, mock_loads, mock_loader, mock_environ):
        test_schema_name = mock.sentinel.schema_name
        test_package_name = mock.sentinel.package_name

        test_loader = mock.sentinel.loader
        mock_loader.return_value = test_loader

        test_rendered_template = mock.sentinel.rendered_template
        mock_template = mock.MagicMock()
        mock_template.render.return_value = test_rendered_template

        mock_env = mock.MagicMock()
        mock_env.get_template.return_value = mock_template

        mock_environ.return_value = mock_env

        test_loaded_schema = mock.sentinel.loaded_schema
        mock_loads.return_value = test_loaded_schema

        res = schemas.get_schema(test_package_name, test_schema_name)

        mock_loader.assert_called_once_with(
            test_package_name, schemas.DEFAULT_SCHEMAS_DIRECTORY)
        mock_environ.assert_called_once_with(loader=test_loader)
        mock_env.get_template.assert_called_once_with(test_schema_name)
        mock_loads.assert_called_once_with(test_rendered_template)

        self.assertEqual(res, test_loaded_schema)

    @mock.patch.object(jsonschema, 'validate')
    def test_validate_value(self, mock_validate):
        test_value = mock.sentinel.test_value
        test_schema = mock.sentinel.test_schema

        schemas.validate_value(test_value, test_schema)

        mock_validate.assert_called_once_with(test_value, test_schema)

    @mock.patch.object(json, 'loads')
    @mock.patch.object(jsonschema, 'validate')
    def test_validate_string(self, mock_validate, mock_loads):
        test_value = mock.sentinel.test_value
        test_string = mock.sentinel.test_string
        test_schema = mock.sentinel.test_schema

        mock_loads.return_value = test_value

        schemas.validate_string(test_string, test_schema)

        mock_loads.assert_called_once_with(test_string)
        mock_validate.assert_called_once_with(test_value, test_schema)
