.. -*- rst -*-

===================
Provider Operations
===================

Lists all the possible types of cloud service connections (providers) and the required schemas.

List Providers
=====================

.. rest_method:: GET /providers

Lists all providers.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Response
--------

.. rest_parameters:: parameters.yaml

  - providers : providers
  - types : provider_types

**Example of List Providers**
  .. literalinclude:: ../api_samples/provider/provider-get-resp.json
     :language: javascript

List Provider Schemas
=====================

.. rest_method:: GET /providers/{platform}/schemas/{provider_type}

Lists a provider schema by given platform and provider type.

**Preconditions**
The platform must support the provider type requested.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

    - platform : platform
    - provider_type : provider_id

Response
--------

.. rest_parameters:: parameters.yaml

  - schemas : provider_schemas
  - connection_info_schema : connection_info_schema
  - destination_environment_schema : destination_environment_schema
  - source_environment_schema : source_environment_schema

**Example of List Providers Schemas Response**
  .. literalinclude:: ../api_samples/provider/openstack-connection-provider-schema-get-resp.json
     :language: javascript
