.. -*- rst -*-

========
Services
========

Lists, creates, updates, shows details for, and deletes services.


List Services
=============

.. rest_method:: GET /services

Lists all services.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Response
--------

.. rest_parameters:: parameters.yaml

    - services : services_array
    - created_at : service_created_at
    - updated_at : service_updated_at
    - deleted_at : service_deleted_at
    - deleted : service_deleted
    - id : service_id
    - host : service_host
    - binary : service_binary
    - topic : service_topic
    - enabled : service_enabled
    - status : service_status
    - providers : service_providers
    - specs : service_specs
    - mapped_regions : service_mapped_regions

**Example of Service List Response**

    .. literalinclude:: ../api_samples/service/service-list-resp.json
        :language: javascript

Show service
============

.. rest_method:: GET /services/{service_id}

Shows details about a service.

**Preconditions**

The service must exist.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - service_id : service_id_path

Response
--------
.. rest_parameters:: parameters.yaml

    - service : service_object
    - created_at : service_created_at
    - updated_at : service_updated_at
    - deleted_at : service_deleted_at
    - deleted : service_deleted
    - id : service_id
    - host : service_host
    - binary : service_binary
    - topic : service_topic
    - enabled : service_enabled
    - status : service_status
    - providers : service_providers
    - specs : service_specs
    - mapped_regions : service_mapped_regions

**Example of Service Show Response**

    .. literalinclude:: ../api_samples/service/service-show-resp.json
        :language: javascript

Create Service
==============

.. rest_method:: POST /services

Creates a service.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

    - service : service_object
    - host : service_host
    - binary : service_binary
    - topic : service_topic
    - enabled : service_enabled
    - mapped_regions : service_mapped_regions

**Example of Service Create Request**

    .. literalinclude:: ../api_samples/service/service-create-req.json
        :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

    - service : service_object
    - created_at : service_created_at
    - updated_at : service_updated_at
    - deleted_at : service_deleted_at
    - deleted : service_deleted
    - id : service_id
    - host : service_host
    - binary : service_binary
    - topic : service_topic
    - enabled : service_enabled
    - status : service_status
    - providers : service_providers
    - specs : service_specs
    - mapped_regions : service_mapped_regions

**Example of Service Create Response**

    .. literalinclude:: ../api_samples/service/service-create-resp.json
        :language: javascript

Update service
==============

.. rest_method:: PUT /services/{service_id}

Updates the already existing service, modifying the mapped regions or availability.

**Preconditions**

The service must exist.

Normal Response Codes: 200

Error response codes:   unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - service_id : service_id_path
    - service : service_object
    - mapped_regions : service_mapped_regions
    - enabled : service_enabled

**Example of Service Update Request**

    .. literalinclude:: ../api_samples/service/service-update-req.json
        :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

    - service : service_object
    - created_at : service_created_at
    - updated_at : service_updated_at
    - deleted_at : service_deleted_at
    - deleted : service_deleted
    - id : service_id
    - host : service_host
    - binary : service_binary
    - topic : service_topic
    - enabled : service_enabled
    - status : service_status
    - providers : service_providers
    - specs : service_specs
    - mapped_regions : service_mapped_regions

**Example of service Update Response**

    .. literalinclude:: ../api_samples/service/service-update-resp.json
        :language: javascript

Delete service
==============

.. rest_method:: DELETE /services/{service_id}

Deletes a service.

**Preconditions**

The service must exist.

Normal response codes: 204

Error response codes: itemNotFound(404),unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

    - service_id : service_id_path

Response
--------

There is no response body for a successful deletion of a service.
