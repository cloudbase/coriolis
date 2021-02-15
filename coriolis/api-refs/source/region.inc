.. -*- rst -*-

=======
Regions
=======

Lists, creates, updates, shows details for, and deletes regions.


List Regions
============

.. rest_method:: GET /regions

Lists all regions.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Response
--------

.. rest_parameters:: parameters.yaml

    - regions : regions_array
    - created_at : region_created_at
    - updated_at : region_updated_at
    - deleted_at : region_deleted_at
    - deleted : region_deleted
    - id : region_id
    - name : region_name
    - description : region_description
    - enabled : region_enabled
    - mapped_services : region_mapped_services
    - mapped_endpoints : region_mapped_endpoints

**Example of Region List Response**

    .. literalinclude:: ../api_samples/region/region-list-resp.json
        :language: javascript

Show Region
===========

.. rest_method:: GET /regions/{region_id}

Shows details about a region.

**Preconditions**

The region must exist.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - region_id : region_id_path

Response
--------
.. rest_parameters:: parameters.yaml

    - region: region_object
    - created_at : region_created_at
    - updated_at : region_updated_at
    - deleted_at : region_deleted_at
    - deleted : region_deleted
    - id : region_id
    - name : region_name
    - description : region_description
    - enabled : region_enabled
    - mapped_services : region_mapped_services
    - mapped_endpoints : region_mapped_endpoints

**Example of Region Show Response**

    .. literalinclude:: ../api_samples/region/region-show-resp.json
        :language: javascript

Create Region
=============

.. rest_method:: POST /regions

Creates a region.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

    - region : region_object
    - name : region_name
    - description : region_description
    - enabled : region_enabled

**Example of Region Create Request**

    .. literalinclude:: ../api_samples/region/region-create-req.json
        :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

    - region: region_object
    - created_at : region_created_at
    - updated_at : region_updated_at
    - deleted_at : region_deleted_at
    - deleted : region_deleted
    - id : region_id
    - name : region_name
    - description : region_description
    - enabled : region_enabled
    - mapped_services : region_mapped_services
    - mapped_endpoints : region_mapped_endpoints

**Example of Region Create Response**

    .. literalinclude:: ../api_samples/region/region-create-resp.json
        :language: javascript

Update Region
=============

.. rest_method:: PUT /regions/{region_id}

Updates the already existing region, modifying its name, description or availability.

**Preconditions**

The region must exist.

Normal Response Codes: 200

Error response codes:   unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - region_id : region_id_path
    - region : region_object
    - name : region_name
    - description : region_description
    - enabled : region_enabled

**Example of Region Update Request**

    .. literalinclude:: ../api_samples/region/region-update-req.json
        :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

    - region: region_object
    - created_at : region_created_at
    - updated_at : region_updated_at
    - deleted_at : region_deleted_at
    - deleted : region_deleted
    - id : region_id
    - name : region_name
    - description : region_description
    - enabled : region_enabled
    - mapped_services : region_mapped_services
    - mapped_endpoints : region_mapped_endpoints

**Example of Region Update Response**

    .. literalinclude:: ../api_samples/region/region-update-resp.json
        :language: javascript

Delete Region
=============

.. rest_method:: DELETE /regions/{region_id}

Deletes a region.

**Preconditions**

The region must exist.

Normal response codes: 204

Error response codes: itemNotFound(404),unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

    - region_id : region_id_path

Response
--------

There is no response body for a successful deletion of a region.
