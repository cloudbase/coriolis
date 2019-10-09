.. -*- rst -*-

=========
Endpoints
=========

Lists, creates, updates, shows details for, and deletes cloud endpoints.


List Endpoints
==============

.. rest_method:: GET /endpoints

Lists all endpoints.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Response
--------

.. rest_parameters:: parameters.yaml

  - endpoints : endpoints_array
  - created_at : endpoint_created_at
  - updated_at : endpoint_updated_at
  - deleted_at : endpoint_deleted_at
  - deleted : endpoint_deleted
  - id : endpoint_id
  - user_id : user_id
  - project_id : project_id
  - connection_info: connection_info
  - type : endpoint_type
  - name : endpoint_name
  - description : endpoint_description

**Example of Endpoint List Response**

  .. literalinclude:: ../api_samples/endpoint/endpoint-list-resp.json
     :language: javascript

Show Endpoint
=============

.. rest_method:: GET /endpoints/{endpoint_id}

Shows details about an endpoint.

**Preconditions**

The endpoint must exist

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - endpoint_id: endpoint_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - endpoint : endpoint_object
  - created_at : endpoint_created_at
  - updated_at : endpoint_updated_at
  - deleted_at : endpoint_deleted_at
  - deleted : endpoint_deleted
  - id : endpoint_id
  - user_id : user_id
  - project_id : project_id
  - connection_info: connection_info
  - type : endpoint_id
  - name : endpoint_created_at
  - description : endpoint_description

**Example of Endpoint Show Response**
  .. literalinclude:: ../api_samples/endpoint/openstack-endpoint-get-resp.json
     :language: javascript

.. note:: If the endpoint was created using the Web-UI, the connection_info property will be saved as a Barbican Secret, which will be returned like in the example below

**Example of Secret Endpoint Show Response**
  .. literalinclude:: ../api_samples/endpoint/endpoint-get-resp-secret.json
     :language: javascript


Update Endpoint
===============

.. rest_method:: PUT /endpoints/{endpoint_id}

**Preconditions**

The endpoint must exist.

Updates an already existing endpoint, modifying its name, description or
connection info.

Error response codes:   unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - endpoint : endpoint_object
   - endpoint_id : endpoint_id_path
   - name: endpoint_name
   - description : endpoint_description
   - connection_info : connection_info

**Example of Endpoint Update Request**

  .. literalinclude:: ../api_samples/endpoint/openstack-endpoint-update-req.json
     :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml


  - created_at : endpoint_created_at
  - updated_at : endpoint_updated_at
  - deleted_at : endpoint_deleted_at
  - deleted : endpoint_deleted
  - id : endpoint_id
  - user_id : user_id
  - project_id : project_id
  - connection_info: connection_info
  - type : endpoint_id
  - name : endpoint_created_at
  - description : endpoint_description

**Example of Endpoint Update Response**
  .. literalinclude:: ../api_samples/endpoint/openstack-endpoint-update-resp.json
     :language: javascript

Delete Endpoint
===============

.. rest_method:: DELETE endpoints/{endpoint_id}

Deletes an endpoint.

**Preconditions**

The endpoint must exist.

Normal response codes: 200

Error response codes: itemNotFound(404),unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

   - endpoint_id : endpoint_id_path

Response
--------

There is no response body for a successful deletion of an endpoint.

Get Endpoint Instances
======================

.. rest_method:: GET /endpoints/{endpoint_id}/instances

Lists the instances of an endpoint.

**Preconditions**

The endpoint must exist and the platform must support listing instances.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - endpoint_id : endpoint_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - instances : instance_array
  - limit : instance_limit
  - name : instance_identifier
  - num_cpu : instance_num_cpu
  - num_cores_per_socket : instance_cores_per_socket
  - nested_virtualization : instance_nested_virtualization
  - memory_mb : instance_memory_mb
  - instance_name : instance_name
  - id : instance_id
  - os_type : instance_os_type
  - flavor_name : instance_flavor

**Example Instance List on Openstack Endpoint**
  .. literalinclude:: ../api_samples/endpoint/endpoint-instance-list-resp.json
     :language: javascript

Get Endpoint Instance
=====================

.. rest_method:: GET /endpoints/{endpoint_id}/instances/{instance_name_base64}

Shows details for a specific instance for an endpoint.

The returned instance information will mandatorily contain the below-listed response fields for all
Coriolis Provider Plugins, while allowing for the plugins to expose extra fields at will.

**Preconditions**

The endpoint must exist and the platform must support listing instances.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - endpoint_id : endpoint_id_path
   - instance_name_base64 : instance_name_base64_path

Response
--------

.. rest_parameters:: parameters.yaml

  - instance : instance_object
  - name : instance_identifier
  - id : instance_id
  - os_type : instance_os_type
  - memory_mb : instance_memory_mb
  - instance_name : instance_name
  - num_cpu: instance_num_cpu
  - num_cores_per_socket : instance_cores_per_socket
  - nested_virtualization : instance_nested_virtualization
  - flavor_name : instance_flavor
  - devices : instance_devices

**Example Instance Show On Openstack Endpoint**

  .. literalinclude:: ../api_samples/endpoint/endpoint-instance-show-resp.json
     :language: javascript

Get Endpoint Destination Options
================================

.. rest_method:: GET /endpoints/{endpoint_id}/destination-options

Shows possible options for destination parameters

The returned destination endpoint information will mandatorily contain the below-listed response fields for all
Coriolis Provider Plugins.

**Preconditions**

The endpoint must exist and the platform must support listing options.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - endpoint_id : endpoint_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - destination_options : destination_options
  - values : option_values
  - config_default : option_config_default
  - name : option_name

**Example Destination Endpoint List On Openstack Endpoint**

  .. literalinclude:: ../api_samples/endpoint/openstack-endpoint-destination-options-list-resp.json
     :language: javascript

Get Endpoint Source Options
===========================

.. rest_method:: GET /endpoints/{endpoint_id}/source-options

Shows possible options for source parameters

The returned source endpoint information will mandatorily contain the below-listed response fields for Coriolis Provider Plugins that support listing source options.

**Preconditions**

The endpoint must exist and must support listing options.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - endpoint_id : endpoint_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - source_options : source_options
  - values : option_values
  - config_default : option_config_default
  - name : option_name

**Example Source Endpoint List On Openstack Endpoint**

  .. literalinclude:: ../api_samples/endpoint/openstack-endpoint-source-options-list-resp.json
     :language: javascript

Get Endpoint Networks
=====================

.. rest_method:: GET /endpoints/{endpoint_id}/networks

Lists all available networks on a given destination endpoint.

**Preconditions**
The endpoint must exist and the platform must support listing networks.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - endpoint_id : endpoint_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - networks : networks_array
  - name : network_name
  - id : network_id

**Example Get Endpoint Networks**

  .. literalinclude:: ../api_samples/endpoint/endpoint-network-list-resp.json
     :language: javascript


Get Endpoint Storage
====================

.. rest_method:: GET endpoints/{endpoint_id}/storage

Lists all available storage backends on a given destination endpoint.

**Preconditions**
The endpoint must exist.

Normal Response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - endpoint_id : endpoint_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - storage_backends : storage_backends
  - name : storage_name
  - id : storage_id

**Example Get Endpoint Storage**
  .. literalinclude:: ../api_samples/endpoint/endpoint-storage-list-resp.json
      :language: javascript

Create Endpoint
===============

.. rest_method:: POST /endpoints/

Creates a cloud endpoint.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

  - connection_info : connection_info
  - type : provider_type
  - name : endpoint_name
  - description : endpoint_description

**Example of Endpoint Create Request**

  .. literalinclude:: ../api_samples/endpoint/openstack-endpoint-create-req.json
     :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - endpoint : endpoint_object
  - name : endpoint_created_at
  - description : endpoint_description
  - id : endpoint_id
  - connection_info: connection_info
  - deleted : endpoint_deleted
  - deleted_at : endpoint_deleted_at
  - created_at : endpoint_created_at
  - updated_at : endpoint_updated_at
  - type : endpoint_type
  - user_id : user_id
  - project_id : project_id

**Example of Endpoint Create Response**

  .. literalinclude:: ../api_samples/endpoint/openstack-endpoint-get-resp.json
      :language: javascript


Endpoint Actions
================

Validate Endpoint Connection
============================

.. rest_method:: POST /endpoints/{endpoint_id}/actions

Validates connection to endpoint.

**Preconditions**

The endpoint must exist.


Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - validate_connection : validate_connection
  - endpoint_id : endpoint_id_path

**Example of Endpoint Validate Connection Request**
  .. literalinclude:: ../api_samples/endpoint/endpoint-validate-connection-req.json
      :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - validate_connection : validate_connection
  - valid : valid_connection
  - message : validation_message

**Example of Endpoint Validate Connection Response**
  .. literalinclude:: ../api_samples/endpoint/endpoint-validate-connection-resp.json
     :language: javascript

.. note:: In case the endpoint validation fails, an error message will be returned, as in the example below

**Example of Failed Endpoint Validation Response**
  .. literalinclude:: ../api_samples/endpoint/endpoint-validate-connection-error-resp.json
     :language: javascript
