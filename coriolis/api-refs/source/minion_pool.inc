.. -*- rst -*-

============
Minion Pools
============

Lists, creates, updates, shows details for and deletes minion pools. It also allocates, deallocates and refreshes information for minion machines for minion pools.


List Minion Pools
=================

.. rest_method:: GET /minion_pools

List details for all minion pools.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Response
--------

.. rest_parameters:: parameters.yaml

  - minion_pools : minion_pools_array
  - id : minion_pool_id
  - name : minion_pool_name
  - notes : minion_pool_notes
  - endpoint_id : minion_pool_endpoint_id
  - environment_options : minion_pool_environment_options
  - os_type : minion_pool_os_type
  - maintenance_trust_id : minion_pool_maintenance_trust_id
  - platform : minion_pool_platform
  - created_at : minion_pool_created_at
  - updated_at : minion_pool_updated_at
  - deleted_at : minion_pool_deleted_at
  - deleted : minion_pool_deleted
  - shared_resources : minion_pool_shared_resources
  - status : minion_pool_status
  - minimum_minions : minimum_minions
  - maximum_minions : maximum_minions
  - minion_max_idle_time : minion_max_idle_time
  - minion_retention_strategy : minion_pool_retention_strategy
  - minion_machines : minion_pool_machines

**Example of Minion Pool List Response**

.. literalinclude:: ../api_samples/minion_pool/minion-pool-list-resp.json
    :language: javascript


Show Minion Pool
================

.. rest_method:: GET /minion_pools/{minion_pool_id}

Shows details about a minion pool.

**Preconditions**

The minion pool must exist.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - minion_pool_id : minion_pool_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - minion_pool : minion_pool_object
  - id : minion_pool_id
  - name : minion_pool_name
  - notes : minion_pool_notes
  - endpoint_id : minion_pool_endpoint_id
  - environment_options : minion_pool_environment_options
  - os_type : minion_pool_os_type
  - maintenance_trust_id : minion_pool_maintenance_trust_id
  - platform : minion_pool_platform
  - created_at : minion_pool_created_at
  - updated_at : minion_pool_updated_at
  - deleted_at : minion_pool_deleted_at
  - deleted : minion_pool_deleted
  - shared_resources : minion_pool_shared_resources
  - status : minion_pool_status
  - minimum_minions : minimum_minions
  - maximum_minions : maximum_minions
  - minion_max_idle_time : minion_max_idle_time
  - minion_retention_strategy : minion_pool_retention_strategy
  - minion_machines : minion_pool_machines
  - user_id : user_id
  - project_id : project_id
  - events : minion_pool_events
  - progress_updates : minion_pool_progress_updates

**Example of Minion Pool Show Response**

  .. literalinclude:: ../api_samples/minion_pool/openstack-minion-pool-show.json
      :language: javascript

Create Minion Pool
==================

.. rest_method:: POST /minion_pools

Creates a minion pool.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

  - minion_pool : minion_pool_object
  - name : minion_pool_name
  - notes : minion_pool_notes
  - endpoint_id : minion_pool_endpoint_id
  - environment_options : minion_pool_environment_options
  - os_type : minion_pool_os_type
  - platform : minion_pool_platform
  - minimum_minions : minimum_minions
  - maximum_minions : maximum_minions
  - minion_max_idle_time : minion_max_idle_time
  - minion_retention_strategy : minion_pool_retention_strategy
  - skip_allocation : minion_pool_skip_allocation

**Example of Minion Pool Create Request**

  .. literalinclude:: ../api_samples/minion_pool/openstack-minion-pool-create-req.json
      :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - minion_pool : minion_pool_object
  - id : minion_pool_id
  - name : minion_pool_name
  - notes : minion_pool_notes
  - endpoint_id : minion_pool_endpoint_id
  - environment_options : minion_pool_environment_options
  - os_type : minion_pool_os_type
  - maintenance_trust_id : minion_pool_maintenance_trust_id
  - platform : minion_pool_platform
  - created_at : minion_pool_created_at
  - updated_at : minion_pool_updated_at
  - deleted_at : minion_pool_deleted_at
  - deleted : minion_pool_deleted
  - shared_resources : minion_pool_shared_resources
  - status : minion_pool_status
  - minimum_minions : minimum_minions
  - maximum_minions : maximum_minions
  - minion_max_idle_time : minion_max_idle_time
  - minion_retention_strategy : minion_pool_retention_strategy
  - minion_machines : minion_pool_machines
  - user_id : user_id
  - project_id : project_id
  - events : minion_pool_events
  - progress_updates : minion_pool_progress_updates

**Example of Minion Pool Create Response**

  .. literalinclude:: ../api_samples/minion_pool/openstack-minion-pool-create-resp.json
      :language: javascript

Update Minion Pool
==================

.. rest_method:: PUT /minion_pools/{minion_pool_id}

Updates a minion pool.

**Preconditions**

The minion pool must exist and be in DEALLOCATED state.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

  .. rest_parameters:: parameters.yaml

    - minion_pool_id : minion_pool_id_path
    - minion_pool : minion_pool_object
    - name : minion_pool_name
    - notes : minion_pool_notes
    - endpoint_id : minion_pool_endpoint_id
    - environment_options : minion_pool_environment_options
    - os_type : minion_pool_os_type
    - platform : minion_pool_platform
    - minimum_minions : minimum_minions
    - maximum_minions : maximum_minions
    - minion_max_idle_time : minion_max_idle_time
    - minion_retention_strategy : minion_pool_retention_strategy
    - notes : minion_pool_notes

**Example of Minion Pool Update Request**

.. literalinclude:: ../api_samples/minion_pool/openstack-minion-pool-update-req.json
    :language: javascript

Response
--------

  .. rest_parameters:: parameters.yaml

    - minion_pool : minion_pool_object
    - id : minion_pool_id
    - name : minion_pool_name
    - notes : minion_pool_notes
    - endpoint_id : minion_pool_endpoint_id
    - environment_options : minion_pool_environment_options
    - os_type : minion_pool_os_type
    - maintenance_trust_id : minion_pool_maintenance_trust_id
    - platform : minion_pool_platform
    - created_at : minion_pool_created_at
    - updated_at : minion_pool_updated_at
    - deleted_at : minion_pool_deleted_at
    - deleted : minion_pool_deleted
    - shared_resources : minion_pool_shared_resources
    - status : minion_pool_status
    - minimum_minions : minimum_minions
    - maximum_minions : maximum_minions
    - minion_max_idle_time : minion_max_idle_time
    - minion_retention_strategy : minion_pool_retention_strategy
    - minion_machines : minion_pool_machines
    - user_id : user_id
    - project_id : project_id
    - events : minion_pool_events
    - progress_updates : minion_pool_progress_updates

**Example of Minion Pool Update Response**

.. literalinclude:: ../api_samples/minion_pool/openstack-minion-pool-update-resp.json
    :language: javascript

Delete Minion Pool
==================

.. rest_method:: DELETE /minion_pools/{minion_pool_id}

Deletes a Minion Pool.

**Preconditions**

The Minion Pool must exist and be in DEALLOCATED state.

Normal response codes: 204

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - minion_pool_id : minion_pool_id_path

Response
--------

There is no response body for a successful deletion.


Minion Pool Actions
===================

Minion Pool Allocate
====================

.. rest_method:: POST /minion_pools/{minion_pool_id}/actions

Allocates Minion Machines for a Minion Pool.

**Preconditions**

The minion pool must exist and be in DEALLOCATED state.


Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - minion_pool_id : minion_pool_id_path
  - allocate : minion_pool_allocate

**Example of Minion Pool Allocation Request**

  .. literalinclude:: ../api_samples/minion_pool/minion-pool-allocate-req.json
      :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - minion_pool : minion_pool_object
  - id : minion_pool_id
  - name : minion_pool_name
  - notes : minion_pool_notes
  - endpoint_id : minion_pool_endpoint_id
  - environment_options : minion_pool_environment_options
  - os_type : minion_pool_os_type
  - maintenance_trust_id : minion_pool_maintenance_trust_id
  - platform : minion_pool_platform
  - created_at : minion_pool_created_at
  - updated_at : minion_pool_updated_at
  - deleted_at : minion_pool_deleted_at
  - deleted : minion_pool_deleted
  - shared_resources : minion_pool_shared_resources
  - status : minion_pool_status
  - minimum_minions : minimum_minions
  - maximum_minions : maximum_minions
  - minion_max_idle_time : minion_max_idle_time
  - minion_retention_strategy : minion_pool_retention_strategy
  - minion_machines : minion_pool_machines
  - user_id : user_id
  - project_id : project_id
  - events : minion_pool_events
  - progress_updates : minion_pool_progress_updates

**Example of Minion Pool Allocation Response**

  .. literalinclude:: ../api_samples/minion_pool/openstack-minion-pool-allocate-resp.json
      :language: javascript

Minion Pool Deallocate
======================

.. rest_method:: POST /minion_pools/{minion_pool_id}/actions

Deallocates Minion Machines for a Minion Pool.

**Preconditions**

The minion pool must exist and be in ALLOCATED or ERROR state.


Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - minion_pool_id : minion_pool_id_path
  - deallocate : minion_pool_deallocate

**Example of Minion Pool Deallocation Request**

  .. literalinclude:: ../api_samples/minion_pool/minion-pool-deallocate-req.json
      :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - minion_pool : minion_pool_object
  - id : minion_pool_id
  - name : minion_pool_name
  - notes : minion_pool_notes
  - endpoint_id : minion_pool_endpoint_id
  - environment_options : minion_pool_environment_options
  - os_type : minion_pool_os_type
  - maintenance_trust_id : minion_pool_maintenance_trust_id
  - platform : minion_pool_platform
  - created_at : minion_pool_created_at
  - updated_at : minion_pool_updated_at
  - deleted_at : minion_pool_deleted_at
  - deleted : minion_pool_deleted
  - shared_resources : minion_pool_shared_resources
  - status : minion_pool_status
  - minimum_minions : minimum_minions
  - maximum_minions : maximum_minions
  - minion_max_idle_time : minion_max_idle_time
  - minion_retention_strategy : minion_pool_retention_strategy
  - minion_machines : minion_pool_machines
  - user_id : user_id
  - project_id : project_id
  - events : minion_pool_events
  - progress_updates : minion_pool_progress_updates

**Example of Minion Pool Deallocation Response**

  .. literalinclude:: ../api_samples/minion_pool/openstack-minion-pool-deallocate-resp.json
      :language: javascript

Minion Pool Refresh
======================

.. rest_method:: POST /minion_pools/{minion_pool_id}/actions

Refreshes Minion Machines for a Minion Pool.

**Preconditions**

The minion pool must exist and be in ALLOCATED state.


Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - minion_pool_id : minion_pool_id_path
  - refresh : minion_pool_refresh

**Example of Minion Pool Refresh Request**

  .. literalinclude:: ../api_samples/minion_pool/minion-pool-refresh-req.json
      :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - minion_pool : minion_pool_object
  - id : minion_pool_id
  - name : minion_pool_name
  - notes : minion_pool_notes
  - endpoint_id : minion_pool_endpoint_id
  - environment_options : minion_pool_environment_options
  - os_type : minion_pool_os_type
  - maintenance_trust_id : minion_pool_maintenance_trust_id
  - platform : minion_pool_platform
  - created_at : minion_pool_created_at
  - updated_at : minion_pool_updated_at
  - deleted_at : minion_pool_deleted_at
  - deleted : minion_pool_deleted
  - shared_resources : minion_pool_shared_resources
  - status : minion_pool_status
  - minimum_minions : minimum_minions
  - maximum_minions : maximum_minions
  - minion_max_idle_time : minion_max_idle_time
  - minion_retention_strategy : minion_pool_retention_strategy
  - minion_machines : minion_pool_machines
  - user_id : user_id
  - project_id : project_id
  - events : minion_pool_events
  - progress_updates : minion_pool_progress_updates

**Example of Minion Pool Refresh Response**

  .. literalinclude:: ../api_samples/minion_pool/openstack-minion-pool-refresh-resp.json
      :language: javascript
