.. -*- rst -*-

========
Replicas
========

Lists, creates, updates, shows details for, and deletes cloud replicas.


List Replicas
=============

.. rest_method:: GET /replicas

List all replicas.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Response
--------

.. rest_parameters:: parameters.yaml

  - replicas : replicas_array
  - id : replica_id
  - deleted : replica_deleted
  - deleted_at : replica_deleted_at
  - executions : replica_empty_execution_array
  - instances : replica_instances
  - notes : replica_notes
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : replica_type
  - origin_endpoint_id : replica_origin_endpoint_id
  - created_at : replica_created_at
  - updated_at : replica_updated_at
  - project_id : project_id
  - base_id : base_id
  - destination_endpoint_id : replica_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - last_execution_status : replica_last_execution_status
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings

**Example of Replica List Response**

.. literalinclude:: ../api_samples/replica/replica-list-resp.json
   :language: javascript


Show Replica
============

.. rest_method:: GET /replicas/{replica_id}

Shows details about a replica.

**Preconditions**

The replica must exist.


Error response codes:   unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - replica_id : replica_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - rpelica : replica_object
  - id : replica_id
  - deleted : replica_deleted
  - deleted_at : replica_deleted_at
  - executions : replica_execution_array
  - instances : replica_instances
  - notes : replica_notes
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : replica_type
  - origin_endpoint_id : replica_origin_endpoint_id
  - created_at : replica_created_at
  - updated_at : replica_updated_at
  - project_id : project_id
  - base_id : base_id
  - destination_endpoint_id : replica_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - last_execution_status : replica_last_execution_status
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings

**Example of Replica Show Response**

  .. literalinclude:: ../api_samples/replica/openstack-replica-get-resp.json
     :language: javascript

Create Replica
==============

.. rest_method:: POST /replicas

Creates a replica.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

  - replica : replica_object
  - destination_environment : replica_destination_environment
  - source_environment : replica_source_environment
  - destination_endpoint_id : replica_destination_endpoint_id
  - origin_endpoint_id : replica_origin_endpoint_id
  - instances : replica_instances
  - network_map : network_map
  - storage_mappings : storage_map
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - user_scripts : user_scripts

**Example of Replica Create Request**

  .. literalinclude:: ../api_samples/replica/openstack-replica-create-req.json
      :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - replica : replica_object
  - id : replica_id
  - deleted : replica_deleted
  - deleted_at : replica_deleted_at
  - executions : replica_execution_array
  - instances : replica_instances
  - notes : replica_notes
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : replica_type
  - origin_endpoint_id : replica_origin_endpoint_id
  - created_at : replica_created_at
  - updated_at : replica_updated_at
  - project_id : project_id
  - base_id : base_id
  - destination_endpoint_id : replica_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - user_scripts : user_scripts

**Example of Replica Create Response**

.. literalinclude:: ../api_samples/replica/openstack-replica-create-resp.json
    :language: javascript

Update Replica
==============

.. rest_method:: PUT /replicas/{replica_id}

Updates a replica.

**Preconditions**
The replica must exist.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

  .. rest_parameters:: parameters.yaml

      - replica_id : replica_id_path
      - destination_environment : replica_destination_environment
      - source_environment : replica_source_environment
      - network_map : replica_network_map
      - notes : replica_update_notes
      - storage_mappings : replica_storage_map

**Example of Replica Update Request**

.. literalinclude:: ../api_samples/replica/replica-update-req.json
    :language: javascript

Response
--------

  .. rest_parameters:: parameters.yaml

      - execution : replica_execution_object
      - number : replica_execution_number
      - action_id : replica_id
      - deleted : replica_execution_deleted
      - deleted_at : replica_execution_deleted_at
      - created_at : replica_execution_created_at
      - updated_at : replica_execution_updated_at
      - status : replica_execution_status
      - tasks : replica_execution_tasks
      - action : replica_execution_action
      - id : replica_execution_id
      - origin_minion_pool_id : origin_minion_pool_id
      - destination_minion_pool_id : destination_minion_pool_id
      - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
      - user_scripts : user_scripts

**Example of Replica Update Response**

.. literalinclude:: ../api_samples/replica/replica-update-resp.json
    :language: javascript


Delete Replica
==============

.. rest_method:: DELETE /replicas/{replica_id}

Deletes a replica.

**Preconditions**

The replica must exist and not have a running execution or migration.

Normal response codes: 204

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - replica_id : replica_id_path

Response
--------

There is no response body for a successful deletion.

Create Replica Execution
========================

.. rest_method:: POST /replicas/{replica_id}/executions

Executes a replica.

**Preconditions**

The replica must exist.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),

Request
-------

.. rest_parameters:: parameters.yaml

    - shutdown_instances : replica_shutdown_instances
    - replica_id : replica_id_path

**Example Replica Execution Create Request**

.. literalinclude:: ../api_samples/replica/replica-execution-req.json
    :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - execution : replica_execution
  - id : replica_execution_id
  - action_id : replica_id
  - deleted : replica_deleted
  - deleted_at : replica_execution_deleted_at
  - created_at : replica_execution_created_at
  - updated_at : replica_execution_updated_at
  - status : replica_execution_status
  - number : replica_execution_number
  - tasks : replica_execution_tasks
  - action : replica_execution_action
  - type : replica_execution_type

**Example Replica Execution Response**

  .. literalinclude:: ../api_samples/replica/openstack-replica-execution-resp.json
   :language: javascript

List Replica Executions
=======================

.. rest_method:: GET /replicas/{replica_id}/executions

List a replica's executions.

**Preconditions**
The replica must exist.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - replica_id : replica_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - executions : replica_execution_array
  - updated_at : replica_updated_at
  - action_id : replica_id
  - deleted : replica_execution_deleted
  - deleted_at : replica_execution_deleted_at
  - created_at : replica_execution_created_at
  - number : replica_execution_number
  - status : replica_execution_status
  - id : replica_execution_id
  - type : replica_execution_type

**Example Replica Execute Response**

  .. literalinclude:: ../api_samples/replica/replica-execution-list-resp.json
     :language: javascript

Show Replica Execution
======================

.. rest_method:: GET /replicas/{replica_id}/executions/{execution_id}

Show a replica's execution.

**Preconditions**
The replica execution must exist.

Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - replica_id : replica_id_path
  - execution_id : execution_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - execution : replica_execution_object
  - number : replica_execution_number
  - action_id : replica_id
  - deleted : replica_execution_deleted
  - deleted_at : replica_execution_deleted_at
  - created_at : replica_execution_created_at
  - updated_at : replica_execution_updated_at
  - status : replica_execution_status
  - tasks : replica_execution_tasks
  - action : replica_execution_action
  - id : replica_execution_id
  - type : replica_execution_type


**Example of Replica Executions Show Response**
  .. literalinclude:: ../api_samples/replica/openstack-replica-execution-show-resp.json
     :language: javascript

Replica Actions
===============


Delete Replica Disks
====================

.. rest_method:: POST /replicas/{replica_id}/actions

Deletes a replicas disks on the destination environment.

**Preconditions**

The replica execution must have at least one successful execution.


Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - delete_disks : replica_delete_disks
    - replica_id : replica_id_path

**Example of Replica Execution Cancel Request**

  .. literalinclude:: ../api_samples/replica/replica-disks-delete-req.json
     :language: javascript

Response
--------

The last execution of the requested replica is returned.

**Example of Delete Replica Disks Response**

  .. literalinclude:: ../api_samples/replica/openstack-replica-execution-show-resp.json
     :language: javascript

Replica Execution Actions
=========================

Cancel Replica Execution
========================

.. rest_method:: POST /replicas/{replica_id}/executions/{execution_id}/actions

Cancels an ongoing replica execution.

**Preconditions**

The replica must exist and its execution must be ongoing.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - force : replica_execution_force
    - replica_id : replica_id_path
    - execution_id : execution_id_path

**Example of Replica Execution Cancel Request**

  .. literalinclude:: ../api_samples/replica/replica-execution-cancel-req.json
     :language: javascript

Response
--------

There is no response to a successful replicas execution cancellation.

Delete Replica Execution
========================
.. rest_method:: DELETE /replicas/{replica_id}/executions/{execution_id}

Deletes an replica execution.

**Preconditions**

The replica execution must not be running.

Normal response codes: 204

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - replica_id : replica_id_path
    - execution_id : execution_id_path

Response
--------

There is no response body for a successful deletion of an replica execution.


Replica Scheduling
==================

Replica Schedule List
=====================

.. rest_method:: GET /replicas/{replica_id}/schedules

List schedules for a replica.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

  .. rest_parameters:: parameters.yaml

    - replica_id : replica_id_path

Response
--------

  .. rest_parameters:: parameters.yaml

    - schedules : replica_schedules
    - id : replica_schedule_id
    - deleted : replica_schedule_deleted
    - deleted_at : replica_schedule_deleted_at
    - created_at : replica_schedule_created_at
    - updated_at : replica_schedule_updated_at
    - replica_id : replica_id
    - enabled : replica_schedule_enabled
    - shutdown_instance : replica_shutdown_instances
    - schedule : replica_schedule
    - minute : replica_schedule_minute
    - hour : replica_schedule_hour
    - dom : replica_schedule_day_of_month
    - month : replica_schedule_month
    - dow : replica_schedule_day_of_week
    - expiration_date : replica_schedule_expiration

**Example of Replica Schedule List Response**

.. literalinclude:: ../api_samples/replica/replica-schedule-list-resp.json
     :language: javascript


Replica Schedule Show
=====================

.. rest_method:: GET /replicas/{replica_id}/schedules/{schedule_id}

Shows a replica schedule.

**Preconditions**
The replica schedule must exist.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

  .. rest_parameters:: parameters.yaml

    - replica_id : replica_id_path
    - schedule_id : replica_schedule_id_path

Response
--------

  .. rest_parameters:: parameters.yaml

    - id : replica_schedule_id
    - deleted : replica_schedule_deleted
    - deleted_at : replica_schedule_deleted_at
    - created_at : replica_schedule_created_at
    - updated_at : replica_schedule_updated_at
    - replica_id : replica_id
    - enabled : replica_schedule_enabled
    - shutdown_instance : replica_shutdown_instances
    - schedule : replica_schedule
    - minute : replica_schedule_minute
    - hour : replica_schedule_hour
    - dom : replica_schedule_day_of_month
    - month : replica_schedule_month
    - dow : replica_schedule_day_of_week
    - expiration_date : replica_schedule_expiration

**Example Replica Schedule Show Response**

.. literalinclude:: ../api_samples/replica/replica-schedule-show-resp.json
    :language: javascript

Replica Schedule Create
=======================

.. rest_method:: POST /replicas/{replica_id}/schedules

Creates a replica schedule.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

  - replica_id : replica_id_path
  - enabled : replica_schedule_enabled
  - shutdown_instance : replica_shutdown_instances
  - schedule : replica_schedule
  - minute : replica_schedule_minute
  - hour : replica_schedule_hour
  - dom : replica_schedule_day_of_month
  - month : replica_schedule_month
  - dow : replica_schedule_day_of_week
  - expiration_date : replica_schedule_expiration_request

**Example of Replica Schedule Create Request**

.. literalinclude:: ../api_samples/replica/replica-schedule-create-req.json
    :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - schedule : replica_schedule
  - id : replica_schedule_id
  - deleted : replica_schedule_deleted
  - deleted_at : replica_schedule_deleted_at
  - created_at : replica_schedule_created_at
  - updated_at : replica_schedule_updated_at
  - replica_id : replica_id
  - enabled : replica_schedule_enabled
  - shutdown_instance : replica_shutdown_instances
  - schedule : replica_schedule
  - minute : replica_schedule_minute
  - hour : replica_schedule_hour
  - dom : replica_schedule_day_of_month
  - month : replica_schedule_month
  - dow : replica_schedule_day_of_week
  - expiration_date : replica_schedule_expiration

**Example of Replica Schedule Create Response**

.. literalinclude:: ../api_samples/replica/replica-schedule-create-resp.json
    :language: javascript

Replica Schedule Update
=======================

.. rest_method:: PUT /replicas/{replica_id}/schedules/{schedule_id}

Updates a replica schedule.

**Preconditions**
The replica schedule must exist.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

  .. rest_parameters:: parameters.yaml

    - replica_id : replica_id_path
    - schedule_id : replica_schedule_id_path
    - shutdown_instance : replica_shutdown_instances
    - schedule : replica_schedule
    - minute : replica_schedule_minute
    - hour : replica_schedule_hour
    - dom : replica_schedule_day_of_month
    - month : replica_schedule_month
    - dow : replica_schedule_day_of_week
    - expiration_date : replica_schedule_expiration_request

**Example of Replica Schedule Update Request**

.. literalinclude:: ../api_samples/replica/replica-schedule-update-req.json
    :language: javascript

Response
--------

  .. rest_parameters:: parameters.yaml

    - id : replica_schedule_id
    - deleted : replica_schedule_deleted
    - deleted_at : replica_schedule_deleted_at
    - created_at : replica_schedule_created_at
    - updated_at : replica_schedule_updated_at
    - replica_id : replica_id
    - enabled : replica_schedule_enabled
    - shutdown_instance : replica_shutdown_instances
    - schedule : replica_schedule
    - minute : replica_schedule_minute
    - hour : replica_schedule_hour
    - dom : replica_schedule_day_of_month
    - month : replica_schedule_month
    - dow : replica_schedule_day_of_week
    - expiration_date : replica_schedule_expiration

**Example of Replica Schedule Update Response**

.. literalinclude:: ../api_samples/replica/replica-schedule-update-resp.json
    :language: javascript

Replica Schedule Delete
=======================

.. rest_method:: DELETE /replicas/{replica_id}/schedules/{schedule_id}

Deletes a replica schedule.

**Preconditions**
The replica schedule must exist.

Normal response codes: 204

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

  .. rest_parameters:: parameters.yaml

    - replica_id : replica_id_path
    - schedule_id : replica_schedule_id

Response
--------

There is no response body for a successful deletion of a replica schedule.
