.. -*- rst -*-

=========
Transfers
=========

Lists, creates, updates, shows details for, and deletes cloud transfers.


List Transfers
==============

.. rest_method:: GET /transfers

List all transfers.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Response
--------

.. rest_parameters:: parameters.yaml

  - transfers : transfers_array
  - id : transfer_id
  - include_task_info: include_task_info
  - show_deleted : show_deleted
  - deleted : transfer_deleted
  - deleted_at : transfer_deleted_at
  - executions : transfer_empty_execution_array
  - instances : transfer_instances
  - notes : transfer_notes
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : transfer_type
  - origin_endpoint_id : transfer_origin_endpoint_id
  - created_at : transfer_created_at
  - updated_at : transfer_updated_at
  - project_id : project_id
  - base_id : base_id
  - destination_endpoint_id : transfer_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - last_execution_status : transfer_last_execution_status
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - user_scripts : user_scripts
  - scenario: scenario_type

**Example of Transfer List Response**

.. literalinclude:: ../api_samples/transfer/transfer-list-resp.json
   :language: javascript


Show Transfer
=============

.. rest_method:: GET /transfers/{transfer_id}

Shows details about a transfer.

**Preconditions**

The transfer must exist.


Error response codes:   unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - transfer_id : transfer_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - transfer : transfer_object
  - id : transfer_id
  - include_task_info: include_task_info
  - deleted : transfer_deleted
  - deleted_at : transfer_deleted_at
  - executions : transfer_execution_array
  - instances : transfer_instances
  - notes : transfer_notes
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : transfer_type
  - origin_endpoint_id : transfer_origin_endpoint_id
  - created_at : transfer_created_at
  - updated_at : transfer_updated_at
  - project_id : project_id
  - base_id : base_id
  - destination_endpoint_id : transfer_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - last_execution_status : transfer_last_execution_status
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - user_scripts : user_scripts
  - scenario: scenario_type

**Example of Transfer Show Response**

  .. literalinclude:: ../api_samples/transfer/openstack-transfer-get-resp.json
     :language: javascript

Create Transfer
===============

.. rest_method:: POST /transfers

Creates a transfer.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

  - transfer : transfer_object
  - destination_environment : transfer_destination_environment
  - source_environment : transfer_source_environment
  - destination_endpoint_id : transfer_destination_endpoint_id
  - origin_endpoint_id : transfer_origin_endpoint_id
  - instances : transfer_instances
  - network_map : network_map
  - storage_mappings : storage_map
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - user_scripts : user_scripts
  - id : transfer_id
  - scenario: scenario_type

**Example of Transfer Create Request**

  .. literalinclude:: ../api_samples/transfer/openstack-transfer-create-req.json
      :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - transfer : transfer_object
  - id : transfer_id
  - deleted : transfer_deleted
  - deleted_at : transfer_deleted_at
  - executions : transfer_execution_array
  - instances : transfer_instances
  - notes : transfer_notes
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : transfer_type
  - origin_endpoint_id : transfer_origin_endpoint_id
  - created_at : transfer_created_at
  - updated_at : transfer_updated_at
  - project_id : project_id
  - base_id : base_id
  - destination_endpoint_id : transfer_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - user_scripts : user_scripts
  - scenario: scenario_type

**Example of Transfer Create Response**

.. literalinclude:: ../api_samples/transfer/openstack-transfer-create-resp.json
    :language: javascript

Update Transfer
===============

.. rest_method:: PUT /transfers/{transfer_id}

Updates a transfer.

**Preconditions**
The transfer must exist.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

  .. rest_parameters:: parameters.yaml

      - transfer_id : transfer_id_path
      - destination_environment : transfer_destination_environment
      - source_environment : transfer_source_environment
      - network_map : transfer_network_map
      - notes : transfer_update_notes
      - storage_mappings : transfer_storage_map

**Example of Transfer Update Request**

.. literalinclude:: ../api_samples/transfer/transfer-update-req.json
    :language: javascript

Response
--------

  .. rest_parameters:: parameters.yaml

      - execution : transfer_execution_object
      - number : transfer_execution_number
      - action_id : transfer_id
      - deleted : transfer_execution_deleted
      - deleted_at : transfer_execution_deleted_at
      - created_at : transfer_execution_created_at
      - updated_at : transfer_execution_updated_at
      - status : transfer_execution_status
      - tasks : transfer_execution_tasks
      - action : transfer_execution_action
      - id : transfer_execution_id
      - origin_minion_pool_id : origin_minion_pool_id
      - destination_minion_pool_id : destination_minion_pool_id
      - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
      - user_scripts : user_scripts

**Example of Transfer Update Response**

.. literalinclude:: ../api_samples/transfer/transfer-update-resp.json
    :language: javascript


Delete Transfer
===============

.. rest_method:: DELETE /transfers/{transfer_id}

Deletes a transfer.

**Preconditions**

The transfer must exist and not have a running execution or deployment.

Normal response codes: 204

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - transfer_id : transfer_id_path

Response
--------

There is no response body for a successful deletion.

Create Transfer Execution
=========================

.. rest_method:: POST /transfers/{transfer_id}/executions

Executes a transfer.

**Preconditions**

The transfer must exist.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),

Request
-------

.. rest_parameters:: parameters.yaml

    - shutdown_instances : transfer_shutdown_instances
    - transfer_id : transfer_id_path

**Example Transfer Execution Create Request**

.. literalinclude:: ../api_samples/transfer/transfer-execution-req.json
    :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - execution : transfer_execution
  - id : transfer_execution_id
  - action_id : transfer_id
  - deleted : transfer_deleted
  - deleted_at : transfer_execution_deleted_at
  - created_at : transfer_execution_created_at
  - updated_at : transfer_execution_updated_at
  - status : transfer_execution_status
  - number : transfer_execution_number
  - tasks : transfer_execution_tasks
  - action : transfer_execution_action
  - type : transfer_execution_type

**Example Transfer Execution Response**

  .. literalinclude:: ../api_samples/transfer/openstack-transfer-execution-resp.json
   :language: javascript

List Transfer Executions
========================

.. rest_method:: GET /transfers/{transfer_id}/executions

List a transfer's executions.

**Preconditions**
The transfer must exist.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

   - transfer_id : transfer_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - executions : transfer_execution_array
  - updated_at : transfer_updated_at
  - action_id : transfer_id
  - deleted : transfer_execution_deleted
  - deleted_at : transfer_execution_deleted_at
  - created_at : transfer_execution_created_at
  - number : transfer_execution_number
  - status : transfer_execution_status
  - id : transfer_execution_id
  - type : transfer_execution_type

**Example Transfer Execute Response**

  .. literalinclude:: ../api_samples/transfer/transfer-execution-list-resp.json
     :language: javascript

Show Transfer Execution
=======================

.. rest_method:: GET /transfers/{transfer_id}/executions/{execution_id}

Show a transfer's execution.

**Preconditions**
The transfer execution must exist.

Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - transfer_id : transfer_id_path
  - execution_id : execution_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - execution : transfer_execution_object
  - number : transfer_execution_number
  - action_id : transfer_id
  - deleted : transfer_execution_deleted
  - deleted_at : transfer_execution_deleted_at
  - created_at : transfer_execution_created_at
  - updated_at : transfer_execution_updated_at
  - status : transfer_execution_status
  - tasks : transfer_execution_tasks
  - action : transfer_execution_action
  - id : transfer_execution_id
  - type : transfer_execution_type


**Example of Transfer Executions Show Response**
  .. literalinclude:: ../api_samples/transfer/openstack-transfer-execution-show-resp.json
     :language: javascript

Transfers Actions
=================


Delete Transfer Disks
=====================

.. rest_method:: POST /transfers/{transfer_id}/actions

Deletes a transfers disks on the destination environment.

**Preconditions**

The transfer execution must have at least one successful execution.


Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - delete_disks : transfer_delete_disks
    - transfer_id : transfer_id_path

**Example of Transfer Execution Cancel Request**

  .. literalinclude:: ../api_samples/transfer/transfer-disks-delete-req.json
     :language: javascript

Response
--------

The last execution of the requested transfer is returned.

**Example of Delete Transfer Disks Response**

  .. literalinclude:: ../api_samples/transfer/openstack-transfer-execution-show-resp.json
     :language: javascript

Transfer Execution Actions
==========================

Cancel Transfer Execution
=========================

.. rest_method:: POST /transfers/{transfer_id}/executions/{execution_id}/actions

Cancels an ongoing transfer execution.

**Preconditions**

The transfer must exist and its execution must be ongoing.

Normal response codes: 200

Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - force : transfer_execution_force
    - transfer_id : transfer_id_path
    - execution_id : execution_id_path

**Example of Transfer Execution Cancel Request**

  .. literalinclude:: ../api_samples/transfer/transfer-execution-cancel-req.json
     :language: javascript

Response
--------

There is no response to a successful transfers execution cancellation.

Delete Transfer Execution
=========================
.. rest_method:: DELETE /transfers/{transfer_id}/executions/{execution_id}

Deletes an transfer execution.

**Preconditions**

The Transfer execution must not be running.

Normal response codes: 204

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

    - transfer_id : transfer_id_path
    - execution_id : execution_id_path

Response
--------

There is no response body for a successful deletion of an transfer execution.


Transfer Scheduling
===================

Transfer Schedule List
======================

.. rest_method:: GET /transfers/{transfer_id}/schedules

List schedules for a transfer.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

  .. rest_parameters:: parameters.yaml

    - transfer_id : transfer_id_path

Response
--------

  .. rest_parameters:: parameters.yaml

    - schedules : transfer_schedules
    - id : transfer_schedule_id
    - deleted : transfer_schedule_deleted
    - deleted_at : transfer_schedule_deleted_at
    - created_at : transfer_schedule_created_at
    - updated_at : transfer_schedule_updated_at
    - transfer_id : transfer_id
    - enabled : transfer_schedule_enabled
    - shutdown_instance : transfer_shutdown_instances
    - schedule : transfer_schedule
    - minute : transfer_schedule_minute
    - hour : transfer_schedule_hour
    - dom : transfer_schedule_day_of_month
    - month : transfer_schedule_month
    - dow : transfer_schedule_day_of_week
    - expiration_date : transfer_schedule_expiration

**Example of Transfer Schedule List Response**

.. literalinclude:: ../api_samples/transfer/transfer-schedule-list-resp.json
     :language: javascript


Transfer Schedule Show
======================

.. rest_method:: GET /transfers/{transfer_id}/schedules/{schedule_id}

Shows a transfer schedule.

**Preconditions**
The transfer schedule must exist.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403), itemNotFound(404)

Request
-------

  .. rest_parameters:: parameters.yaml

    - transfer_id : transfer_id_path
    - schedule_id : transfer_schedule_id_path

Response
--------

  .. rest_parameters:: parameters.yaml

    - id : transfer_schedule_id
    - deleted : transfer_schedule_deleted
    - deleted_at : transfer_schedule_deleted_at
    - created_at : transfer_schedule_created_at
    - updated_at : transfer_schedule_updated_at
    - transfer_id : transfer_id
    - enabled : transfer_schedule_enabled
    - shutdown_instance : transfer_shutdown_instances
    - schedule : transfer_schedule
    - minute : transfer_schedule_minute
    - hour : transfer_schedule_hour
    - dom : transfer_schedule_day_of_month
    - month : transfer_schedule_month
    - dow : transfer_schedule_day_of_week
    - expiration_date : transfer_schedule_expiration

**Example Transfer Schedule Show Response**

.. literalinclude:: ../api_samples/transfer/transfer-schedule-show-resp.json
    :language: javascript

Transfer Schedule Create
========================

.. rest_method:: POST /transfers/{transfer_id}/schedules

Creates a transfer schedule.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

  - transfer_id : transfer_id
  - enabled : transfer_schedule_enabled
  - shutdown_instance : transfer_shutdown_instances
  - schedule : transfer_schedule
  - minute : transfer_schedule_minute
  - hour : transfer_schedule_hour
  - dom : transfer_schedule_day_of_month
  - month : transfer_schedule_month
  - dow : transfer_schedule_day_of_week
  - expiration_date : transfer_schedule_expiration_request

**Example of Transfer Schedule Create Request**

.. literalinclude:: ../api_samples/transfer/transfer-schedule-create-req.json
    :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - schedule : transfer_schedule
  - id : transfer_schedule_id
  - deleted : transfer_schedule_deleted
  - deleted_at : transfer_schedule_deleted_at
  - created_at : transfer_schedule_created_at
  - updated_at : transfer_schedule_updated_at
  - transfer_id : transfer_id
  - enabled : transfer_schedule_enabled
  - shutdown_instance : transfer_shutdown_instances
  - schedule : transfer_schedule
  - minute : transfer_schedule_minute
  - hour : transfer_schedule_hour
  - dom : transfer_schedule_day_of_month
  - month : transfer_schedule_month
  - dow : transfer_schedule_day_of_week
  - expiration_date : transfer_schedule_expiration

**Example of Transfer Schedule Create Response**

.. literalinclude:: ../api_samples/transfer/transfer-schedule-create-resp.json
    :language: javascript

Transfer Schedule Update
========================

.. rest_method:: PUT /transfers/{transfer_id}/schedules/{schedule_id}

Updates a transfer schedule.

**Preconditions**
The transfer schedule must exist.

Normal response codes: 200

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

  .. rest_parameters:: parameters.yaml

    - transfer_id : transfer_id
    - schedule_id : transfer_schedule_id_path
    - shutdown_instance : transfer_shutdown_instances
    - schedule : transfer_schedule
    - minute : transfer_schedule_minute
    - hour : transfer_schedule_hour
    - dom : transfer_schedule_day_of_month
    - month : transfer_schedule_month
    - dow : transfer_schedule_day_of_week
    - expiration_date : transfer_schedule_expiration_request

**Example of Transfer Schedule Update Request**

.. literalinclude:: ../api_samples/transfer/transfer-schedule-update-req.json
    :language: javascript

Response
--------

  .. rest_parameters:: parameters.yaml

    - id : transfer_schedule_id
    - deleted : transfer_schedule_deleted
    - deleted_at : transfer_schedule_deleted_at
    - created_at : transfer_schedule_created_at
    - updated_at : transfer_schedule_updated_at
    - transfer_id : transfer_id
    - enabled : transfer_schedule_enabled
    - shutdown_instance : transfer_shutdown_instances
    - schedule : transfer_schedule
    - minute : transfer_schedule_minute
    - hour : transfer_schedule_hour
    - dom : transfer_schedule_day_of_month
    - month : transfer_schedule_month
    - dow : transfer_schedule_day_of_week
    - expiration_date : transfer_schedule_expiration

**Example of Transfer Schedule Update Response**

.. literalinclude:: ../api_samples/transfer/transfer-schedule-update-resp.json
    :language: javascript

Transfer Schedule Delete
========================

.. rest_method:: DELETE /transfers/{transfer_id}/schedules/{schedule_id}

Deletes a transfer schedule.

**Preconditions**
The transfer schedule must exist.

Normal response codes: 204

Error response codes: unauthorized(401),
forbidden(403)

Request
-------

  .. rest_parameters:: parameters.yaml

    - transfer_id : transfer_id_path
    - schedule_id : transfer_schedule_id

Response
--------

There is no response body for a successful deletion of a transfer schedule.
