.. -*- rst -*-

===========
Deployments
===========

Lists, creates, cancels, shows details for, and deletes Coriolis deployments.


List Deployments
================

.. rest_method:: GET /deployments

Lists all deployments.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Response
--------

.. rest_parameters:: parameters.yaml

  - deployments : deployments_array
  - id : deployment_id
  - include_task_info : include_task_info
  - show_deleted : show_deleted
  - deleted : deployment_deleted
  - deleted_at : deployment_deleted_at
  - instances : deployment_instances
  - notes : deployment_notes
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : deployment_type
  - origin_endpoint_id : deployment_origin_endpoint_id
  - created_at : deployment_created_at
  - updated_at : deployment_updated_at
  - project_id : project_id
  - transfer_id : transfer_id
  - base_id : base_id
  - destination_endpoint_id : deployment_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - last_execution_status : deployment_last_execution_status
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - user_scripts : user_scripts
  - transfer_id : transfer_id
  - transfer_scenario_type : transfer_scenario_type
  - shutdown_instances : deployment_shutdown_instances

**Example of a Deployment List**

  .. literalinclude:: ../api_samples/deployment/openstack-deployment-list-detail-resp.json
    :language: javascript


Show Deployment
===============

.. rest_method:: GET /deployments/{deployment_id}

Shows details about a deployment.

**Preconditions**

The deployment must exist.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

  - deployment_id : deployment_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - deployment : deployment_object
  - id : deployment_id
  - include_task_info : include_task_info
  - deleted : deployment_deleted
  - deleted_at : deployment_deleted_at
  - instances : deployment_instances
  - notes : deployment_notes
  - tasks : deployment_tasks
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : deployment_type
  - origin_endpoint_id : deployment_origin_endpoint_id
  - created_at : deployment_created_at
  - updated_at : deployment_updated_at
  - project_id : project_id
  - transfer_id : transfer_id
  - base_id : base_id
  - destination_endpoint_id : deployment_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - last_execution_status : deployment_last_execution_status
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - user_scripts : user_scripts
  - transfer_id : transfer_id
  - transfer_scenario_type : transfer_scenario_type
  - shutdown_instances : deployment_shutdown_instances

**Example of a Deployment Show Response**
  .. literalinclude:: ../api_samples/deployment/openstack-deployment-get-resp.json
     :language: javascript


Create Deployment From Transfer
===============================

.. rest_method:: POST /deployments

Creates deployment from a successful transfer execution.

**Preconditions**

A transfer execution must exist.

Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)


Request
-------

.. rest_parameters:: parameters.yaml

    - deployment : deployment_object
    - transfer_id : transfer_id
    - force : deployment_force
    - clone_disks : deployment_clone_disks
    - skip_os_morphing : skip_os_morphing
    - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings

**Example of Deployment Create From Transfer Request**
  .. literalinclude:: ../api_samples/deployment/deployment-from-transfer-req.json
     :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - deployment : deployment_object
  - id : deployment_id
  - deleted : deployment_deleted
  - deleted_at : deployment_deleted_at
  - instances : deployment_instances
  - notes : deployment_notes
  - status : deployment_notes
  - tasks : deployment_tasks
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environtment : source_environment
  - type : deployment_type
  - info : deployment_info
  - origin_endpoint_id : deployment_origin_endpoint_id
  - created_at : deployment_created_at
  - updated_at : deployment_updated_at
  - project_id : project_id
  - transfer_id : transfer_id
  - base_id : base_id
  - destination_endpoint_id : deployment_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map
  - last_execution_status : deployment_last_execution_status
  - origin_minion_pool_id : origin_minion_pool_id
  - destination_minion_pool_id : destination_minion_pool_id
  - instance_osmorphing_minion_pool_mappings : instance_osmorphing_minion_pool_mappings
  - shutdown_instances : deployment_shutdown_instances

**Example of Deployment Create From Transfer Response**
  .. literalinclude:: ../api_samples/deployment/openstack-deployment-from-transfer-resp.json
     :language: javascript

Delete Deployment
=================
.. rest_method:: DELETE /deployments/{deployment_id}

Deletes an non-running deployment.

**Preconditions**

The deployment must exist and not be running.

Normal response codes: 204

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - deployment_id : deployment_id_path

Response
--------

There is no response body for a successful deletion of an deployment.


Cancel Deployment
=================

.. rest_method:: POST /deployments/{deployment_id}/actions

Cancels a running deployment.

**Preconditions**

The deployment must exist and must be running.

Normal response codes: 200


Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - deployment_id : deployment_id_path
  - force : deployment_cancel_force
  - cancel : deployment_cancel

**Example of A Deployment Cancellation Request**
  .. literalinclude:: ../api_samples/deployment/deployment-cancel-req.json
     :language: javascript

Response
--------

There is no response to a successful deployment cancellation.
