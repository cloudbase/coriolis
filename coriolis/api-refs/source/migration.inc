.. -*- rst -*-

==========
Migrations
==========

Lists, creates, cancels, shows details for, and deletes Coriolis migrations.


List Migrations
===============

.. rest_method:: GET /migrations/detail

Lists all migrations.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Response
--------

.. rest_parameters:: parameters.yaml

  - migrations : migrations_array
  - id : migration_id
  - deleted : migration_deleted
  - deleted_at : migration_deleted_at
  - instances : migration_instances
  - notes : migration_notes
  - status : migration_status
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : migration_type
  - origin_endpoint_id : migration_origin_endpoint_id
  - created_at : migration_created_at
  - updated_at : migration_updated_at
  - project_id : project_id
  - replica_id : migration_replica_id
  - base_id : base_id
  - destination_endpoint_id : migration_destination_endpoint_id
  - tasks : migration_tasks
  - network_map : network_map
  - storage_mappings : storage_map

**Example of a Migration List**

  .. literalinclude:: ../api_samples/migration/openstack-migration-list-detail-resp.json
    :language: javascript


Show Migration
==============

.. rest_method:: GET /migrations/{migration_id}

Shows details about a migration.

**Preconditions**

The migration must exist.

Normal response codes: 200

Error response codes:   unauthorized(401),
forbidden(403)

Request
-------

.. rest_parameters:: parameters.yaml

  - migration_id : migration_id_path

Response
--------

.. rest_parameters:: parameters.yaml

  - migration : migration_object
  - id : migration_id
  - deleted : migration_deleted
  - deleted_at : migration_deleted_at
  - instances : migration_instances
  - notes : migration_notes
  - status : migration_status
  - tasks : migration_tasks
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : migration_type
  - origin_endpoint_id : migration_origin_endpoint_id
  - created_at : migration_created_at
  - updated_at : migration_updated_at
  - project_id : project_id
  - replica_id : migration_replica_id
  - base_id : base_id
  - destination_endpoint_id : migration_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map

**Example of a Migration Show Response**
  .. literalinclude:: ../api_samples/migration/openstack-migration-get-resp.json
     :language: javascript

Create Migration
================

.. rest_method:: POST /migrations

Creates a normal migration.

Normal response codes: 200

forbidden(403) Error response codes:   unauthorized(401),

Request
-------

.. rest_parameters:: parameters.yaml

  - migration : migration_object
  - network_map : network_map
  - storage_mappings : storage_map
  - destination_environment : destination_environment
  - source_environment : source_environment
  - skip_os_morphing : skip_os_morphing
  - destination_endpoint_id : migration_destination_endpoint_id
  - origin_endpoint_id : migration_origin_endpoint_id
  - instances : migration_instances

**Example Migration Create Request**
  .. literalinclude:: ../api_samples/migration/openstack-migration-create-req.json
    :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - migration : migration_object
  - id : migration_id
  - deleted : migration_deleted
  - deleted_at : migration_deleted_at
  - instances : migration_instances
  - notes : migration_notes
  - status : migration_notes
  - tasks : migration_tasks
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environment : source_environment
  - type : migration_type
  - info : migration_info
  - origin_endpoint_id : migration_origin_endpoint_id
  - created_at : migration_created_at
  - updated_at : migration_updated_at
  - project_id : project_id
  - replica_id : migration_replica_id
  - base_id : base_id
  - destination_endpoint_id : migration_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map

**Example Migration Create Response**
  .. literalinclude:: ../api_samples/migration/openstack-migration-create-resp.json
    :language: javascript

Create Migration From Replica
=============================

.. rest_method:: POST /migrations

Creates migration from a successful replica execution.

Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)


Request
-------

.. rest_parameters:: parameters.yaml

    - migration : migration_object
    - replica_id : replica_id
    - force : migration_force
    - clone_disks : migration_clone_disks
    - skip_os_morphing : skip_os_morphing

**Example of Migration Create From Replica Request**
  .. literalinclude:: ../api_samples/migration/migration-from-replica-req.json
     :language: javascript

Response
--------

.. rest_parameters:: parameters.yaml

  - migration : migration_object
  - id : migration_id
  - deleted : migration_deleted
  - deleted_at : migration_deleted_at
  - instances : migration_instances
  - notes : migration_notes
  - status : migration_notes
  - tasks : migration_tasks
  - user_id : user_id
  - destination_environment : destination_environment
  - source_environtment : source_environment
  - type : migration_type
  - info : migration_info
  - origin_endpoint_id : migration_origin_endpoint_id
  - created_at : migration_created_at
  - updated_at : migration_updated_at
  - project_id : project_id
  - replica_id : migration_replica_id
  - base_id : base_id
  - destination_endpoint_id : migration_destination_endpoint_id
  - network_map : network_map
  - storage_mappings : storage_map


**Example of Migration Create From Replica Response**
  .. literalinclude:: ../api_samples/migration/openstack-migration-from-replica-resp.json
     :language: javascript

Delete Migration
================
.. rest_method:: DELETE migrations/{migration_id}

Deletes an non-running migration.

**Preconditions**

The migration must exist and not be running.

Normal response codes: 200

Error response codes:
unauthorized(401), forbidden(403), itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - migration_id : migration_id_path

Response
--------

There is no response body for a successful deletion of an migration.


Cancel Migration
================

.. rest_method:: POST /migrations/{migration_id}/actions

Cancels a running migration.

**Preconditions**

The migration must exist and must be running.

Normal response codes: 200


Error response codes: unauthorized(401), forbidden(403),
itemNotFound(404)

Request
-------

.. rest_parameters:: parameters.yaml

  - migration_id : migration_id_path
  - force : migration_cancel_force
  - cancel : migration_cancel

**Example of A Migration Cancellation Request**
  .. literalinclude:: ../api_samples/migration/migration-cancel-req.json
     :language: javascript

Response
--------

There is no response to a successful migration cancellation.
