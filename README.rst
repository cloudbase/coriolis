Project Coriolis
================

*Cloud Migration as a Service*


Migrating existing workloads between clouds is a necessity for a large number
of use cases, especially for user moving from traditional virtualization
technologies like VMware vSphere or Microsoft System Center VMM to Azure /
AzureStack, OpenStack, Amazon AWS or Google Cloud. Furthermore, cloud to cloud
migrations, like AWS to Azure are also a common requirement.

Project Coriolis™ addresses exactly those requirements, in particular migrating
Linux (Ubuntu, Red Hat / CentOS, SUSE, Debian, Fedora) and Windows virtual
machine, templates, storage and networking configurations.

There are some tricky scenarios where Coriolis excels: to begin with, virtual
machines need to be moved between different hypervisors, which means including
new operating system drivers and tools, for example cloud-init / cloudbase-init
in the OpenStack use case, LIS kernel modules on Hyper-V and Azure and so on.


The project is largely using Oslo libraries with an architecture meant to be
familiar for devops used to OpenStack, as shown in the diagram below.
A clear separation between stateless microservices and proper usage of queues
allows scalability and fault tolerance from the start. For PoCs and small
environments, all components can be deployed on a single host / VM / container.

Authentication and endpoint discovery is based on Keystone (in the typical
OpenStack way), which means that passing an X-Auth-Token header from an
existing Keystone session allows an easy integration with other components,
e.g. in Horizon's console.
The same token is passed to other components along the pipeline, which also
implies that importing virtual machines and other resources to the same
OpenStack infrastructure doesn’t require further authentication.

Authentication to external clouds (Azure, AWS, etc) or virtualisation solutions
(vSphere, SCVMM, etc) in order to export virtual resources requires credentials
that can be saved in Barbican, thus avoiding the need to pass secrets directly
to the API.

Cloud resources that can be migrated:

- Virtual machines
- Virtual Mmchine templates
- Storage
- Network configurations

VM disks are converted to the desired target format and drivers / tools are
automatically added where appropriate during the process (e. cloud-init on
OpenStack, KVM Windows VirtIO drivers, etc).

The migration jobs are split in import / export tasks with a scheduler taking
care of choosing a worker node where this can be executed. Each task contains
progress update info that the client can poll to follow the progress of the
operations. Tasks can be relatively long running, depending on the storage
size, so proper status reporting was included in the design from the start.


.. image:: https://cloudbase.it/wp-content/uploads/2016/02/coriolis-diagram.svg

Keystone configuration
----------------------

Here's an example Keystone service and endpoints configuration:
::

    openstack service create --name coriolis --description "Cloud Migration as a Service" migration

    ENDPOINT_URL="http://hostname:7667/v1/%(tenant_id)s"
    openstack endpoint create --region RegionOne migration `
    --publicurl $ENDPOINT_URL `
    --internalurl $ENDPOINT_URL `
    --adminurl $ENDPOINT_URL

    openstack user create --password-prompt coriolis
    openstack role add --project service --user coriolis admin

API
---

The API is also very straightforward, here’s a complete example available on
Postman: https://www.getpostman.com/collections/734dc164d4b6b00cd8fc


Create a migration job:

    POST http://server:7667/v1/%project_id/migrations

Example request body:
::
    {
        "migration": {
            "origin": {
                "type": "vmware_vsphere",
                "connection_info": {
                    “secret_id": "ebe69d82-da6f-451e-a0f6-3551d0f7ef85"
                }
            },
            "destination": {
                "type": "openstack",
                "target_environment": {
                    "flavor_name": "m1.small",
                    "network_map": {
                        "VM Network": "private",
                        "VM Network Local": "public"
                    }
                }
            },
            "instances": ["CentOS 7”, “RHEL 7.2”, “Ubuntu 14.04”, “WS 2012 R2"]
        }
    }

Note: here’s an example secret stored in Barbican with vSphere connection info:
::
    {
        "host": “10.0.0.10”,
        "username": “user@vsphere.local",
        "password": “Password",
        "allow_untrusted": true
    }

List migrations:

    GET http://server:7667/v1/%(project_id)s/migrations
    GET http://server:7667/v1/%(project_id)s/migrations/detail


Get a migration job info:

    GET http://server:7667/v1/%(project_id)s/migrations/%(migration_id)s


Cancel a migration job:

This API allows the user to interrupt any running job.

    POST http://server:7667/v1/%(project_id)s/migrations/%(migration_id)s/action

Request body:
::
    { "cancel": null }


Delete a migration job:

    DELETE http://server:7667/v1/%(project_id)s/migrations/%(migration_id)s

Note: only completed, failed or cancelled jobs can be deleted.


API bindings
------------

We’re currently working on API bindings as well, starting with Python and
Golang.

Any feedback is well appreciated!

.. _Python: http://www.python.org/
