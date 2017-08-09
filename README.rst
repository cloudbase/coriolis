=========================
Coriolis deployment tools
=========================

Clone this repo: ::

    git clone https://bitbucket.org/cloudbase/coriolis-docker


Prepare the OS
==============

This includes installing Docker, Ansible and other dependencies.

Oracle Linux 7.x: ::

    ./coriolis-docker/prereqs_ol7.sh

Ubuntu 16.04: ::

    TODO


Build Kolla images (optional)
=============================

Skip this unless new images are needed.

::

    ./coriolis-docker/kolla/build.sh

To push the images to the Docker registry:

::

    docker login
    ./coriolis-docker/kolla/build.sh push


Deploy OpenStack components with Kolla
======================================

This will take care of deploying the OpenStack components needed by Coriolis (Keystone, Barbican) along with MariaDB, RabbitMQ, etc.

The parameter is the **public interface name**.

::

     ./coriolis-docker/kolla/deploy.sh eth0

Destroy a Kolla deployment
==========================

This will remove all the Kolla Docker containers.

::

    ./coriolis-docker/kolla/destroy.sh

Build Coriolis images (optional)
================================

Skip this unless new images are needed.

::

    ./coriolis-docker/build.sh

To push the images to the Docker registry, set the following before calling
*build.sh*:

::

    # Edit "config-build.yml" and set:
    # docker_push_images: true
    docker login registry.cloudbase.it

Additional build related options can be found in *config-build.yml*.

Deploy Coriolis
===============

Note: this required Keystone to be already deployed via Kolla.

::

    docker login registry.cloudbase.it
    ./coriolis-docker/deploy.sh eth0
