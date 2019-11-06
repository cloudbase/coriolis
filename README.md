# Coriolis deployment tools

## Clone the repository

```
git clone https://bitbucket.org/cloudbase/coriolis-docker
```

## Prepare the OS

This includes installing Docker, Ansible and other dependencies.

### Oracle Linux 7.x
```
./coriolis-docker/prereqs_ol7.sh
```

### Ubuntu 18.04
```
./coriolis-docker/prereqs_ubuntu18.04.sh
```

## Build Kolla images (optional)

Skip this unless new images are needed.

```
./coriolis-docker/kolla/build.sh
```

## Deploy OpenStack components with Kolla

This will take care of deploying the OpenStack components needed by Coriolis
(Keystone, Barbican) along with MariaDB, RabbitMQ, etc:

```
./coriolis-docker/kolla/deploy.sh
```

## Destroy a Kolla deployment

This will remove all the Kolla Docker containers:
```
./coriolis-docker/kolla/destroy.sh
```

## Build Coriolis images (optional)

Skip this unless new images are needed.
```
./coriolis-docker/coriolis-ansible build
```

To push the images to the Docker registry, set the following before building
the images:

* Add the following option to `./coriolis-docker/config.yml` file (if the
file doesn't exist, create it via
`cp ./coriolis-docker/config.yml.sample ./coriolis-docker/config.yml`):
```
docker_push_images: true
```

* Login to Docker registry:
```
docker login registry.cloudbase.it
```

## Deploy Coriolis

Note: this required Keystone to be already deployed via Kolla.
```
docker login registry.cloudbase.it
./coriolis-docker/coriolis-ansible deploy
```

## Reconfigure Coriolis

Note: this required Keystone to be already deployed via Kolla.
```
docker login registry.cloudbase.it
./coriolis-docker/coriolis-ansible reconfigure
```

## Expose Coriolis

```
./coriolis-docker/expose_coriolis.py
```

## Destroy Coriolis

```
./coriolis-docker/coriolis-ansible destroy
```

## Validate the deployment

Execute some basic Coriolis and OpenStack commands to make sure that all
components have been deployed and configured properly:
```
source /etc/kolla/admin-openrc.sh
openstack endpoint list
openstack secret list
coriolis endpoint list
```

## Updating an existing appliance

Given access to the upstream Docker registry, the appliance can be simply
updated by running:
```
cd /root/coriolis-docker
git pull origin master
./coriolis-ansible update
```
