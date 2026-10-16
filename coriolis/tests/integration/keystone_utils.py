# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

"""
Keystone container helpers for the integration tests.

Starts Keystone (using the official kolla image) backed by a "keystone" schema
on a running mariadb container, bootstraps it, and points coriolis' [keystone],
[trustee] and [keystone_authtoken] config groups at it.
"""

import json
import os
import time
import urllib.error
import urllib.request
import uuid

from keystoneauth1 import identity as ks_identity
from keystoneauth1 import loading as ks_loading
from keystoneauth1 import session as ks_session
from keystoneclient.v3 import client as kc_v3
from oslo_config import cfg
from oslo_log import log as logging

from coriolis import exception
from coriolis import utils as coriolis_utils
from coriolis.tests.integration import utils as test_utils

LOG = logging.getLogger(__name__)

# kolla publishes the official Keystone images that kolla-ansible deploys in
# production OpenStack.
KEYSTONE_IMAGE = "quay.io/openstack.kolla/keystone:2026.1-ubuntu-noble"

# Project / domain / role name used to bootstrap Keystone for this session.
PROJECT_NAME = 'integration-project'
DOMAIN_NAME = 'Default'
ROLE_NAME = 'admin'

_STARTUP_SH = """#!/bin/bash -x
set -o errexit
set -o pipefail

FERNET_KEY_DIR="/etc/keystone/fernet-keys"
n=0

while [ ! -f "${FERNET_KEY_DIR}/0" ]; do
    if [ $n -gt 30 ]; then
        exit 1
    fi
    n=$(( n + 1 ))
    sleep 5
done

exec /var/lib/kolla/venv/bin/uwsgi --ini /etc/keystone/keystone-uwsgi.ini
"""

# Keystone 2026.1 (and its kolla image) dropped Apache in favour of uWSGI, which
# Keystone ships configuration for itself ("httpd/keystone-uwsgi-public.ini" in
# the source tree).
_UWSGI_INI = """
[uwsgi]
module = keystone.wsgi.api:application

http = 0.0.0.0:5000

# Keystone tokens and catalog documents are large; the 4k default is too small.
buffer-size = 65535

master = true
processes = 2
threads = 4
enable-threads = true

# This ensures that file descriptors aren't shared between keystone processes.
lazy-apps = true

# Recommended by uWSGI to avoid a thundering herd on accept.
thunder-lock = true

# "docker stop" sends SIGTERM, which uWSGI otherwise treats as a request to
# gracefully reload, leaving the master running until the stop timeout expires.
die-on-term = true
"""


class KeystoneContainer:
    """A bootstrapped Keystone container for the integration tests session."""

    def __init__(
        self, workdir, session_tag, db_container_name, db_ip, db_username, db_password
    ):
        self._workdir = workdir
        self._db_container_name = db_container_name
        self._db_ip = db_ip
        self._db_username = db_username
        self._db_password = db_password

        self.container_name = "coriolis-test-keystone-%s" % session_tag
        self.admin_username = "admin"
        self.admin_password = str(uuid.uuid4())
        self.auth_url = None
        # Real project ID assigned by Keystone at bootstrap time; resolved in start().
        self.project_id = None

    def setup(self):
        """Start and bootstrap the container, and configure coriolis.

        Reuses the mariadb container for a second "keystone" schema.
        """
        self._create_database()
        config_dir = self._write_config_files()

        coriolis_utils.exec_process(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.container_name,
                # Required by kolla_set_configs, the image's entrypoint
                # config renderer, to actually copy our config_files in.
                "-e",
                "KOLLA_CONFIG_STRATEGY=COPY_ALWAYS",
                "-v",
                f"{config_dir}:/var/lib/kolla/config_files:ro",
                KEYSTONE_IMAGE,
            ]
        )

        keystone_ip = test_utils.get_container_ip(self.container_name)
        self.auth_url = "http://%s:5000/v3" % keystone_ip
        self._bootstrap("http://%s:5000/v3/" % keystone_ip)
        self._wait_until_ready()

        keystone_client = kc_v3.Client(session=self.get_admin_session())
        self.project_id = keystone_client.projects.find(name=PROJECT_NAME).id

        self._configure_coriolis()

    def teardown(self):
        coriolis_utils.exec_process(["docker", "stop", self.container_name])
        coriolis_utils.exec_process(["docker", "rm", self.container_name])

    def get_auth(self):
        """Returns a keystoneauth plugin for the bootstrapped admin user."""
        return ks_identity.v3.Password(
            auth_url=self.auth_url,
            username=self.admin_username,
            password=self.admin_password,
            project_name=PROJECT_NAME,
            user_domain_name=DOMAIN_NAME,
            project_domain_name=DOMAIN_NAME,
        )

    def get_admin_session(self):
        return ks_session.Session(auth=self.get_auth())

    # mariadb may not have finished starting up right after `docker run -d`, and the
    # one-shot `docker exec mysql` below has no retry of its own.
    @coriolis_utils.retry_on_error(
        max_attempts=60,
        sleep_seconds=1,
        retried_exceptions=(exception.CoriolisException,),
    )
    def _create_database(self):
        """Create the "keystone" schema on the shared mariadb container."""
        coriolis_utils.exec_process(
            [
                "docker",
                "exec",
                self._db_container_name,
                "mysql",
                "-u",
                self._db_username,
                f"-p{self._db_password}",
                "-e",
                "CREATE DATABASE IF NOT EXISTS keystone;",
            ]
        )

    def _write_config_files(self):
        """Writes the kolla config files and returns their directory."""
        db_url = ('mysql+pymysql://%(user)s:%(password)s@%(host)s:3306/keystone') % {
            "user": self._db_username,
            "password": self._db_password,
            "host": self._db_ip,
        }

        config_dir = os.path.join(self._workdir, "keystone_config")
        os.makedirs(config_dir)

        keystone_conf = (
            "[DEFAULT]\n"
            "log_file = /var/log/kolla/keystone/keystone.log\n"
            "use_stderr = True\n\n"
            "[database]\n"
            "connection = %(db_url)s\n"
            "max_retries = -1\n"
            "retry_interval = 1\n\n"
            "[token]\n"
            "provider = fernet\n\n"
            "[cache]\n"
            "enabled = False\n\n"
            "[oslo_concurrency]\n"
            "lock_path = /var/lib/keystone/tmp\n"
        ) % {"db_url": db_url}

        with open(os.path.join(config_dir, "keystone.conf"), "w") as f:
            f.write(keystone_conf)
        with open(os.path.join(config_dir, "keystone-uwsgi.ini"), "w") as f:
            f.write(_UWSGI_INI)
        with open(os.path.join(config_dir, "keystone-startup.sh"), "w") as f:
            f.write(_STARTUP_SH)

        # Its only purpose is to force kolla_set_configs to create the
        # /var/log/kolla/keystone directory that Keystone logs into (it only
        # creates parent directories of "config_files" destinations).
        with open(os.path.join(config_dir, "log_dir_placeholder"), "w"):
            pass

        config_json = {
            "command": "/usr/bin/keystone-startup.sh",
            "config_files": [
                {
                    "source": "/var/lib/kolla/config_files/keystone-startup.sh",
                    "dest": "/usr/bin/keystone-startup.sh",
                    "owner": "root",
                    "perm": "0755",
                },
                {
                    "source": "/var/lib/kolla/config_files/keystone.conf",
                    "dest": "/etc/keystone/keystone.conf",
                    "owner": "keystone",
                    "perm": "0600",
                },
                {
                    "source": "/var/lib/kolla/config_files/keystone-uwsgi.ini",
                    "dest": "/etc/keystone/keystone-uwsgi.ini",
                    "owner": "root",
                    "perm": "0644",
                },
                {
                    "source": "/var/lib/kolla/config_files/log_dir_placeholder",
                    "dest": "/var/log/kolla/keystone/.placeholder",
                    "owner": "root",
                    "perm": "0644",
                },
            ],
        }
        with open(os.path.join(config_dir, "config.json"), "w") as f:
            json.dump(config_json, f)

        return config_dir

    def _manage(self, *args):
        coriolis_utils.exec_process(
            [
                "docker",
                "exec",
                self.container_name,
                "keystone-manage",
                "--config-file",
                "/etc/keystone/keystone.conf",
            ]
            + list(args)
        )

    def _bootstrap(self, keystone_url):
        self._manage("db_sync")
        self._manage(
            "fernet_setup",
            "--keystone-user",
            "keystone",
            "--keystone-group",
            "keystone",
        )
        self._manage(
            "bootstrap",
            "--bootstrap-username",
            self.admin_username,
            "--bootstrap-password",
            self.admin_password,
            "--bootstrap-project-name",
            PROJECT_NAME,
            "--bootstrap-role-name",
            ROLE_NAME,
            "--bootstrap-service-name",
            "keystone",
            "--bootstrap-region-id",
            "RegionOne",
            "--bootstrap-admin-url",
            keystone_url,
            "--bootstrap-internal-url",
            keystone_url,
            "--bootstrap-public-url",
            keystone_url,
        )

    def _wait_until_ready(self, timeout=120):
        last_exc = None

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(self.auth_url, timeout=5) as resp:
                    if resp.status in (200, 300):
                        return
            except (urllib.error.URLError, OSError) as exc:
                last_exc = exc

            time.sleep(2)

        raise exception.CoriolisException(
            "Keystone at %s not ready after %ds: %s"
            % (self.auth_url, timeout, last_exc)
        )

    def _configure_coriolis(self):
        cfg.CONF.set_override('auth_url', self.auth_url, group='keystone')
        cfg.CONF.set_override('identity_api_version', 3, group='keystone')

        # The "password" auth plugin's own options (auth_url, username, ...)
        # only get registered into CONF once something loads that plugin for
        # a given group; register them upfront so set_override() below works.
        password_plugin_opts = ks_loading.get_auth_plugin_conf_options('password')
        for group in ('trustee', 'keystone_authtoken'):
            cfg.CONF.register_opts(password_plugin_opts, group=group)
            cfg.CONF.set_override('auth_type', 'password', group=group)
            cfg.CONF.set_override('auth_url', self.auth_url, group=group)
            cfg.CONF.set_override('username', self.admin_username, group=group)
            cfg.CONF.set_override('password', self.admin_password, group=group)
            cfg.CONF.set_override('user_domain_name', DOMAIN_NAME, group=group)

            if group != 'trustee':
                # coriolis.keystone loads [trustee] with an explicit trust_id once a
                # trust exists. A plugin can't be scoped to both a project and a trust
                # at once, so [trustee] must stay unscoped (unset project_name /
                # project_domain_name), while [keystone_authtoken]'s own SA plugin
                # (used to validate incoming tokens, never trust-scoped) needs both.
                cfg.CONF.set_override('project_name', PROJECT_NAME, group=group)
                cfg.CONF.set_override('project_domain_name', DOMAIN_NAME, group=group)

        cfg.CONF.set_override(
            'www_authenticate_uri', self.auth_url, group='keystone_authtoken'
        )
        cfg.CONF.set_override('interface', 'public', group='keystone_authtoken')
