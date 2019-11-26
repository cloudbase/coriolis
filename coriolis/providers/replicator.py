# Copyright 2019 Cloudbase Solutions Srl
# All Rights Reserved.

import errno
import json
import os
import shutil
import tempfile
import time
import uuid

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units
from sshtunnel import SSHTunnelForwarder
import paramiko
import requests

from coriolis import exception
from coriolis import utils


LOG = logging.getLogger(__name__)

HASH_METHOD_SHA256 = "sha256"
HASH_METHOD_XXHASH = "xxhash"

REPLICATOR_PATH = "/usr/bin/replicator"
REPLICATOR_DIR = "/etc/coriolis-replicator"
REPLICATOR_STATE = "/tmp/replicator_state.json"
REPLICATOR_USERNAME = "replicator"
REPLICATOR_GROUP_NAME = "replicator"
REPLICATOR_SVC_NAME = "coriolis-replicator"

DEFAULT_REPLICATOR_PORT = 4433

replicator_opts = [
    cfg.IntOpt('port',
               default=DEFAULT_REPLICATOR_PORT,
               help='The replicator TCP port.'),
]

CONF = cfg.CONF
CONF.register_opts(replicator_opts, 'replicator')


class Client(object):

    def __init__(self, ip, port, credentials, ssh_conn_info,
                 event_handler, use_compression=False,
                 use_tunnel=False):
        self._ip = ip
        self._use_tunnel = use_tunnel
        self._port = port
        self._event_manager = event_handler
        self._creds = credentials
        self._ssh_conn_info = ssh_conn_info
        self._use_compression = use_compression
        self._cli = self._get_session()
        self._tunnel = None
        self._ip_via_tunnel = None
        self._port_via_tunnel = None
        self._test_connection()

    @property
    def repl_host(self):
        if self._ip_via_tunnel is not None:
            return self._ip_via_tunnel
        return self._ip

    @property
    def repl_port(self):
        if self._port_via_tunnel is not None:
            return self._port_via_tunnel
        return self._port

    @property
    def _base_uri(self):
        return "https://%s:%s" % (self.repl_host, self.repl_port)

    def _setup_tunnel_connection(self):
        self._tunnel = self._get_ssh_tunnel()
        self._tunnel.start()
        host, port = self._tunnel.local_bind_address
        self._ip_via_tunnel = host
        self._port_via_tunnel = port

    def _test_connection(self):
        """
        Attempt to connect to the IP/port pair. If direct connection
        fails, set up a SSH tunnel and attempt a connection through that.
        """
        if self._use_tunnel:
            # It was explicitly requested to use a tunnel
            self._setup_tunnel_connection()
        else:
            self._event_manager.progress_update(
                "Testing direct connection to replicator (%s:%s)" % (
                    self._ip, self._port))
            try:
                utils.wait_for_port_connectivity(
                    self._ip, self._port, max_wait=30)
                return
            except BaseException as err:
                LOG.debug("failed to connect to %s:%s Error: %s "
                          "Trying tunneled connection" % (
                              self._ip, self._port, err))
                self._event_manager.progress_update(
                    "Direct connection to replicator failed. "
                    "Setting up tunnel.")
                self._setup_tunnel_connection()

        self._event_manager.progress_update(
            "Testing connection to replicator (%s:%s)" % (
                self.repl_host, self.repl_port))
        try:
            utils.wait_for_port_connectivity(
                self.repl_host, self.repl_port, max_wait=30)
        except BaseException as err:
            self._tunnel.stop()
            LOG.warning(
                "failed to connect to replicator: %s" % err)
            raise

    def _get_ssh_tunnel(self):
        """
        gets a SSH tunnel object. Note, this does not start the tunnel,
        it simply creates the object, without actually connecting.
        """
        remote_host = self._ssh_conn_info["hostname"]
        remote_port = self._ssh_conn_info["port"]
        remote_user = self._ssh_conn_info["username"]
        local_host = "127.0.0.1"
        remote_port = self._ssh_conn_info.get("port", 22)

        pkey = self._ssh_conn_info.get("pkey")
        password = self._ssh_conn_info.get("password")
        if any([pkey, password]) is False:
            raise exception.CoriolisException(
                "Either password or pkey is required")

        server = SSHTunnelForwarder(
            (remote_host, remote_port),
            ssh_username=remote_user,
            ssh_pkey=pkey,
            ssh_password=password,
            # bind to remote replicator port
            remote_bind_address=(local_host, self._port),
            # select random port on this end.
            local_bind_address=(local_host, 0),
        )
        return server

    def raw_disk_uri(self, disk_name):
        diskUri = "%s/device/%s" % (self._base_uri, disk_name)
        return diskUri

    def _get_session(self):
        sess = requests.Session()
        sess.cert = (
            self._creds["client_cert"],
            self._creds["client_key"])
        sess.verify = self._creds["ca_cert"]
        return sess

    @utils.retry_on_error()
    def get_status(self, device=None, brief=True):
        uri = "%s/api/v1/dev/" % (self._base_uri)
        if device is not None:
            uri = "%s/%s/" % (uri, device)
        params = {
            "brief": brief,
        }
        status = self._cli.get(uri, params=params)
        status.raise_for_status()
        return status.json()

    @utils.retry_on_error()
    def get_chunks(self, device, skip_zeros=False):
        uri = "%s/api/v1/dev/%s/chunks/" % (self._base_uri, device)
        params = {
            "skipZeros": skip_zeros,
        }
        chunks = self._cli.get(uri, params=params)
        chunks.raise_for_status()
        return chunks.json()

    @utils.retry_on_error()
    def get_changes(self, device):
        uri = "%s/api/v1/dev/%s/chunks/changes/" % (self._base_uri, device)
        chunks = self._cli.get(uri)
        chunks.raise_for_status()
        return chunks.json()

    @utils.retry_on_error()
    def get_disk_size(self, disk):
        diskUri = self.raw_disk_uri(disk)
        info = self._cli.head(diskUri)
        info.raise_for_status()
        return int(info.headers["Content-Length"])

    @utils.retry_on_error()
    def download_chunk(self, disk, chunk):
        diskUri = self.raw_disk_uri(disk)

        offset = int(chunk["offset"])
        end = offset + int(chunk["length"]) - 1

        headers = {
            "Range": "bytes=%s-%s" % (offset, end),
        }
        if self._use_compression is False:
            headers["Accept-encoding"] = "identity"

        data = self._cli.get(
            diskUri, headers=headers)
        data.raise_for_status()
        return data.content


class Replicator(object):

    def __init__(self, conn_info, event_manager, volumes_info, replica_state,
                 use_compression=None, ignore_mounted=True,
                 hash_method=HASH_METHOD_SHA256, watch_devices=True,
                 chunk_size=10485760, use_tunnel=False):
        self._event_manager = event_manager
        self._repl_state = replica_state
        self._conn_info = conn_info
        self._config_dir = None
        self._cert_dir = tempfile.mkdtemp()
        self._volumes_info = volumes_info

        self._use_compression = use_compression
        if self._use_compression is None:
            self._use_compression = CONF.compress_transfers

        self._watch_devices = watch_devices
        self._hash_method = hash_method
        self._ignore_mounted = ignore_mounted
        self._chunk_size = chunk_size
        self._ssh = self._setup_ssh()
        self._credentials = None
        self._cli = None
        self._use_tunnel = use_tunnel

    def __del__(self):
        if self._cert_dir is not None:
            utils.ignore_exceptions(
                shutil.rmtree)(self._cert_dir)

    def _init_replicator_client(self, credentials):
        """
        Helper function to create an instance of the replicator
        client.
        """
        ssh_conn_info = self._parse_source_ssh_conn_info(
            self._conn_info)
        args = self._parse_replicator_conn_info(
            self._conn_info)
        self._cli = Client(
            args["ip"], args["port"],
            credentials, ssh_conn_info,
            self._event_manager,
            use_compression=self._use_compression,
            use_tunnel=self._use_tunnel)

    def _setup_ssh(self):
        args = self._parse_source_ssh_conn_info(
            self._conn_info)
        ssh = self._get_ssh_client(args)
        return ssh

    def _reconnect_ssh(self):
        if self._ssh:
            utils.ignore_exceptions(self._ssh.close)()

        self._ssh = self._setup_ssh()
        return self._ssh

    def init_replicator(self):
        self._credentials = utils.retry_on_error()(
            self._setup_replicator)(self._ssh)
        utils.retry_on_error()(
            self._init_replicator_client)(self._credentials)
        LOG.debug(
            "Disk status after Replicator initialization: %s",
            self._cli.get_status(device=None, brief=True))

    def get_current_disks_status(self):
        """ Returns a list of the current status of the attached data disks.
        The root disk of the worker VM is NOT returned.
        The result is a list with elements of the following format:
        [{
            'device-path': '/dev/xvdf',
            'device-name': 'xvdf',
            'size': 10737418240,
            'checksum-algorithm': 'sha256',
            'chunk-size': 10485760,
            'logical-sector-size': 512,
            'physical-sector-size': 512,
            'alignment-offset': 0,
            'has-mounted-partitions': False,
            'checksum-status': {
                'status': 'running',
                'total-chunks': 1024,
                'completed-chunks': 0,
                'percentage': 0},
            'partitions': [{
                'name': 'xvdf1',
                'sectors': 20969439,
                'uuid': '73e9659d-2fd9-46ca-a341-5a8637c416ee',
                'fs': 'ext4',
                'start-sector': 2048,
                'end-sector': 20971486,
                'alignment-offset': 0}]
        }]
        """
        return self._cli.get_status()

    def attach_new_disk(
            self, disk_id, attachf, previous_disks_status=None,
            retry_period=30, retry_count=10):
        """ Returns the volumes_info for the disk attached by running
        `attachf`. This is achieved by comparing the disk statuses before and
        after the execution of the attachment operation.

        param disk_id: str(): the 'disk_id' of the info from self._volumes_info
            for the disk which shall be attached by `attachf()`.
        param attachf: fun(): argument-less function which attaches the disk.
            The function should perform any waits required to reasonably expect
            the worker OS to have noticed the new disk, or reboot the worker VM
            and re-run `init_replicator()` if deemed necessary.
        param previous_disks_status: dict(): previous status of the disks as
            returned by `get_current_disks_status()`.

        return: dict(): returns the volumes_info associated to the new disk.
        """
        # check if volume with given ID is present in self._volumes_info:
        matching_vols = [
            vol for vol in self._volumes_info
            if vol['disk_id'] == disk_id]
        if not matching_vols:
            raise exception.CoriolisException(
                "No information regarding volume with ID '%s'. "
                "Cannot track its attachment." % disk_id)
        if len(matching_vols) > 1:
            raise exception.CoriolisException(
                "Multiple volumes info with ID '%s' found: %s" % (
                    disk_id, matching_vols))
        vol_info = matching_vols[0]

        # get/refresh current device paths:
        if not previous_disks_status:
            previous_disks_status = self._cli.get_status()
        LOG.debug(
            "Disks status pre-attachment of %s: %s",
            disk_id, previous_disks_status)
        previous_device_paths = [
            dev['device-path'] for dev in previous_disks_status]

        # run attachment function and get new device paths:
        attachf()

        # graciously wait for disk to appear:
        new_disks_status = None
        new_device_paths = None
        for i in range(retry_count):
            new_disks_status = self._cli.get_status()
            new_device_paths = [dev['device-path'] for dev in new_disks_status]
            LOG.debug(
                "Polled devices while waiting for disk '%s' to attach "
                "(try %d/%d): %s", disk_id, i+1, retry_count, new_device_paths)

            # check for missing/multiple new device paths:
            missing_device_paths = (
                set(previous_device_paths) - set(new_device_paths))
            if missing_device_paths:
                LOG.warn(
                    "The following devices from the previous disk state qeury "
                    "are no longer detected: %s", [
                        dev for dev in previous_disks_status
                        if dev['device-path'] in missing_device_paths])

            new_device_paths = set(new_device_paths) - set(previous_device_paths)
            if new_device_paths:
                break
            else:
                LOG.debug(
                    "Sleeping %d seconds for disk '%s' to get attached.",
                    retry_period, disk_id)
                time.sleep(retry_period)

        if not new_device_paths:
            raise exception.CoriolisException(
                "No new device paths have appeared after volume attachment of "
                "disk with ID: %s" % disk_id)
        if len(new_device_paths) > 1:
            raise exception.CoriolisException(
                "Multiple device paths have appeared after attachment of disk "
                "with ID %s: %s" % (
                    disk_id,
                    [dev for dev in new_disks_status
                     if dev['device-path'] in new_device_paths]))

        # record the new 'disk_path' for the volume:
        vol_info['disk_path'] = new_device_paths.pop()
        LOG.debug(
            "New device following attachment of disk '%s': %s",
            disk_id, vol_info['disk_path'])

        return vol_info

    def wait_for_chunks(self):
        if self._cli is None:
            raise exception.CoriolisException(
                "replicator not initialized. Run init_replicator()")
        perc_steps = {}

        while True:
            status = self._cli.get_status()
            done = []
            for vol in status:
                devName = vol["device-path"]
                perc_step = perc_steps.get(devName)
                if perc_step is None:
                    perc_step = self._event_manager.add_percentage_step(
                        100,
                        message_format=("Chunking progress for disk %s: "
                                        "{:.0f}%%") % devName)
                    perc_steps[devName] = perc_step
                perc_done = vol["checksum-status"]["percentage"]
                self._event_manager.set_percentage_step(
                    perc_step, perc_done)
                done.append(int(perc_done) == 100)
            if all(done):
                break
            else:
                time.sleep(5)

    @utils.retry_on_error()
    def _get_ssh_client(self, args):
        """
        gets a paramiko SSH client
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(**args)
        return ssh

    def _parse_source_ssh_conn_info(self, conn_info):
        # if we get valid SSH connection info we can
        # use it to copy the binary, and potentially
        # create a SSH tunnel through which we will
        # connect to the coriolis replicator
        required = ('ip', 'username')
        port = conn_info.get('port', 22)
        password = conn_info.get('password', None)
        pkey = conn_info.get('pkey', None)
        for i in required:
            if conn_info.get(i) is None:
                raise exception.CoriolisException(
                    "missing required field: %s" % i)
        if any([password, pkey]) is False:
            raise exception.CoriolisException(
                "Either password or pkey is required")

        args = {
            "hostname": conn_info["ip"],
            "username": conn_info["username"],
            "password": password,
            "pkey": pkey,
            "port": port,
        }
        return args

    def _get_replicator_state_file(self):
        """
        Looks for replicator state in volumes_info. If none
        is found, replicator will be initialized without it.
        """
        # if state is not present, just return an empty array
        # saves us the trouble of an extra if during the setup
        # of the replicator process
        state = self._repl_state
        filename = tempfile.mkstemp()[1]
        with open(filename, 'w') as fp:
            json.dump(state, fp)
        return filename

    def _parse_replicator_conn_info(self, conn_info):
        # The IP should be the same one as the SSH IP.
        # Only the port will differ, and that is configurable.
        ip = conn_info.get("ip", None)
        return {
            "ip": ip,
            "port": CONF.replicator.port,
        }

    @utils.retry_on_error()
    def _copy_file(self, ssh, localPath, remotePath):
        tmp = os.path.join("/tmp", str(uuid.uuid4()))

        sftp = paramiko.SFTPClient.from_transport(ssh.get_transport())
        try:
            # Check if the remote file already exists
            sftp.stat(remotePath)
        except IOError as ex:
            if ex.errno != errno.ENOENT:
                raise
            sftp.put(localPath, tmp)
            utils.exec_ssh_cmd(
                ssh, "sudo mv %s %s" % (tmp, remotePath), get_pty=True)
        finally:
            sftp.close()

    @utils.retry_on_error()
    def _copy_replicator_cmd(self, ssh):
        local_path = os.path.join(
            utils.get_resources_bin_dir(), 'replicator')
        self._copy_file(ssh, local_path, REPLICATOR_PATH)
        utils.exec_ssh_cmd(
            ssh, "sudo chmod +x %s" % REPLICATOR_PATH, get_pty=True)

    def _setup_replicator_group(self, ssh, group_name=REPLICATOR_GROUP_NAME):
        """ Sets up a group with the given name and adds the
        user we're connected as to it.

        Returns True if the group already existed, else False.
        """
        group_exists = utils.exec_ssh_cmd(
            ssh,
            "getent group %(group)s > /dev/null && echo 1 || echo 0" % {
                "group": REPLICATOR_GROUP_NAME})
        if int(group_exists) == 0:
            utils.exec_ssh_cmd(
                ssh, "sudo groupadd %s" % group_name, get_pty=True)
            # NOTE: this is required in order for the user we connected
            # as to be able to read the certs:
            # NOTE2: the group change will only take effect after we reconnect:
            utils.exec_ssh_cmd(
                ssh, "sudo usermod -aG %s %s" % (
                    REPLICATOR_GROUP_NAME, self._conn_info['username']),
                get_pty=True)

        return int(group_exists) == 1

    def _setup_replicator_user(self, ssh):
        # check for and create replicator user:
        user_exists = utils.exec_ssh_cmd(
            ssh,
            "getent passwd %(user)s > /dev/null && echo 1 || echo 0" % {
                "user": REPLICATOR_USERNAME})
        if int(user_exists) == 0:
            utils.exec_ssh_cmd(
                ssh, "sudo useradd -m -s /bin/bash -g %s %s" % (
                    REPLICATOR_GROUP_NAME, REPLICATOR_USERNAME),
                get_pty=True)
            utils.exec_ssh_cmd(
                ssh, "sudo usermod -aG disk %s" % REPLICATOR_USERNAME,
                get_pty=True)

    @utils.retry_on_error()
    def _exec_replicator(self, ssh, port, certs, state_file):
        cmdline = ("%(replicator_path)s run -hash-method=%(hash_method)s "
                   "-ignore-mounted-disks=%(ignore_mounted)s "
                   "-listen-port=%(listen_port)s "
                   "-chunk-size=%(chunk_size)s "
                   "-watch-devices=%(watch_devs)s "
                   "-state-file=%(state_file)s "
                   "-ca-cert=%(ca_cert)s -cert=%(srv_cert)s "
                   "-key=%(srv_key)s" % {
                       "replicator_path": REPLICATOR_PATH,
                       "hash_method": self._hash_method,
                       "ignore_mounted": json.dumps(self._ignore_mounted),
                       "watch_devs": json.dumps(self._watch_devices),
                       "listen_port": str(port),
                       "state_file": state_file,
                       "chunk_size": self._chunk_size,
                       "ca_cert": certs["ca_crt"],
                       "srv_cert": certs["srv_crt"],
                       "srv_key": certs["srv_key"],
                   })
        utils.create_service(
            ssh, cmdline, REPLICATOR_SVC_NAME,
            run_as=REPLICATOR_USERNAME)

    def _fetch_remote_file(self, ssh, remote_file, local_file):
        # TODO(gsamfira): make this re-usable
        with open(local_file, 'wb') as fd:
            data = utils.retry_on_error()(
                utils.read_ssh_file)(ssh, remote_file)
            fd.write(data)

    @utils.retry_on_error(sleep_seconds=5)
    def _setup_certificates(self, ssh, args):
        # TODO(gsamfira): coriolis-replicator and coriolis-writer share
        # the functionality of being able to generate certificates
        # This will either be replaced with proper certificate management
        # in Coriolis, and the needed files will be pushed to the services
        # that need them (userdata or ssh), or the two applications will be
        # merged into one, and we will deduplicate this functionallity.
        remote_base_dir = REPLICATOR_DIR
        ip = args["ip"]

        ca_crt_name = "ca-cert.pem"
        client_crt_name = "client-cert.pem"
        client_key_name = "client-key.pem"

        srv_crt_name = "srv-cert.pem"
        srv_key_name = "srv-key.pem"

        remote_ca_crt = os.path.join(remote_base_dir, ca_crt_name)
        remote_client_crt = os.path.join(remote_base_dir, client_crt_name)
        remote_client_key = os.path.join(remote_base_dir, client_key_name)
        remote_srv_crt = os.path.join(remote_base_dir, srv_crt_name)
        remote_srv_key = os.path.join(remote_base_dir, srv_key_name)

        ca_crt = os.path.join(self._cert_dir, ca_crt_name)
        client_crt = os.path.join(self._cert_dir, client_crt_name)
        client_key = os.path.join(self._cert_dir, client_key_name)

        exist = []
        for i in (remote_ca_crt, remote_client_crt, remote_client_key,
                  remote_srv_crt, remote_srv_key):
            exist.append(utils.test_ssh_path(ssh, i))

        force_fetch = False
        if not all(exist):
            utils.exec_ssh_cmd(
                ssh, "sudo mkdir -p %s" % remote_base_dir, get_pty=True)
            utils.exec_ssh_cmd(
                ssh,
                "sudo %(replicator_cmd)s gen-certs -output-dir "
                "%(cert_dir)s -certificate-hosts %(extra_hosts)s" % {
                    "replicator_cmd": REPLICATOR_PATH,
                    "cert_dir": remote_base_dir,
                    "extra_hosts": ip,
                },
                get_pty=True)
            utils.exec_ssh_cmd(
                ssh, "sudo chown -R %(user)s:%(group)s %(cert_dir)s" % {
                    "cert_dir": remote_base_dir,
                    "user": REPLICATOR_USERNAME,
                    "group": REPLICATOR_GROUP_NAME
                }, get_pty=True)
            utils.exec_ssh_cmd(
                ssh, "sudo chmod -R g+r %(cert_dir)s" % {
                    "cert_dir": remote_base_dir,
                }, get_pty=True)
            force_fetch = True

        exists = []
        for i in (ca_crt, client_crt, client_key):
            exists.append(os.path.isfile(i))

        if not all(exists) or force_fetch:
            # certificates either are missing, or have been regenerated
            # on the writer worker. We need to fetch them.
            self._fetch_remote_file(ssh, remote_ca_crt, ca_crt)
            self._fetch_remote_file(ssh, remote_client_crt, client_crt)
            self._fetch_remote_file(ssh, remote_client_key, client_key)

        return {
            "local": {
                "client_cert": client_crt,
                "client_key": client_key,
                "ca_cert": ca_crt,
            },
            "remote": {
                "srv_crt": remote_srv_crt,
                "srv_key": remote_srv_key,
                "ca_crt": remote_ca_crt,
            },
        }

    @utils.retry_on_error()
    def _setup_replicator(self, ssh):
        # copy the binary, set up the service, generate certificates,
        # start service
        state_file = self._get_replicator_state_file()
        self._copy_file(ssh, state_file, REPLICATOR_STATE)
        utils.exec_ssh_cmd(
            ssh, "sudo chmod 755 %s" % REPLICATOR_STATE, get_pty=True)
        os.remove(state_file)

        args = self._parse_replicator_conn_info(self._conn_info)
        self._copy_replicator_cmd(ssh)
        group_existed = self._setup_replicator_group(
            ssh, group_name=REPLICATOR_GROUP_NAME)
        if not group_existed:
            # NOTE: we must reconnect so that our user being added to the new
            # Replicator group can take effect:
            ssh = self._reconnect_ssh()
        self._setup_replicator_user(ssh)
        certs = self._setup_certificates(ssh, args)
        self._exec_replicator(
            ssh, args["port"], certs["remote"], REPLICATOR_STATE)
        return certs["local"]

    def _get_size_from_chunks(self, chunks):
        ret = 0
        for chunk in chunks:
            ret += chunk["length"]
        return ret / units.Mi

    def _find_vol_state(self, name, state):
        for vol in state:
            if vol["device-name"] == name:
                return vol
        return None

    def replicate_disks(self, source_volumes_info, backup_writer):
        """
        Fetch the block diff and send it to the backup_writer.
        If the target_is_zeroed parameter is set to True, on initial
        sync, zero blocks will be skipped. On subsequent syncs, even
        zero blocks will be synced, as we cannot tell if those zeros
        are part of a file or not.

        source_volumes_info should be of the following format:
        [
            {
                "disk_id": the_provider_ID_of_the_volume,
                "disk_path": /dev/sdb,
            },
        ]
        """
        LOG.warning("Source volumes info is: %r" % source_volumes_info)
        state = self._repl_state
        isInitial = False
        if state is None or len(state) == 0:
            isInitial = True
        curr_state = self._cli.get_status(brief=False)

        for volume in source_volumes_info:
            dst_vol_idx = None
            for idx, vol in enumerate(self._volumes_info):
                if vol["disk_id"] == volume["disk_id"]:
                    dst_vol_idx = idx
                    break

            if dst_vol_idx is None:
                raise exception.CoriolisException(
                    "failed to find a coresponding volume in volumes_info"
                    " for %s" % volume["disk_id"])

            dst_vol = self._volumes_info[dst_vol_idx]

            devName = volume["disk_path"]
            if devName.startswith('/dev'):
                devName = devName[5:]

            state_for_vol = self._find_vol_state(devName, curr_state)
            if isInitial and dst_vol.get("zeroed", False) is True:
                # This is an initial sync of the disk, and we can
                # skip zero chunks
                chunks = self._cli.get_chunks(
                    devName, skip_zeros=True)
            else:
                # subsequent sync. Get changes.
                chunks = self._cli.get_changes(devName)

            size = self._get_size_from_chunks(chunks)

            msg = ("Disk replication progress for %s (%.3f MB):"
                   " {:.0f}%%") % (volume["disk_path"], size)
            perc_step = self._event_manager.add_percentage_step(
                len(chunks), message_format=msg)

            total = 0
            with backup_writer.open("", volume['disk_id']) as destination:
                for chunk in chunks:
                    offset = int(chunk["offset"])
                    destination.seek(offset)
                    data = self._cli.download_chunk(devName, chunk)
                    destination.write(data)
                    total += 1
                    self._event_manager.set_percentage_step(
                        perc_step, total)
            dst_vol["replica_state"] = state_for_vol

        self._repl_state = curr_state
        return self._repl_state

    def _download_full_disk(self, disk, path):
        self._event_manager.progress_update(
            "Downloading %s as thick file" % disk)
        diskUri = self._cli.raw_disk_uri(disk)
        size = self._cli.get_disk_size(disk)

        perc_step = self._event_manager.add_percentage_step(
            size, message_format="Downloading disk /dev/%s : "
            "{:.0f}%%" % disk)

        total = 0
        with self._cli._cli.get(diskUri, stream=True) as dw:
            with open(path, 'wb') as dsk:
                for chunk in dw.iter_content(chunk_size=self._chunk_size):
                    if chunk:
                        writen = dsk.write(chunk)
                        total += writen
                    self._event_manager.set_percentage_step(
                        perc_step, total)

    def _download_sparse_disk(self, disk, path, chunks):
        self._event_manager.progress_update(
            "Downloading %s as sparse file" % disk)
        size = self._cli.get_disk_size(disk)
        size_from_chunks = self._get_size_from_chunks(chunks)
        total = 0
        with open(path, 'wb') as fp:
            # create sparse file
            fp.truncate(size)
            perc_step = self._event_manager.add_percentage_step(
                len(chunks), message_format="Disk download progress for "
                "/dev/%s (%s MB): {:.0f}%%" % (disk, size_from_chunks))
            for chunk in chunks:
                offset = int(chunk["offset"])
                # seek to offset
                fp.seek(offset)

                data = self._cli.download_chunk(disk, chunk)
                fp.write(data)

                total += 1
                self._event_manager.set_percentage_step(
                    perc_step, total)

    def download_disk(self, disk, path):
        """
        Download the disk from source, into path. If client calls
        wait_for_chunks() before executing this function, a sparse
        file will be created with written chunks, otherwise, the result
        will be a thickly provisioned disk file.
        """
        diskName = disk
        if diskName.startswith("/dev"):
            # just get the name
            diskName = diskName[5:]
        chunks = self._cli.get_chunks(
            device=diskName, skip_zeros=True)
        if len(chunks) == 0:
            self._download_full_disk(diskName, path)
        else:
            self._download_sparse_disk(diskName, path, chunks)
