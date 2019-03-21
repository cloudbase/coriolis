import tempfile
import zipfile
import json
import paramiko
import threading
import errno
import os
import time
import requests

from coriolis import utils
from coriolis import exception

from oslo_log import log as logging
from oslo_utils import units

from sshtunnel import SSHTunnelForwarder

LOG = logging.getLogger(__name__)

HASH_METHOD_SHA256 = "sha256"
HASH_METHOD_XXHASH = "xxhash"

REPLICATOR_PATH = "/usr/bin/replicator"
REPLICATOR_STATE = "/tmp/replicator_state.json"
REPLICATOR_USERNAME = "replicator"
DEFAULT_REPLICATOR_PORT = 4433


class Client(object):

    def __init__(self, ip, port, credentials, ssh_conn_info,
                 event_handler, use_compression=False):
        self._ip = ip
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

    def __del__(self):
        if self._tunnel is not None:
            try:
                self._tunnel.stop()
            except BaseException as err:
                LOG.warning(
                    "failed to stop tunnel: %s" % err)

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
        self._event_manager.progress_update(
            "Testing direct connection to replicator (%s:%s)" % (
                self._ip, self._port))
        try:
            utils.wait_for_port_connectivity(
                self._ip, self._port, max_wait=2)
            return
        except BaseException as err:
            LOG.debug("failed to connect to %s:%s Error: %s "
                      "Trying tunneled connection" % (
                          self._ip, self._port, err))
            self._event_manager.progress_update(
                "Direct connection to replicator failed. Setting up tunnel.")
            self._setup_tunnel_connection()

        self._event_manager.progress_update(
            "Testing tunneled connection to replicator (%s:%s)" % (
                self.repl_host, self.repl_port))
        try:
            utils.wait_for_port_connectivity(
                self.repl_host, self.repl_port, max_wait=30)
        except BaseException as err:
            self._tunnel.stop()
            LOG.warning(
                "failed to connect to replicator through tunnel: %s" % err)
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

    def get_chunks(self, device, skip_zeros=False):
        uri = "%s/api/v1/dev/%s/chunks/" % (self._base_uri, device)
        params = {
            "skipZeros": skip_zeros,
        }
        chunks = self._cli.get(uri, params=params)
        chunks.raise_for_status()
        return chunks.json()

    def get_disk_size(self, disk):
        diskUri = self.raw_disk_uri(disk)
        info = self._cli.head(diskUri)
        info.raise_for_status()
        return int(info.headers["Content-Length"])

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
                 use_compression=False, ignore_mounted=True,
                 hash_method=HASH_METHOD_SHA256, watch_devices=True,
                 chunk_size=10485760):
        self._event_manager = event_manager
        self._repl_state = replica_state
        self._conn_info = conn_info
        self._config_dir = None
        self._cert_dir = None
        self._lock = threading.Lock()
        self._volumes_info = volumes_info
        self._use_compression = use_compression
        self._watch_devices = watch_devices
        self._hash_method = hash_method
        self._ignore_mounted = ignore_mounted
        self._chunk_size = chunk_size
        self._ssh = self._setup_ssh()
        self._credentials = None
        self._cli = None
        self._stdout = None
        self._stdin = None
        self._stderr = None

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
            use_compression=self._use_compression)

    def _setup_ssh(self):
        args = self._parse_source_ssh_conn_info(
            self._conn_info)
        ssh = self._get_ssh_client(args)
        return ssh

    def init_replicator(self):
        utils.retry_on_error()(self._setup_replicator)(self._ssh)
        self._credentials = utils.retry_on_error()(
            self._fetch_certificates)()
        utils.retry_on_error()(
            self._init_replicator_client)(self._credentials)

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
                        message_format=("Disk %s chunk processing progress: "
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
        # the replicator only really needs an IP and
        # a port. Authentication is done using client
        # certificates, which we fetch after the
        # application is started.

        # The IP should be the same one as the SSH IP.
        # Only the port will differ
        ip = conn_info.get("ip", None)
        port = conn_info.get("replicator_port", DEFAULT_REPLICATOR_PORT)
        return {
            "ip": ip,
            "port": port,
        }

    def _copy_file(self, ssh, localPath, remotePath):
        tmp = tempfile.mkstemp()[1]
        try:
            os.remove(tmp)
        except BaseException:
            pass

        with self._lock:
            sftp = ssh.open_sftp()
            try:
                # Check if the remote file already exists
                sftp.stat(remotePath)
            except IOError as ex:
                if ex.errno != errno.ENOENT:
                    raise
                sftp.put(localPath, tmp)
                utils.exec_ssh_cmd(ssh, "sudo mv %s %s" % (tmp, remotePath))
            finally:
                sftp.close()

    @utils.retry_on_error()
    def _copy_replicator_cmd(self, ssh):
        local_path = os.path.join(
            utils.get_resources_dir(), 'replicator')
        self._copy_file(ssh, local_path, REPLICATOR_PATH)
        utils.exec_ssh_cmd(ssh, "sudo chmod +x %s" % REPLICATOR_PATH)

    def _setup_replicator_user(self, ssh):
        user_exists = utils.exec_ssh_cmd(
            ssh, "getent passwd replicator > /dev/null && echo 1 || echo 0")
        if int(user_exists) == 0:
            utils.exec_ssh_cmd(
                ssh, "sudo useradd -m -s /bin/bash %s" % REPLICATOR_USERNAME)
            utils.exec_ssh_cmd(
                ssh, "sudo usermod -aG disk %s" % REPLICATOR_USERNAME)

    @utils.retry_on_error()
    def _exec_replicator(self, ssh, args, state_file):
        ip = args["ip"]
        # add 127.0.0.1 to the mix. If we need to tunnel, we will need it
        cert_hosts = ",".join([ip, "127.0.0.1"])
        port = args["port"]
        self._config_dir = utils.exec_ssh_cmd(
            ssh, "mktemp -d").decode().rstrip("\n")
        utils.exec_ssh_cmd(
            ssh,
            "sudo chown %(user)s:%(user)s %(config_dir)s" % {
                "config_dir": self._config_dir,
                "user": REPLICATOR_USERNAME,
            })
        cmdline = ("/usr/bin/replicator -certificate-hosts=%(cert_hosts)s "
                   "-config-dir=%(cfgdir)s -hash-method=%(hash_method)s "
                   "-ignore-mounted-disks=%(ignore_mounted)s "
                   "-listen-port=%(listen_port)s "
                   "-chunk-size=%(chunk_size)s "
                   "-watch-devices=%(watch_devs)s "
                   "-state-file=%(state_file)s" % {
                       "cfgdir": self._config_dir,
                       "cert_hosts": cert_hosts,
                       "hash_method": self._hash_method,
                       "ignore_mounted": json.dumps(self._ignore_mounted),
                       "watch_devs": json.dumps(self._watch_devices),
                       "listen_port": str(port),
                       "state_file": state_file,
                       "chunk_size": self._chunk_size,
                   })
        self._event_manager.progress_update("running %s" % cmdline)
        self._stdin, self._stdout, self._stderr = ssh.exec_command(
            "sudo -u %(username)s -- %(cmdline)s > /tmp/replicator.log" % {
                "cmdline": cmdline,
                "username": REPLICATOR_USERNAME,
            })
        count = 0
        # wait 5 seconds. If process exits, raise
        # TODO(gsamfira): create system service? That should take care of
        # restarting the replicator process if it fails.
        while True:
            if count >= 5:
                break
            if self._stdout.channel.exit_status_ready():
                exit_code = self._stdout.channel.recv_exit_status()
                if exit_code:
                    stderr = self._stderr.read()
                    stdout = self._stdout.read()
                    raise exception.CoriolisException(
                        "failed to start replicator: stdout: "
                        "%s; stderr: %s" % (stdout, stderr))
            time.sleep(1)
            count += 1

    @utils.retry_on_error()
    def _setup_replicator(self, ssh):
        # copy the binary and execute it.
        state_file = self._get_replicator_state_file()
        self._copy_file(ssh, state_file, REPLICATOR_STATE)
        utils.exec_ssh_cmd(ssh, "sudo chmod 755 %s" % REPLICATOR_STATE)

        args = self._parse_replicator_conn_info(self._conn_info)
        self._copy_replicator_cmd(ssh)
        self._setup_replicator_user(ssh)
        self._exec_replicator(ssh, args, REPLICATOR_STATE)

    def _fetch_certificates(self):
        """
        Fetch the client certificates
        Returns a dict with paths to the certificates
        {
            "client_cert": "/tmp/tmp.RAA6wsQG4s/client-cert.pem",
            "client_key": "/tmp/tmp.RAA6wsQG4s/client-key.pem",
            "ca_cert": "/tmp/tmp.RAA6wsQG4s/ca-cert.pem",
        }
        """
        if self._cert_dir is None:
            self._cert_dir = tempfile.mkdtemp()

        clientCrt = os.path.join(self._cert_dir, "client-cert.pem")
        clientKey = os.path.join(self._cert_dir, "client-key.pem")
        caCert = os.path.join(self._cert_dir, "ca-cert.pem")

        def progressf(curr, total):
            LOG.debug("Copied %s/%s", curr, total)

        if self._config_dir is None:
            raise exception.CoriolisException(
                "Not initialized. Run _setup_replicator().")

        localCertZip = os.path.join(self._cert_dir, "client-creds.zip")
        zipFile = os.path.join(
            self._config_dir, "ssl/client/client-creds.zip")

        utils.exec_ssh_cmd(
            self._ssh, "sudo cp -f %s /tmp/creds.zip" % zipFile)
        utils.exec_ssh_cmd(
            self._ssh, "sudo chmod +r /tmp/creds.zip")

        with self._lock:
            sftp = self._ssh.open_sftp()
            try:
                sftp.get("/tmp/creds.zip", localCertZip, callback=progressf)
            finally:
                sftp.close()

        zFile = zipfile.ZipFile(localCertZip)
        zFile.extractall(path=self._cert_dir)
        return {
            "client_cert": clientCrt,
            "client_key": clientKey,
            "ca_cert": caCert,
        }

    def _update_state(self, volumes_info):
        """
        should be called from replicate_disks every time a disk
        is successfully synced. Coriolis does not yet send partial
        updates with migration info, so I suppose if we call it once
        on replica success, it should be enough.
        """
        pass

    def _get_size_from_chunks(self, chunks):
        ret = 0
        for chunk in chunks:
            ret += chunk["length"]
        return ret / units.Gi

    def replicate_disks(self, source_volumes_info, backup_writer):
        """
        Fetch the block diff and send it to the backup_writer.
        If the target_is_zeroed parameter is set to True, on initial
        sync, zero blocks will be skipped. On subsequent syncs, even
        zero blocks will be synced, as we cannot tell if those zeros
        are part of a file or not.

        source_volumes_info should be of the following format:

        {
            "disk_id": the_provider_ID_of_the_volume,
            "disk_path": /dev/sdb,
        }
        """
        state = self._volumes_info.get("replication_state", None)
        isInitial = False
        if state is None:
            isInitial = True

        for volume in source_volumes_info:
            dst_vol = None
            for vol in self._volumes_info:
                if vol["disk_id"] == volume["disk_id"]:
                    dst_vol = vol
                    break

            if dst_vol is None:
                raise exception.CoriolisException(
                    "failed to find a coresponding volume in volumes_info"
                    " for %s" % volume["disk_id"])

            devName = volume["disk_path"]
            if devName.startswith('/dev'):
                devNane = devName[5:]

            if isInitial and dst_vol.get("zeroed", False) is True:
                # This is an initial sync of the disk, and we can
                # skip zero chunks
                chunks = self._cli.get_chunks(
                    devNane, skip_zeros=True)
            else:
                # subsequent sync. Get changes.
                chunks = self._cli.get_changes(devNane)

            size = self._get_size_from_chunks(chunks)

            msg = ("Replicating disk %s (%s GB):"
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

        status = self._cli.get_status()
        self._volumes_info["replication_state"] = status
        return self._volumes_info

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
                len(chunks), message_format="Downloading disk /dev/%s (%s GB):"
                " {:.0f}%%" % (disk, size_from_chunks))
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
