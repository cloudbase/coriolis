from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class ReplicatorClient(object):

    def __init__(self, conn_info, event_handler, volumes_info, use_gzip=False):
        self._event = event_handler
        self._conn_info = conn_info
        self._stdout = None
        self._stdin = None
        self._stderr = None
        self._volumes_info = volumes_info
        self._use_gzip = use_gzip

    def _parse_ssh_conn_info(self, conn_info):
        # if we get valid SSH connection info we can
        # use it to copy the binary, and potentially
        # create a SSH tunnel through which we will
        # connect to the coriolis replicator
        pass

    def _get_replicator_state_file(self, volumes_info):
        """
        Looks for replicator state in volumes_info. If none
        is found, replicator will be initialized without it.
        """
        pass

    def _parse_replicator_conn_info(self, conn_info):
        # the replicator only really needs an IP and
        # a port. Authentication is done using client
        # certificates, which we fetch after the
        # application is started.
        pass

    def _setup_replicator(self):
        # copy the binary and execute it.
        pass

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
        pass

    def replicate_disks(self, backup_writer):
        """
        Fetch the block diff and send it to the backup_writer
        """
        pass

    def download_disk(self, disk, path):
        """
        Download the disk from source, into path. This will result
        in a sparse RAW file.
        """
        pass
