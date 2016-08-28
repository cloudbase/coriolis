# Copyright 2016 Cloudbase Solutions Srl
# All Rights Reserved.

import abc
import contextlib
import os
import re
import struct
import sys
import threading
import time
from urllib import request
import uuid

import eventlet
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units
import paramiko
from pyVim import connect
from pyVmomi import vim

from coriolis import constants
from coriolis import exception
from coriolis.providers import base
from coriolis.providers.vmware_vsphere import guestid
from coriolis import schemas
from coriolis.providers.vmware_vsphere import vixdisklib
from coriolis import utils

vmware_vsphere_opts = [
    cfg.StrOpt('vdiskmanager_path',
               default='vmware-vdiskmanager',
               help='The vmware-vdiskmanager path.'),
]

CONF = cfg.CONF
CONF.register_opts(vmware_vsphere_opts, 'vmware_vsphere')

LOG = logging.getLogger(__name__)

vixdisklib.init()


class _BaseBackupWriter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def _open(self):
        pass

    @contextlib.contextmanager
    def open(self, path, disk_id):
        self._path = path
        self._disk_id = disk_id
        self._open()
        try:
            yield self
        finally:
            self.close()

    @abc.abstractmethod
    def seek(self, pos):
        pass

    @abc.abstractmethod
    def truncate(self, size):
        pass

    @abc.abstractmethod
    def write(self, data):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class _FileBackupWriter(_BaseBackupWriter):
    def _open(self):
        # Create file if it doesnt exist
        open(self._path, 'ab+').close()
        self._file = open(self._path, 'rb+')

    def seek(self, pos):
        self._file.seek(pos)

    def truncate(self, size):
        self._file.truncate(size)

    def write(self, data):
        self._file.write(data)

    def close(self):
        self._file.close()


class _SSHBackupWriter(_BaseBackupWriter):
    def __init__(self, ip, port, username, pkey, password, volumes_info):
        self._ip = ip
        self._port = port
        self._username = username
        self._pkey = pkey
        self._password = password
        self._volumes_info = volumes_info
        self._ssh = None

    @contextlib.contextmanager
    def open(self, path, disk_id):
        self._path = path
        self._disk_id = disk_id
        self._open()
        try:
            yield self
            # Don't send a message via ssh on exception
            self.close()
        except:
            self._ssh.close()
            raise

    @utils.retry_on_error()
    def _connect_ssh(self):
        LOG.info("Connecting to SSH host: %(ip)s:%(port)s" %
                 {"ip": self._ip, "port": self._port})
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(
            hostname=self._ip,
            port=self._port,
            username=self._username,
            pkey=self._pkey,
            password=self._password)

    @utils.retry_on_error()
    def _copy_helper_cmd(self):
        sftp = self._ssh.open_sftp()
        local_path = os.path.join(
            utils.get_resources_dir(), 'write_data')
        sftp.put(local_path, 'write_data')
        sftp.close()

    @utils.retry_on_error()
    def _exec_helper_cmd(self):
        self._msg_id = 0
        self._offset = 0
        self._stdin, self._stdout, self._stderr = self._ssh.exec_command(
            "chmod +x write_data && sudo ./write_data")

    def _encode_data(self, content):
        path = [v for v in self._volumes_info
                if v["disk_id"] == self._disk_id][0]["volume_dev"]

        LOG.info("Guest path: %s", path)
        LOG.info("Offset: %s", self._offset)
        LOG.info("Content len: %s", len(content))

        data_len = len(path) + 1 + 8 + len(content)
        return (struct.pack("<I", self._msg_id) +
                struct.pack("<I", data_len) +
                path.encode() + b'\0' +
                struct.pack("<Q", self._offset) +
                content)

    def _encode_eod(self):
        return struct.pack("<I", self._msg_id) + struct.pack("<I", 0)

    @utils.retry_on_error()
    def _send_msg(self, data):
        self._msg_id += 1
        self._stdin.write(data)
        self._stdin.flush()
        out_msg_id = self._stdout.read(4)

    def _open(self):
        self._connect_ssh()
        self._copy_helper_cmd()
        self._exec_helper_cmd()

    def seek(self, pos):
        self._offset = pos

    def truncate(self, size):
        pass

    def write(self, data):
        self._send_msg(self._encode_data(data))
        self._offset += len(data)

    def close(self):
        self._send_msg(self._encode_eod())
        ret_val = self._stdout.channel.recv_exit_status()
        if ret_val:
            raise exception.CoriolisException(
                "An exception occurred while writing data on target. "
                "Error code: %s" % ret_val)
        self._ssh.close()


class ExportProvider(base.BaseReplicaExportProvider):

    connection_info_schema = schemas.get_schema(
        __name__, schemas.PROVIDER_CONNECTION_INFO_SCHEMA_NAME)

    @utils.retry_on_error()
    def _convert_disk_type(self, disk_path, target_disk_path, target_type=0):
        utils.exec_process([CONF.vmware_vsphere.vdiskmanager_path, "-r",
                            disk_path, "-t", str(target_type),
                            target_disk_path])

    def _wait_for_task(self, task):
        while task.info.state not in [vim.TaskInfo.State.success,
                                      vim.TaskInfo.State.error]:
            time.sleep(.1)
        if task.info.state == vim.TaskInfo.State.error:
            raise exception.CoriolisException(task.info.error.msg)

    @staticmethod
    def _keep_alive_vmware_conn(si, exit_event):
        try:
            while True:
                LOG.debug("VMware connection keep alive")
                si.CurrentTime()
                if exit_event.wait(60):
                    return
        finally:
            LOG.debug("Exiting VMware connection keep alive thread")

    @utils.retry_on_error()
    @contextlib.contextmanager
    def _connect(self, connection_info):
        host = connection_info["host"]
        port = connection_info.get("port", 443)
        username = connection_info["username"]
        password = connection_info["password"]
        allow_untrusted = connection_info.get("allow_untrusted", False)

        # pyVmomi locks otherwise
        sys.modules['socket'] = eventlet.patcher.original('socket')
        ssl = eventlet.patcher.original('ssl')

        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        if allow_untrusted:
            context.verify_mode = ssl.CERT_NONE

        LOG.info("Connecting to: %s:%s" % (host, port))
        si = connect.SmartConnect(
            host=host,
            user=username,
            pwd=password,
            port=port,
            sslContext=context)

        thread = None
        try:
            thread_exit_event = threading.Event()
            thread = threading.Thread(
                target=self._keep_alive_vmware_conn,
                args=(si, thread_exit_event))
            thread.start()

            yield context, si
        finally:
            connect.Disconnect(si)
            if thread:
                thread_exit_event.set()
                thread.join()

    def _wait_for_vm_status(self, vm, status, max_wait=120):
        i = 0
        while i < max_wait and vm.runtime.powerState != status:
            time.sleep(1)
            i += 1
        return i < max_wait

    def _get_vm(self, si, instance_path):
        vm = None
        container = si.content.rootFolder
        path_items = [p.replace('\\/', '/') for p in
                      re.split(r'(?<!\\)/', instance_path)]
        if len(path_items) == 1:
            if len(container.childEntity) > 1:
                raise exception.InvalidInput(
                    "There's more than one container in the VMWare root "
                    "folder, please specify the full path for the VM, e.g. "
                    "\"Datacenter1/VM1\"")
            else:
                container = container.childEntity[0].vmFolder

        LOG.debug("VM path items: %s", path_items)
        for i, path_item in enumerate(path_items):
            l = [o for o in container.childEntity if o.name == path_item]
            if not l:
                raise exception.InstanceNotFound(instance_name=instance_path)
            item = l[0]
            if (i + 1 == len(path_items) and
                    isinstance(item, vim.VirtualMachine)):
                vm = item
            elif isinstance(item, vim.Datacenter):
                container = item.vmFolder
            else:
                container = item

        if vm is None:
            raise exception.InstanceNotFound(instance_name=instance_path)

        return vm

    @utils.retry_on_error()
    def _shutdown_vm(self, vm):
        if vm.runtime.powerState != vim.VirtualMachinePowerState.poweredOff:
            power_off = True
            if (vm.guest.toolsRunningStatus !=
                    vim.vm.GuestInfo.ToolsRunningStatus.guestToolsNotRunning):
                self._event_manager.progress_update("Shutting down guest OS")
                vm.ShutdownGuest()
                if self._wait_for_vm_status(
                        vm, vim.VirtualMachinePowerState.poweredOff):
                    power_off = False

            if power_off:
                self._event_manager.progress_update(
                    "Powering off the virtual machine")
                task = vm.PowerOff()
                self._wait_for_task(task)

    @utils.retry_on_error()
    def _get_vm_info(self, si, instance_path):

        LOG.info("Retrieving data for VM: %s" % instance_path)
        vm = self._get_vm(si, instance_path)

        firmware_type_map = {
            vim.vm.GuestOsDescriptor.FirmwareType.bios:
                constants.FIRMWARE_TYPE_BIOS,
            vim.vm.GuestOsDescriptor.FirmwareType.efi:
                constants.FIRMWARE_TYPE_EFI
        }

        vm_info = {
            'num_cpu': vm.config.hardware.numCPU,
            'num_cores_per_socket': vm.config.hardware.numCoresPerSocket,
            'memory_mb': vm.config.hardware.memoryMB,
            'firmware_type': firmware_type_map[vm.config.firmware],
            'nested_virtualization': vm.config.nestedHVEnabled,
            'dynamic_memory_enabled':
                not vm.config.memoryReservationLockedToMax,
            'name': vm.config.name,
            'guest_id': vm.config.guestId,
            'os_type': guestid.GUEST_ID_OS_TYPE_MAP.get(vm.config.guestId),
            'id': vm._moId,
        }

        LOG.info("vm info: %s" % str(vm_info))

        disk_ctrls = []
        devices = [d for d in vm.config.hardware.device if
                   isinstance(d, vim.vm.device.VirtualController)]
        for device in devices:
            ctrl_type = None
            if isinstance(device, vim.vm.device.VirtualPCIController):
                ctrl_type = "PCI"
            elif isinstance(device, vim.vm.device.VirtualSIOController):
                ctrl_type = "SIO"
            elif isinstance(device, vim.vm.device.VirtualIDEController):
                ctrl_type = "IDE"
            elif isinstance(device, vim.vm.device.VirtualSATAController):
                ctrl_type = "SATA"
            elif isinstance(device, vim.vm.device.VirtualSCSIController):
                ctrl_type = "SCSI"
            else:
                continue
            disk_ctrls.append({'id': device.key, 'type': ctrl_type,
                               'bus_number': device.busNumber})

        disks = []
        devices = [d for d in vm.config.hardware.device if
                   isinstance(d, vim.vm.device.VirtualDisk)]
        for device in devices:
            disks.append({'size_bytes': device.capacityInBytes,
                          'unit_number': device.unitNumber,
                          'id': device.key,
                          'controller_id': device.controllerKey,
                          'path': device.backing.fileName,
                          'format': constants.DISK_FORMAT_VMDK})

        cdroms = []
        devices = [d for d in vm.config.hardware.device if
                   isinstance(d, vim.vm.device.VirtualCdrom)]
        for device in devices:
            cdroms.append({'unit_number': device.unitNumber, 'id': device.key,
                           'controller_id': device.controllerKey})

        floppies = []
        devices = [d for d in vm.config.hardware.device if
                   isinstance(d, vim.vm.device.VirtualFloppy)]
        for device in devices:
            floppies.append({'unit_number': device.unitNumber,
                             'id': device.key,
                             'controller_id': device.controllerKey})

        nics = []
        devices = [d for d in vm.config.hardware.device if
                   isinstance(d, vim.vm.device.VirtualEthernetCard)]
        for device in devices:
            nics.append({'mac_address': device.macAddress, 'id': device.key,
                         'name': device.deviceInfo.label,
                         'network_name': device.backing.network.name})

        serial_ports = []
        devices = [d for d in vm.config.hardware.device if
                   isinstance(d, vim.vm.device.VirtualSerialPort)]
        for device in devices:
            serial_ports.append({'id': device.key})

        boot_order = []
        for boot_device in vm.config.bootOptions.bootOrder:
            if isinstance(boot_device, vim.vm.BootOptions.BootableDiskDevice):
                boot_order.append({"type": "disk",
                                   "id": boot_device.deviceKey})
            elif isinstance(boot_device,
                            vim.vm.BootOptions.BootableCdromDevice):
                boot_order.append({"type": "cdrom", "id": None})
            elif isinstance(boot_device,
                            vim.vm.BootOptions.BootableEthernetDevice):
                boot_order.append({"type": "ethernet",
                                   "id": boot_device.deviceKey})
            elif isinstance(boot_device,
                            vim.vm.BootOptions.BootableFloppyDevice):
                boot_order.append({"type": "floppy", "id": None})

        vm_info["devices"] = {
            "nics": nics,
            "controllers": disk_ctrls,
            "disks": disks,
            "cdroms": cdroms,
            "floppies": floppies,
            "serial_ports": serial_ports
        }
        vm_info["boot_order"] = boot_order

        return vm_info, vm

    @utils.retry_on_error()
    def _export_disks(self, vm, export_path, context):
        disk_paths = []

        self._shutdown_vm(vm)
        lease = vm.ExportVm()
        while True:
            if lease.state == vim.HttpNfcLease.State.ready:
                try:
                    tot_downloaded_bytes = 0
                    for du in [du for du in lease.info.deviceUrl if du.disk]:
                        # Key format: '/vm-70/VirtualLsiLogicController0:0'
                        ctrl_str, address = du.key[
                            du.key.rindex('/') + 1:].split(':')

                        def _get_class_name(obj):
                            return obj.__class__.__name__.split('.')[-1]

                        for i, ctrl in enumerate(
                            [d for d in vm.config.hardware.device if
                             isinstance(
                                d, vim.vm.device.VirtualController) and
                                ctrl_str.startswith(_get_class_name(d))]):
                            if int(ctrl_str[len(_get_class_name(ctrl)):]) == i:
                                disk_key = [
                                    d for d in vm.config.hardware.device if
                                    isinstance(
                                        d, vim.vm.device.VirtualDisk) and
                                    d.controllerKey == ctrl.key and
                                    d.unitNumber == int(address)][0].key
                                break

                        response = request.urlopen(du.url, context=context)
                        path = os.path.join(export_path, du.targetId)
                        disk_paths.append({
                            'path': path,
                            'id': disk_key,
                            'format': constants.DISK_FORMAT_VMDK})

                        LOG.info("Downloading: %s" % path)
                        with open(path, 'wb') as f:
                            while True:
                                chunk = response.read(1024 * 1024)
                                if not chunk:
                                    break
                                tot_downloaded_bytes += len(chunk)
                                f.write(chunk)
                                lease.HttpNfcLeaseProgress(
                                    int(tot_downloaded_bytes * 100 /
                                        (lease.info.totalDiskCapacityInKB *
                                         1024)))

                    lease.HttpNfcLeaseComplete()
                    return disk_paths
                except:
                    lease.HttpNfcLeaseAbort()
                    raise
            elif lease.state == vim.HttpNfcLease.State.error:
                raise exception.CoriolisException(lease.error.msg)
            else:
                time.sleep(.1)

    def _connect_vixdisklib(self, connection_info, context,
                            vmx_spec, snapshot_ref):
        host = connection_info["host"]
        port = connection_info.get("port", 443)
        username = connection_info["username"]
        password = connection_info["password"]

        thumbprint = utils.get_ssl_cert_thumbprint(context, host, port)

        return vixdisklib.connect(
            server_name=host,
            port=port,
            thumbprint=thumbprint,
            username=username,
            password=password,
            vmx_spec=vmx_spec,
            snapshot_ref=snapshot_ref)

    def _take_vm_snapshot(self, vm, snapshot_name, memory=False, quiesce=True):
        task = vm.CreateSnapshot_Task(
            name=snapshot_name, memory=False, quiesce=True)
        self._wait_for_task(task)
        return task.info.result

    def _remove_vm_snapshot(self, snapshot, remove_children=False):
        self._wait_for_task(snapshot.RemoveSnapshot_Task(remove_children))

    @contextlib.contextmanager
    def _take_temp_vm_snapshot(self, vm, snapshot_name, memory=False,
                               quiesce=True):
        self._event_manager.progress_update("Creating snapshot")
        snapshot = self._take_vm_snapshot(vm, snapshot_name, memory, quiesce)
        try:
            yield snapshot
        finally:
            self._event_manager.progress_update("Removing snapshot")
            self._remove_vm_snapshot(snapshot)

    @utils.retry_on_error()
    def _backup_snapshot_disks(self, snapshot, export_path, connection_info,
                               context, disk_paths, backup_writer,
                               incremental):
        vm = snapshot.vm
        vmx_spec = "moref=%s" % vm._GetMoId()
        snapshot_ref = snapshot._GetMoId()
        pos = 0
        sector_size = vixdisklib.VIXDISKLIB_SECTOR_SIZE
        max_sectors_per_read = 40 * 2048

        with self._connect_vixdisklib(connection_info, context,
                                      vmx_spec, snapshot_ref) as conn:
            for disk in [d for d in snapshot.config.hardware.device
                         if isinstance(d, vim.vm.device.VirtualDisk)]:
                change_id = '*'

                l = [d for d in disk_paths if d['id'] == disk.key]
                if l:
                    disk_path = l[0]
                    if incremental:
                        change_id = disk_path["change_id"]
                    path = disk_path["path"]
                else:
                    path = os.path.join(export_path, "disk-%s.raw" % disk.key)
                    disk_path = {
                        'path': path,
                        'id': disk.key,
                        'format': constants.DISK_FORMAT_RAW}
                    disk_paths.append(disk_path)

                LOG.info("CBT change id: %s", change_id)
                changed_disk_areas = vm.QueryChangedDiskAreas(
                    snapshot, disk.key, pos, change_id)

                backup_disk_path = disk.backing.fileName

                disk_size = changed_disk_areas.length
                changed_area_size = sum(
                    [x.length for x in changed_disk_areas.changedArea])

                if not changed_area_size:
                    self._event_manager.progress_update(
                        "No blocks to replicate for disk: %s" %
                        backup_disk_path)
                    continue

                if change_id == '*':
                    self._event_manager.progress_update(
                        "Performing full CBT replica for disk: {path}. "
                        "Disk size: {disk_size:,}. Written blocks size: "
                        "{changed_area_size:,}".format(
                            path=backup_disk_path,
                            disk_size=disk_size,
                            changed_area_size=changed_area_size))
                else:
                    self._event_manager.progress_update(
                        "Performing incremental CBT replica for disk: {path}. "
                        "Disk size: {disk_size:,}. Changed blocks size: "
                        "{changed_area_size:,}".format(
                            path=backup_disk_path,
                            disk_size=disk_size,
                            changed_area_size=changed_area_size))

                with vixdisklib.open(
                        conn, backup_disk_path) as disk_handle:

                    with backup_writer.open(path, disk.key) as f:
                        # Create a sparse file
                        f.truncate(disk.capacityInBytes)

                        total_written_bytes = 0
                        perc_step = self._event_manager.add_percentage_step(
                            changed_area_size,
                            message_format="Disk %s replica progress: "
                            "{:.0f}%%" % backup_disk_path)

                        for area in changed_disk_areas.changedArea:
                            start_sector = area.start // sector_size
                            num_sectors = area.length // sector_size

                            f.seek(area.start)

                            i = 0
                            while i < num_sectors:
                                curr_num_sectors = min(
                                    num_sectors - i, max_sectors_per_read)

                                buf_size = curr_num_sectors * sector_size
                                buf = vixdisklib.get_buffer(buf_size)

                                LOG.debug(
                                    "Read start sector: %s, num sectors: %s" %
                                    (start_sector + i, curr_num_sectors))

                                vixdisklib.read(
                                    disk_handle, start_sector + i,
                                    curr_num_sectors, buf)
                                i += curr_num_sectors

                                f.write(buf.raw)

                                total_written_bytes += buf_size
                                self._event_manager.set_percentage_step(
                                    perc_step, total_written_bytes)

                disk_path['change_id'] = disk.backing.changeId

    def _backup_disks(self, vm, export_path, connection_info, context):
        if not vm.config.changeTrackingEnabled:
            raise exception.CoriolisException("Change Tracking not enabled")

        disk_paths = []

        LOG.info("First backup pass")
        snapshot_name = str(uuid.uuid4())
        with self._take_temp_vm_snapshot(vm, snapshot_name) as snapshot:
            self._backup_snapshot_disks(
                snapshot, export_path, connection_info, context,
                disk_paths, _FileBackupWriter(), incremental=False)

        self._shutdown_vm(vm)

        LOG.info("Second backup pass")
        snapshot_name = str(uuid.uuid4())
        with self._take_temp_vm_snapshot(vm, snapshot_name) as snapshot:
            self._backup_snapshot_disks(
                snapshot, export_path, connection_info, context,
                disk_paths, _FileBackupWriter(), incremental=True)

        return disk_paths

    def export_instance(self, ctxt, connection_info, instance_name,
                        export_path):
        self._event_manager.progress_update("Connecting to vSphere host")
        with self._connect(connection_info) as (context, si):
            self._event_manager.progress_update(
                "Retrieving virtual machine data")
            vm_info, vm = self._get_vm_info(si, instance_name)

            # Take advantage of CBT if available
            backup_disks = vm.config.changeTrackingEnabled

            self._event_manager.progress_update("Exporting disks")
            if backup_disks:
                disk_paths = self._backup_disks(
                    vm, export_path, connection_info, context)
            else:
                disk_paths = self._export_disks(vm, export_path, context)

        if not backup_disks:
            self._event_manager.progress_update(
                "Converting virtual disks format")
            for disk_path in disk_paths:
                path = disk_path["path"]
                LOG.info("Converting VMDK type: %s" % path)
                tmp_path = "%s.tmp" % path
                self._convert_disk_type(path, tmp_path)
                os.remove(path)
                os.rename(tmp_path, path)

        disks = vm_info["devices"]["disks"]

        for disk_path in disk_paths:
            disk_info = [d for d in disks if d["id"] == disk_path["id"]][0]
            disk_info["format"] = disk_path["format"]
            disk_info["path"] = os.path.abspath(disk_path["path"])

        return vm_info

    def get_replica_instance_info(self, ctxt, connection_info, instance_name):
        self._event_manager.progress_update("Connecting to vSphere host")
        with self._connect(connection_info) as (context, si):
            self._event_manager.progress_update(
                "Retrieving virtual machine data")
            vm_info, vm = self._get_vm_info(si, instance_name)

            if not vm.config.changeTrackingEnabled:
                raise exception.CoriolisException(
                    "Changed Block Tracking must be enabled in order to "
                    "replicate a VM")

        return vm_info

    def shutdown_instance(self, ctxt, connection_info, instance_name):
        self._event_manager.progress_update("Connecting to vSphere host")
        with self._connect(connection_info) as (context, si):
            vm = self._get_vm(si, instance_name)
            self._shutdown_vm(vm)

    def replicate_disks(self, ctxt, connection_info, instance_name,
                        source_conn_info, target_conn_info, volumes_info,
                        incremental):
        ip = target_conn_info["ip"]
        port = target_conn_info.get("port", 22)
        username = target_conn_info["username"]
        pkey = target_conn_info.get("pkey")
        password = target_conn_info.get("password")

        LOG.info("Waiting for connectivity on host: %(ip)s:%(port)s",
                 {"ip": ip, "port": port})
        utils.wait_for_port_connectivity(ip, port)

        with self._connect(connection_info) as (context, si):
            vm = self._get_vm(si, instance_name)

            backup_writer = _SSHBackupWriter(
                ip, port, username, pkey, password, volumes_info)

            snapshot_name = str(uuid.uuid4())
            disk_paths = []
            for volume_info in volumes_info:
                disk_paths.append(
                    {"id": volume_info["disk_id"],
                     "change_id": volume_info.get("change_id", "*"),
                     "path": ""})

            with self._take_temp_vm_snapshot(vm, snapshot_name) as snapshot:
                self._backup_snapshot_disks(
                    snapshot, "", connection_info, context,
                    disk_paths, backup_writer, incremental)

            for volume_info in volumes_info:
                change_id = [d["change_id"] for d in disk_paths
                             if d["id"] == volume_info["disk_id"]][0]
                volume_info["change_id"] = change_id

        return volumes_info

    def deploy_replica_source_resources(self, ctxt, connection_info):
        return {"migr_resources": None, "connection_info": None}

    def delete_replica_source_resources(self, ctxt, connection_info,
                                        migr_resources_dict):
        pass
