import abc


class BaseOSMorphingTools(object):
    def __init__(self, os_root_dir):
        self._os_root_dir = os_root_dir

    @staticmethod
    def check_os(ssh, os_root_dir):
        raise NotImplementedError()

    @abc.abstractmethod
    def set_dhcp(self, ssh):
        pass

    def get_packages(self, hypervisor, platform):
        pass

    def update_packages_list(self, ssh):
        pass

    def install_packages(self, ssh, package_names):
        pass

    def uninstall_packages(self, ssh, package_names):
        pass
