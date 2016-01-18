import socket
import subprocess
import traceback


def exec_process(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out, std_err = p.communicate()
    if p.returncode:
        raise Exception(
            "Command \"%s\" failed with exit code: %s\nstdout: %s\nstd_err: %s"
            % (args, p.returncode, std_out, std_err))
    return std_out


def get_hostname():
    return socket.gethostname()


def get_exception_details():
    return traceback.format_exc()
