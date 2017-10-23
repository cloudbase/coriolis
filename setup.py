import os
import platform
import setuptools
import subprocess


def _compile_and_install():
    if platform.system() != "Linux":
        return
    dirname = os.path.dirname(__file__)
    resources = os.path.join(dirname, "coriolis", "resources")
    if os.path.isdir(resources):
        subprocess.check_call(
            ["make"], cwd=resources, shell=True)


_compile_and_install()
setuptools.setup(
    setup_requires=['pbr>=1.8'],
    pbr=True)
