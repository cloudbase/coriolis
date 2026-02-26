#!/usr/bin/env python3

import os
import sys
import argparse
import importlib.util
from packaging.version import Version
import subprocess

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
CORIOLIS_ANSIBLE_BIN = os.path.join(BASE_DIR, "coriolis-ansible")


def load_module_from_path(path):
    module_name = os.path.splitext(os.path.basename(path))[0]
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def discover_upgrade_modules(upgrade_dir):
    modules = []

    if not os.path.exists(upgrade_dir):
        return []

    for file in os.listdir(upgrade_dir):
        if file.endswith(".py") and file != "__init__.py":
            full_path = os.path.join(upgrade_dir, file)
            module = load_module_from_path(full_path)

            if not hasattr(module, "VERSION"):
                raise RuntimeError(f"{file} missing VERSION attribute")

            modules.append(module)

    modules.sort(key=lambda m: Version(m.VERSION))
    return modules


def run_upgrades(upgrade_dir, current_version, new_version):
    current_v = Version(current_version)
    new_v = Version(new_version)

    executed = []

    print("\n=== Running Pre-Deploy Scripts ===")

    for module in discover_upgrade_modules(upgrade_dir):
        version = Version(module.VERSION)

        if current_v < version <= new_v:
            print(f"Executing upgrade {module.VERSION}")

            if not hasattr(module, "pre_deploy"):
                raise RuntimeError(f"{module.VERSION} missing pre_deploy()")

            module.pre_deploy(BASE_DIR)
            executed.append(module)

    print("=== Pre-Deploy Complete ===\n")
    return executed


def revert_upgrades(executed_modules):
    print("\n=== Reverting Executed Upgrades ===")

    for module in reversed(executed_modules):
        if hasattr(module, "revert"):
            print(f"Reverting {module.VERSION}")
            module.revert()

    print("=== Revert Complete ===\n")


def deploy():
    subprocess.check_call([CORIOLIS_ANSIBLE_BIN, "bootstrap"])
    subprocess.check_call([CORIOLIS_ANSIBLE_BIN, "deploy"])
    print("Deployment successful.")


def main():
    parser = argparse.ArgumentParser(
        description="Versioned Pre-Deploy Upgrade Runner")
    parser.add_argument("-u", required=True, help="Upgrade scripts directory")
    parser.add_argument(
        "-c", required=True, help="Current deployed release version")
    parser.add_argument(
        "-n", required=True, help="New target release version")

    args = parser.parse_args()

    executed = []

    try:
        executed = run_upgrades(args.u, args.c, args.n)
        deploy()

    except Exception as e:
        print(f"\nERROR: {e}")
        revert_upgrades(executed)
        sys.exit(1)


if __name__ == "__main__":
    main()
