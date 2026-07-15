#!/usr/bin/env python3

import argparse
import sys

from packaging.version import InvalidVersion
from packaging.version import Version


def main():
    parser = argparse.ArgumentParser(
        description="Validates that a given version is a valid patch "
                     "version for the currently installed Coriolis "
                     "release, i.e. that it only bumps the patch/micro "
                     "component of the currently installed release.")
    parser.add_argument(
        "-c", "--current", required=True,
        help="Currently installed Coriolis release version.")
    parser.add_argument(
        "-n", "--new", required=True,
        help="Candidate patch version.")

    args = parser.parse_args()

    try:
        current_version = Version(args.current)
    except InvalidVersion:
        print(f"ERROR: The currently installed release version "
              f"'{args.current}' (read from /etc/coriolis/coriolis.release) "
              f"is not a valid version.")
        sys.exit(1)

    try:
        new_version = Version(args.new)
    except InvalidVersion:
        print(f"ERROR: '{args.new}' is not a valid version.")
        sys.exit(1)

    if new_version <= current_version:
        print(f"ERROR: Patch version '{args.new}' must be greater than the "
              f"currently installed release '{args.current}'.")
        sys.exit(1)

    current_base = current_version.release[:2]
    new_base = new_version.release[:2]
    if new_base != current_base:
        base_str = ".".join(str(part) for part in current_base)
        print(f"ERROR: '{args.new}' is not a patch version of the "
              f"currently installed release '{args.current}'. A patch "
              f"version may only change the patch/micro component of the "
              f"release, i.e. it must be based on '{base_str}'.")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
