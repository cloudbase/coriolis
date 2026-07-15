#!/usr/bin/env python3

import argparse
import sys

from packaging.version import InvalidVersion
from packaging.version import Version


def main():
    parser = argparse.ArgumentParser(
        description="Compares two Coriolis release version strings. "
                     "Exits with 0 if the new version is strictly greater "
                     "than the current one, or with 1 otherwise. "
                     "This avoids treating version strings as floats "
                     "(e.g. '2607.10' being incorrectly considered smaller "
                     "than '2607.9').")
    parser.add_argument(
        "-c", "--current", required=True, help="Current version.")
    parser.add_argument(
        "-n", "--new", required=True, help="New version.")

    args = parser.parse_args()

    try:
        current_version = Version(args.current)
        new_version = Version(args.new)
    except InvalidVersion as ex:
        print(f"ERROR: {ex}")
        sys.exit(2)

    sys.exit(0 if new_version > current_version else 1)


if __name__ == "__main__":
    main()
