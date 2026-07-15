#!/usr/bin/env python3

import argparse
import subprocess
import sys

# Docker images which share the 'coriolis-<name>' naming convention, but
# are not individually-versioned/patchable Coriolis components (either a
# base image only used at build time, or a support/utility container).
EXCLUDED_COMPONENTS = frozenset(("common", "console-editor"))

# Well-known acronyms which should stay upper-case in display names,
# rather than being title-cased (e.g. 'Api' -> 'API').
ACRONYM_OVERRIDES = {"api": "API", "ui": "UI"}


def display_name(key):
    words = key.replace("-", " ").replace("_", " ").split()
    return " ".join(
        ACRONYM_OVERRIDES.get(word.lower(), word.title()) for word in words)


def discover_component_images(registry, namespace):
    prefix = f"{registry}/{namespace}/coriolis-"

    try:
        output = subprocess.check_output(
            ["docker", "images", "--format", "{{.Repository}}"], text=True)
    except FileNotFoundError:
        print("ERROR: The 'docker' command is not available.",
              file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as ex:
        print(f"ERROR: Failed to list Docker images: {ex}", file=sys.stderr)
        sys.exit(1)

    components = {}
    for repository in output.splitlines():
        repository = repository.strip()
        if not repository.startswith(prefix):
            continue

        image_name = repository[len(f"{registry}/{namespace}/"):]
        key = image_name[len("coriolis-"):]
        if not key or key in EXCLUDED_COMPONENTS:
            continue

        components[key] = image_name

    return components


def main():
    parser = argparse.ArgumentParser(
        description="Lists the Coriolis components which can be "
                     "individually patched, discovered from the Docker "
                     "images already pulled on this appliance for the "
                     "'coriolis-<component>' repositories under the "
                     "configured Docker registry/namespace. Prints one "
                     "tab-separated line per component, sorted by name: "
                     "<key>\\t<display_name>\\t<ansible_role_tag>\\t"
                     "<docker_image_tag_var>\\t<docker_image_name>")
    parser.add_argument(
        "-r", "--registry", required=True, help="Docker registry.")
    parser.add_argument(
        "-n", "--namespace", required=True, help="Docker namespace.")

    args = parser.parse_args()

    components = discover_component_images(args.registry, args.namespace)

    for key in sorted(components):
        image_name = components[key]
        role_tag = key
        image_tag_var = f"coriolis_{key.replace('-', '_')}_docker_image_tag"
        print(f"{key}\t{display_name(key)}\t{role_tag}\t{image_tag_var}\t"
              f"{image_name}")


if __name__ == "__main__":
    main()
