#!/usr/bin/env python3

import json
import os

DAEMON_JSON_PATH = "/etc/docker/daemon.json"

LOG_CONFIG = {
    "log-driver": "json-file",
    "log-opts": {
        "max-size": "50m",
        "max-file": "3"
    }
}


def load_existing_config(path):
    if not os.path.exists(path):
        return {}

    with open(path, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            raise RuntimeError(f"{path} exists but contains invalid JSON")


def merge_config(existing, new):
    merged = existing.copy()

    for key, value in new.items():
        if isinstance(value, dict):
            merged.setdefault(key, {}).update(value)
        else:
            merged[key] = value

    return merged


def write_config(path, config):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(config, f, indent=2, sort_keys=True)
        f.write("\n")


def main():
    existing_config = load_existing_config(DAEMON_JSON_PATH)
    final_config = merge_config(existing_config, LOG_CONFIG)
    write_config(DAEMON_JSON_PATH, final_config)


if __name__ == "__main__":
    main()
