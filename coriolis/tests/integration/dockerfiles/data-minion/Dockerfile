# Copyright 2026 Cloudbase Solutions Srl
# All Rights Reserved.

FROM ubuntu:24.04

# dbus is required for systemd to fully manage units;
# sudo is used by replicator / writer setup.
RUN apt-get update && apt-get install -y --no-install-recommends \
    dbus \
    openssh-server \
    sudo \
    systemd \
    && rm -rf /var/lib/apt/lists/*

RUN systemctl enable ssh

RUN sed -i \
        -e 's/^#\?PermitRootLogin.*/PermitRootLogin yes/' \
        -e 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' \
        -e 's/^#\?AuthorizedKeysFile.*/AuthorizedKeysFile .ssh\/authorized_keys/' \
        /etc/ssh/sshd_config && \
    echo 'StrictModes no' >> /etc/ssh/sshd_config

# systemd requires these folders.
VOLUME ["/run", "/run/lock"]

CMD ["/lib/systemd/systemd"]
