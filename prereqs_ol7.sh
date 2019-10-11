#!/bin/bash
set -e

yum install kernel-uek -y
# grep '^menuentry' /boot/grub2/grub.cfg
sed -i 's/GRUB_DEFAULT=.*/GRUB_DEFAULT=0/g' /etc/default/grub

# Comment the following line if you don't want the eth[0-9]+ ifname naming
# Note: you might need to edit and rename the corresponding
# /etc/sysconfig/network-scripts/ifcfg-* files
echo 'GRUB_CMDLINE_LINUX="net.ifnames=0"' >> /etc/default/grub

grub2-mkconfig -o /boot/grub2/grub.cfg

# Reboot to load the new kernel
# reboot

# on VMware:
# yum install open-vm-tools -y

# on Oracle VM
# yum install ovmd xenstoreprovider libovmapi ovm-template-config* --enablerepo=ol7_addons -y
# systemctl enable ovmd
# systemctl start ovmd

# If you need to run this instance on different hardware / hypervisors:
# dracut -f --no-hostonly

sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/selinux/config
setenforce permissive

curl -s -o /etc/yum.repos.d/public-yum-ol7.repo http://yum.oracle.com/public-yum-ol7.repo
yum install yum-utils -y

curl -o epel-release-latest-7.noarch.rpm http://fedora.mirrors.telekom.ro/pub/epel/epel-release-latest-7.noarch.rpm
rpm -ivh epel-release-latest-7.noarch.rpm
yum-config-manager --disable epel

yum install -y ntp
systemctl enable ntpd
systemctl start ntpd

yum install -y python3 python3-pip

yum install docker-engine --enablerepo=ol7_addons -y
systemctl enable docker
systemctl start docker

# NOTE: IPv4 forwarding is required for rebuilding container images
# and resolving DNS inside the running containers:
echo "net.ipv4.ip_forward=1" > /etc/sysctl.d/99-docker-ipv4-forwarding.conf

yum install git -y
yum groupinstall development tools -y

# Required for generating the SSL cert for the Web proxy:
yum install -y openssl

# Required for our Ansible tasks
pip3 install ansible pymysql virtualenv oslo_config docker

firewall-cmd --permanent --zone=public --add-port=35357/tcp
firewall-cmd --permanent --zone=public --add-port=5000/tcp
firewall-cmd --permanent --zone=public --add-port=9311/tcp
firewall-cmd --permanent --zone=public --add-port=7667/tcp
firewall-cmd --permanent --zone=public --add-port=80/tcp
firewall-cmd --permanent --zone=public --add-port=443/tcp
firewall-cmd --reload

# Optional, in case a Cisco VPN is needed
# yum install vpnc --enablerepo epel -y
