## K3S Cluster Setup Guide

### K3S Server Setup
1. Find the `setup-k3s-server.sh` script in this directory.
2. Give this script execution permission: `sudo chmod +x ./setup-k3s-server.sh`
3. Run this script and take note of the K3S_TOKEN. This will be required for agent setup.
4. Run `source ~/.bashrc` or open a new shell to access kubectl commands without sudo.

### K3S Agent Setup
