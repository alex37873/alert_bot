#!/bin/bash

# set -e

sudo systemctl stop alertbot.service

sudo systemctl disable alertbot.service

sudo rm /etc/systemd/system/alertbot.service

sudo systemctl daemon-reload

sudo systemctl reset-failed

# sudo rm -rf ~/bot/logs

echo "alertbot service uninstalled."
