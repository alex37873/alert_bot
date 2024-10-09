#!/bin/bash

# set -e

sudo cp alertbot.service /etc/systemd/system/

sudo systemctl daemon-reload

sudo systemctl enable alertbot.service

sudo systemctl start alertbot.service

echo "alertbot service installed and started."
