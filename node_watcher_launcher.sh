#!/bin/bash

NODE_WATCHER_ENVIRONMENT="prod"
export NODE_WATCHER_ENVIRONMENT

SLACK_CHANNEL="<your_slack_channel>"
export SLACK_CHANNEL

SLACK_THREAD_URI_PREFIX="https://<your_organization>.slack.com/archives/"
export SLACK_THREAD_URI_PREFIX

NODE_EXPORER_API_PREFIX="https://node-explorer.andrew.mesh.nycmesh.net/api/"
export NODE_EXPORER_API_PREFIX

BIRD_API_PREFIX="https://api.andrew.mesh.nycmesh.net/api/v1/ospf/history/"
export BIRD_API_PREFIX

echo "put API token: "
read -s NODE_WATCHER_TOKEN
export NODE_WATCHER_TOKEN



# this starts backup loop, then detaches it and exits the shell
# you can find it via `ps aux | grep backup_loop`
nohup python3 node_watcher.py >/dev/null 2>&1 &

echo "OK"
echo
echo "node_watcher.py should now be running, logs: cat node_watcher.log"
echo "to see the process ID: ps aux | grep node_watcher"