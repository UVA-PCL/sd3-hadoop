#!/bin/bash

source $(dirname $0)/config.sh
for HOST in "${EXPERIMENT_HOSTS[@]}"; do
    ssh $HOST "$SCRIPT_DIR/${@}"
done
