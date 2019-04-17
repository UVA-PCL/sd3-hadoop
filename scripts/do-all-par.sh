#!/bin/bash

source $(dirname $0)/config.sh
for HOST in "${EXPERIMENT_HOSTS[@]}"; do
    ssh $HOST "nohup $SCRIPT_DIR/${@} > $SCRIPT_DIR/$HOST.log" &
done
