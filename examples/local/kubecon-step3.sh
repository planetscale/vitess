#!/bin/bash

source ./env.sh

# Migrate data

vtworker \
    $TOPOLOGY_FLAGS \
    -cell zone1 \
    -log_dir "$VTDATAROOT"/tmp \
    -alsologtostderr \
    -use_v3_resharding_mode \
    VerticalSplitClone -min_healthy_tablets=1 -tables=t1 vitess/0

# Migrate where traffic is served to new location

./lvtctl.sh MigrateServedFrom vitess/0 rdonly 
./lvtctl.sh MigrateServedFrom vitess/0 replica
./lvtctl.sh MigrateServedFrom vitess/0 master

# Drop tables from legacy

./lvtctl.sh ApplySchema -sql "drop table t1"  legacy 
./lvtctl.sh SetShardTabletControl -blacklisted_tables=t1 -remove legacy/0 rdonly
./lvtctl.sh SetShardTabletControl -blacklisted_tables=t1 -remove legacy/0 replica
./lvtctl.sh SetShardTabletControl -blacklisted_tables=t1 -remove legacy/0 master


