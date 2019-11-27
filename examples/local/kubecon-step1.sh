#!/bin/bash
set -xe

source ./env.sh

CELL=zone1 ./etcd-up.sh
CELL=zone1 ./vtctld-up.sh
sleep 10

# Step a Tablet for the external MySQL master

vttablet \
 $TOPOLOGY_FLAGS \
 -tablet-path 'zone1-100' \
 -init_keyspace 'legacy' \
 -init_shard 0 \
 -init_tablet_type 'replica' \
 -port 15100 \
 -grpc_port 16100 \
 -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 -db_host 127.0.0.1 \
 -db_port 19327 \
 -db_app_user msandbox \
 -db_app_password msandbox &

# External MySQL replica

vttablet \
 $TOPOLOGY_FLAGS \
 -tablet-path 'zone1-101' \
 -init_keyspace 'legacy' \
 -init_shard 0 \
 -init_tablet_type 'replica' \
 -port 15101 \
 -grpc_port 16101 \
 -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 -db_host 127.0.0.1 \
 -db_port 19328 \
 -db_app_user msandbox \
 -db_app_password msandbox &

# External MySQL rdonly

vttablet \
 $TOPOLOGY_FLAGS \
 -tablet-path 'zone1-102' \
 -init_keyspace 'legacy' \
 -init_shard 0 \
 -init_tablet_type 'rdonly' \
 -port 15102 \
 -grpc_port 16102 \
 -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
 -db_host 127.0.0.1 \
 -db_port 19329 \
 -db_app_user msandbox \
 -db_app_password msandbox &

sleep 20

# Make the master a master
./lvtctl.sh TabletExternallyReparented zone1-100

# Create an initial empty vschema 
./lvtctl.sh ApplyVSchema -vschema_file vschema_legacy_initial.json legacy

# start vtgate
CELL=zone1 ./vtgate-up.sh

