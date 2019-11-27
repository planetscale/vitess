#!/bin/bash

source ./env.sh

# Create a new keyspace for Vitess, but serve it from existing setup
./lvtctl.sh CreateKeyspace -served_from='master:legacy,replica:legacy,rdonly:legacy' vitess

# Create 3x tablets for the keyspace
CELL=zone1 KEYSPACE=vitess UID_BASE=200 ./vttablet-up.sh

# Set one of the tablets to master, copy the schema for t1 from legacy
# Update the vschema for legacy (t2) and vitess (t1)
./lvtctl.sh InitShardMaster -force vitess/0 zone1-200
./lvtctl.sh CopySchemaShard -tables t1 legacy/0 vitess/0
./lvtctl.sh ApplyVSchema -vschema_file vschema_legacy_vsplit.json legacy 
./lvtctl.sh ApplyVSchema -vschema_file vschema_vitess_vsplit.json vitess

