#!/bin/bash

# Copyright 2019 The Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# this script brings up zookeeper and all the vitess components
# required for a single shard deployment.

set -e

# shellcheck disable=SC2128
script_root=$(dirname "${BASH_SOURCE}")

if [[ $EUID -eq 0 ]]; then
   echo "This script refuses to be run as root. Please switch to a regular user."
   exit 1
fi

# start topo server
if [ "${TOPO}" = "zk2" ]; then
    CELL=zone1 "$script_root/zk-up.sh"
else
    CELL=zone1 "$script_root/etcd-up.sh"
fi

# start vtctld
CELL=zone1 "$script_root/vtctld-up.sh"

# start vttablets for keyspace commerce
TABLETS_UIDS=0 CELL=zone1 KEYSPACE=lookup UID_BASE=100 "$script_root/vttablet-up.sh"
TABLETS_UIDS=0 SHARD=-40 CELL=zone1 KEYSPACE=main UID_BASE=200 "$script_root/vttablet-up.sh"
TABLETS_UIDS=0 SHARD=40-80 CELL=zone1 KEYSPACE=main UID_BASE=300 "$script_root/vttablet-up.sh"
TABLETS_UIDS=0 SHARD=80-c0 CELL=zone1 KEYSPACE=main UID_BASE=400 "$script_root/vttablet-up.sh"
TABLETS_UIDS=0 SHARD=c0- CELL=zone1 KEYSPACE=main UID_BASE=500 "$script_root/vttablet-up.sh"

# set one of the replicas to master
./lvtctl.sh InitShardMaster -force lookup/0 zone1-100
./lvtctl.sh InitShardMaster -force main/-40 zone1-200
./lvtctl.sh InitShardMaster -force main/40-80 zone1-300
./lvtctl.sh InitShardMaster -force main/80-c0 zone1-400
./lvtctl.sh InitShardMaster -force main/c0- zone1-500

# create the schema
./lvtctl.sh ApplySchema -sql-file create_lookup_schema.sql lookup
./lvtctl.sh ApplySchema -sql-file create_customer_schema.sql main

# create the vschema
./lvtctl.sh ApplyVSchema -vschema_file lookup_vschema.json lookup
./lvtctl.sh ApplyVSchema -vschema_file customer_vschema.json main

# start vtgate
CELL=zone1 "$script_root/vtgate-up.sh"

disown -a
