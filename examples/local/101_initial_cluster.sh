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

# Initialize topo server and then start
CELL=zone1 ./etcd.sh start

# start vtctld
CELL=zone1 ./vtctld.sh start

# start vttablets for keyspace commerce
# TODO: should we split this into init and start, and run init in parallel?
# TODO: Split into mysqlctld and vttablet?

for uid in 100 101 102; do
 CELL=zone1 KEYSPACE=commerce uid=$uid ./mysqlctl.sh start
 CELL=zone1 KEYSPACE=commerce uid=$uid ./vttablet.sh start
done

echo "Tablets are up!";

# Todo: wait for tablets to be listening


# set one of the replicas to master
./lvtctl.sh InitShardMaster -force commerce/0 zone1-100

# create the schema
./lvtctl.sh ApplySchema -sql-file create_commerce_schema.sql commerce

# create the vschema
./lvtctl.sh ApplyVSchema -vschema_file vschema_commerce_initial.json commerce

# start vtgate
CELL=zone1 ./vtgate.sh start
