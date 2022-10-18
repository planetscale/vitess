#!/bin/bash

# Copyright 2020 The Vitess Authors.
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

# This test runs through the scripts in examples/local to make sure they work.
# It should be kept in sync with the steps in https://vitess.io/docs/get-started/local/
# So we can detect if a regression affecting a tutorial is introduced.


read -p "**************** Setting up initial cluster with commerce keyspace **************** "
source build.env

#set -xe

cd "$VTROOT/examples/local"
unset VTROOT # ensure that the examples can run without VTROOT now.

source ./env.sh # Required so that "mysql" works from alias

./101_initial_cluster.sh

while true; do
 mysql --binary-as-hex=false -e 'show keyspaces' &>/dev/null && break
 sleep 1
done;

mysql --binary-as-hex=false < ../common/insert_commerce_data.sql
mysql --table --binary-as-hex=false < ../common/select_commerce_data.sql

echo
read -p "**************** Setting up customer keyspace **************** "

./201_customer_tablets.sh

for shard in "customer/0"; do
 while true; do
  mysql "$shard" --binary-as-hex=false -e 'show tables' &>/dev/null && break
  sleep 1
 done;
done;

echo
read -p "**************** Running MoveTables to move customer and corder tables from product keyspace to customer keyspace **************** "

./202_move_tables.sh

while true; do
 vtctlclient workflow customer.commerce2customer show | grep -q '"State": "Running"' && break
 sleep 1
done;

echo
read -p "**************** Switching read and write traffic to customer keyspace **************** "

./203_switch_reads.sh

./204_switch_writes.sh

mysql --binary-as-hex=false --table < ../common/select_customer0_data.sql
# Expected to fail!
mysql --binary-as-hex=false --table < ../common/select_commerce_data.sql &>/dev/null || echo "DenyList working, as expected"
./205_clean_commerce.sh
# Expected to fail!
mysql --binary-as-hex=false --table < ../common/select_commerce_data.sql &>/dev/null || echo "Tables missing, as expected"

echo
read -p "**************** Setting up sharded customer keyspace **************** "

./301_customer_sharded.sh
./302_new_shards.sh

for shard in "customer/-80" "customer/80-"; do
 while true; do
  mysql "$shard" --binary-as-hex=false -e 'show tables' &>/dev/null && break
  sleep 1
 done;
done;

echo
read -p "**************** Resharding from unsharded to two shards -80/80- **************** "

./303_reshard.sh

while true; do
 vtctlclient workflow customer.cust2cust show | grep -q '"State": "Running"' && break
 sleep 1
done;

echo
read -p "**************** Switching read and write traffic to sharded customer keyspace **************** "

./304_switch_reads.sh
./305_switch_writes.sh

mysql --binary-as-hex=false --table < ../common/select_customer-80_data.sql
mysql --binary-as-hex=false --table < ../common/select_customer80-_data.sql

./310_materialize_demo_setup.sh

#./306_down_shard_0.sh

#./401_teardown.sh

