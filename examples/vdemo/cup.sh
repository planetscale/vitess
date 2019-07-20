#!/bin/bash

# Copyright 2018 The Vitess Authors.
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

# This is an example script that creates a single shard vttablet deployment.

set -e

source kalias.source

# start topo server
if [ "${TOPO}" = "zk2" ]; then
    CELL=test ./zk-up.sh
else
    CELL=test ./etcd-up.sh
fi

./vtctld-up.sh

SHARD=0 UID_BASE=100 KEYSPACE=product ./vttablet-up.sh "$@" &
SHARD=-80 UID_BASE=200 KEYSPACE=customer ./vttablet-up.sh "$@" &
SHARD=80- UID_BASE=300 KEYSPACE=customer ./vttablet-up.sh "$@" &
SHARD=-80 UID_BASE=400 KEYSPACE=merchant ./vttablet-up.sh "$@" &
SHARD=80- UID_BASE=500 KEYSPACE=merchant ./vttablet-up.sh "$@" &
wait

sleep 10s
$kvtctl InitShardMaster -force product/0 test-100 &
$kvtctl InitShardMaster -force customer/-80 test-200 &
$kvtctl InitShardMaster -force customer/80- test-300 &
$kvtctl InitShardMaster -force merchant/-80 test-400 &
$kvtctl InitShardMaster -force merchant/80- test-500 &
wait

$kvtctl ApplySchema -sql "$(cat product.sql)" product
$kvtctl ApplyVSchema -vschema "$(cat product.json)" product
$kvtctl ApplySchema -sql "$(cat customer.sql)" customer
$kvtctl ApplyVSchema -vschema "$(cat customer.json)" customer
$kvtctl ApplySchema -sql "$(cat merchant.sql)" merchant
$kvtctl ApplyVSchema -vschema "$(cat merchant.json)" merchant

./vtgate-up.sh
