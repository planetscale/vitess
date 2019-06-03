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

# This is a convenience script to run the mysql client against the local example.

source kalias.source

$kvtctl VReplicationExec zone1-376976400 'insert into _vt.vreplication (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('"'"'vt_merchant'"'"', '"'"'keyspace:\"customer\" shard:\"-80\" filter:<rules:<match:\"orders\" filter:\"select * from orders where in_keyrange(mname, \'"'"'unicode_loose_md5\'"'"', \'"'"'-80\'"'"')\" > > '"'"', '"'"''"'"', 9999, 9999, '"'"'master'"'"', 0, 0, '"'"'Running'"'"')'
$kvtctl VReplicationExec zone1-376976400 'insert into _vt.vreplication (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('"'"'vt_merchant'"'"', '"'"'keyspace:\"customer\" shard:\"80-\" filter:<rules:<match:\"orders\" filter:\"select * from orders where in_keyrange(mname, \'"'"'unicode_loose_md5\'"'"', \'"'"'-80\'"'"')\" > > '"'"', '"'"''"'"', 9999, 9999, '"'"'master'"'"', 0, 0, '"'"'Running'"'"')'
$kvtctl VReplicationExec zone1-253099400 'insert into _vt.vreplication (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('"'"'vt_merchant'"'"', '"'"'keyspace:\"customer\" shard:\"-80\" filter:<rules:<match:\"orders\" filter:\"select * from orders where in_keyrange(mname, \'"'"'unicode_loose_md5\'"'"', \'"'"'80-\'"'"')\" > > '"'"', '"'"''"'"', 9999, 9999, '"'"'master'"'"', 0, 0, '"'"'Running'"'"')'
$kvtctl VReplicationExec zone1-253099400 'insert into _vt.vreplication (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('"'"'vt_merchant'"'"', '"'"'keyspace:\"customer\" shard:\"80-\" filter:<rules:<match:\"orders\" filter:\"select * from orders where in_keyrange(mname, \'"'"'unicode_loose_md5\'"'"', \'"'"'80-\'"'"')\" > > '"'"', '"'"''"'"', 9999, 9999, '"'"'master'"'"', 0, 0, '"'"'Running'"'"')'
