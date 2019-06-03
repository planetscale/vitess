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

$kvtctl ApplyVSchema -vschema='
  {
    "sharded": true,
    "vindexes": {
      "md5": {
        "type": "unicode_loose_md5"
      }
    },
    "tables": {
      "merchant": {
        "column_vindexes": [
          {
            "column": "mname",
            "name": "md5"
          }
        ],
        "columns": [
          {
            "name": "mname",
            "type": "VARCHAR"
          },{
            "name": "category"
          }
        ],
        "column_list_authoritative": true
      },
      "orders": {
        "column_vindexes": [
          {
            "column": "mname",
            "name": "md5"
          }
        ]
      }
    }
  }' merchant
$kvtctl ApplyRoutingRules -rules='{"rules":[{"fromTable":"orders","toTables":["customer.orders","merchant.orders"]}, {"fromTable":"product","toTables":["product.product","customer.product"]}]}'
