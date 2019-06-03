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
      "hash": {
        "type": "hash"
      }
    },
    "tables": {
      "customer": {
        "column_vindexes": [
          {
            "column": "cid",
            "name": "hash"
          }
        ],
        "auto_increment": {
          "column": "cid",
          "sequence": "customer_seq"
        },
        "columns": [
          {
            "name": "cid"
          },{
            "name": "name",
            "type": "VARCHAR"
          },{
            "name": "balance"
          }
        ],
        "column_list_authoritative": true
      },
      "orders": {
        "column_vindexes": [
          {
            "column": "cid",
            "name": "hash"
          }
        ],
        "auto_increment": {
          "column": "oid",
          "sequence": "order_seq"
        },
        "columns": [
          {
            "name": "oid"
          },{
            "name": "cid"
          },{
            "name": "mname",
            "type": "VARCHAR"
          },{
            "name": "pid"
          },{
            "name": "price"
          }
        ],
        "column_list_authoritative": true
      },
      "product": {
        "type": "reference"
      }
    }
  }' customer
