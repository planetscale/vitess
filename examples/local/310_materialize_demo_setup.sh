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
# this script brings down the tablets for customer/0 keyspace

source env.sh

vtctlclient Workflow customer.cust2cust delete
vtctlclient Workflow commerce.cust2cust_reverse delete

vtctlclient InitShardPrimary -- --force customer/-80 zone1-300
vtctlclient InitShardPrimary -- --force customer/80- zone1-400

vtctlclient ExecuteFetchAsDba zone1-300 "CREATE FUNCTION total(price int, qty int)  RETURNS int  DETERMINISTIC  RETURN price * qty;"
vtctlclient ExecuteFetchAsDba zone1-400 "CREATE FUNCTION total(price int, qty int)  RETURNS int  DETERMINISTIC  RETURN price * qty;"

echo
read -p "**************** Starting Materialize workflow wf_corder to denormalize corder **************** "

vtctlclient Materialize '{"workflow": "wf_corder", "source_keyspace": "customer", "target_keyspace": "customer", "table_settings": [ {"target_table": "corder_facts", "source_expression": "select order_id, customer_id, email, sku, CONCAT(\"xxxx-xxxx-xxxx-\", RIGHT(credit_card,4)) as credit_card, price, qty, total(price, qty) total_price, month(created) as created_month, year(created) as created_year from corder where sku <> \"SKU-1003\" and price >= 2"  }] }'

echo
read -p "**************** Starting Materialize workflow wf_sales to aggregate sales **************** "
vtctlclient Materialize '{"workflow": "wf_sales", "source_keyspace": "customer", "target_keyspace": "commerce", "table_settings": [ {"target_table": "sales", "source_expression": "select sku, count(*) as num_orders,  sum(qty) as  total_qty, sum(total_price) as total_sales from corder_facts group by sku"  }] }'
