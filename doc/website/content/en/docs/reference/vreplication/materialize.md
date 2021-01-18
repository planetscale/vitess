---
title: Materialize
description: 
weight: 70
---

### Command

```
Materialize <json_spec>
```

### Description

Materialize is a low level vreplication API that allows for generalized materialization of tables. The target tables
can be copies, aggregations or views. The target tables are kept in sync in near-realtime.

You can specify multiple tables to materialize using the json_spec parameter.
  
### Parameters

#### JSON spec details
<div class="cmd">

* *workflow* name to refer to this materialization
* *source_keyspace* keyspace containing the source table
* *target_keyspace* keyspace to materialize to
* *table_settings* list of views to be materialized and the associated query
  * *target_table* name of table to which to materialize the data to
  * *source_expression* the materialization query
  
</div>

#### Example
```
Materialize '{"workflow": "product_sales", "source_keyspace": "commerce", "target_keyspace": "customer", 
    "table_settings": [{"target_table": "sales_by_sku", 
    "source_expression": "select sku, count(*), sum(price) from corder group by order_id"}]}'
```


### A Materialize Workflow

Once you decide on your materialization requirements, you need to initiate a VReplication workflow as follows:

1. Initiate the migration using Materialize
2. Monitor the workflow using [Workflow](../workflow) or [VExec](../vexec)
3. Start accessing your views once the workflow has started Replicating

### Notes

There are special commands to perform common materialization tasks and you should prefer them
to using Materialize directly.
* If you just want to copy tables to a different keyspace use [MoveTables](../movetables).
* If you want to change sharding strategies use [Reshard](../reshard) instead
