---
title: Local Install via Docker
description: Instructions for using Vitess on your machine for testing purposes
weight: 1
featured: true
aliases: ['/docs/tutorials/local-docker/']
---

This guide illustrates how to run a local testing Vitess setup via Docker. The Vitess environment is identical to the [local setup](../local/), but without having to install software on one's host other than Docker.

## Check out the vitessio/vitess repository

Clone the GitHub repository via:

- SSH: `git clone git@github.com:vitessio/vitess.git`, or:
- HTTP: `git clone https://github.com/vitessio/vitess.git`

```shell
cd vitess
```

## Build the docker image

In your shell, execute:

```shell
make docker_local
```

This creates a docker image named `vitess/local` (aka `vitess/local:latest`)

## Run the docker image

Execute: 

```shell
./docker/local/run.sh
```

This will set up a MySQL replication topology, as well as `etcd`, `vtctld` and `vtgate` services. 

- `vtgate` listens on [http://127.0.0.1:15001/debug/status](http://127.0.0.1:15001/debug/status) 
- `vtctld` listens on [http://127.0.0.1:15000/debug/status](http://127.0.0.1:15000/debug/status) 
- Control panel is available at [http://localhost:15000/app/](http://localhost:15000/app/)

From within the docker shell, aliases are set up for your convenience. Try the following `mysql` commands to connect to various tablets:

- `mysql commerce`
- `mysql commerce@master`
- `mysql commerce@replica`
- `mysql commerce@rdonly`

You will find that Vitess runs a single keyspace, single shard cluster.


## Summary

In this example, we deployed a single unsharded keyspace named `commerce`. Unsharded keyspaces have a single shard named `0`. The following schema reflects a common ecommerce scenario that was created by the script:

```sql
create table product (
  sku varbinary(128),
  description varbinary(128),
  price bigint,
  primary key(sku)
);
create table customer (
  customer_id bigint not null auto_increment,
  email varbinary(128),
  primary key(customer_id)
);
create table corder (
  order_id bigint not null auto_increment,
  customer_id bigint,
  sku varbinary(128),
  price bigint,
  primary key(order_id)
);
```

The schema has been simplified to include only those fields that are significant to the example:

* The `product` table contains the product information for all of the products.
* The `customer` table has a `customer_id` that has an `auto_increment`. A typical customer table would have a lot more columns, and sometimes additional detail tables.
* The `corder` table (named so because `order` is an SQL reserved word) has an `order_id` auto-increment column. It also has foreign keys into `customer(customer_id)` and `product(sku)`.

## Next Steps

You can now proceed with [MoveTables](../../user-guides/migration/move-tables).

Exiting the docker shell terminates and destroys the vitess cluster.

