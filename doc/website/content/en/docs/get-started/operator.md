---
title: Vitess Operator for Kubernetes
weight: 3
featured: true
aliases: ['/docs/tutorials/kubernetes/','/user-guide/sharding-kubernetes.html', '/docs/get-started/scaleway/','/docs/get-started/kubernetes/']
---

PlanetScale provides a [Vitess Operator for Kubernetes](https://github.com/planetscale/vitess-operator), released under the Apache 2.0 license. The following steps show how to get started using Minikube:

## Prerequisites

Before we get started, let’s get a few things out of the way:

1. Install [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/) and start a Minikube engine. We recommend using Kubernetes 1.14, as this is a common denominator across public clouds:
    ```bash
    minikube start --kubernetes-version=v1.14.10 --cpus=8 --memory=11000 --disk-size=50g
    ```

    If you do not have a machine with 11GB of memory to spare, you could also consider using GKE instead. An equivalent setup can be deployed from the Cloud Shell with:
    ```bash
    gcloud container clusters create vitess --cluster-version 1.14 --zone us-east1-b --num-nodes 5
    ```

2. Install [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) and ensure it is in your `PATH`. For example, on Linux:

    ```bash
    curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.14.9/bin/linux/amd64/kubectl
    ```

1. Install the MySQL client locally. For example, on Ubuntu:

    ```bash
    apt install mysql-client
    ```

1. Install vtctlclient locally:

    If you are familiar with Go development, the easiest way to do this is:
    ```bash
    go get vitess.io/vitess/go/cmd/vtctlclient
    ```
    If not, you can also [download the latest Vitess release](https://github.com/vitessio/vitess/releases) and extract `vtctlclient` from it.

## Install the Operator

Change to the operator example directory:

```bash
git clone git@github.com:vitessio/vitess.git
cd vitess/examples/operator
```

Install the operator:

```bash
kubectl apply -f operator.yaml
```

## Bring up an initial cluster

In this directory, you will see a group of yaml files. The first digit of each file name indicates the phase of example. The next two digits indicate the order in which to execute them. For example, `101_initial_cluster.yaml` is the first file of the first phase. We shall execute that now:

```bash
kubectl apply -f 101_initial_cluster.yaml
```

### Verify cluster

You can check the state of your cluster with `kubectl get pods`. After a few minutes, it should show that all pods are in the status of running:

```bash
$ kubectl get pods
NAME                                             READY   STATUS    RESTARTS   AGE
example-etcd-faf13de3-1                          1/1     Running   0          78s
example-etcd-faf13de3-2                          1/1     Running   0          78s
example-etcd-faf13de3-3                          1/1     Running   0          78s
example-vttablet-zone1-2469782763-bfadd780       3/3     Running   1          78s
example-vttablet-zone1-2548885007-46a852d0       3/3     Running   1          78s
example-zone1-vtctld-1d4dcad0-59d8498459-kwz6b   1/1     Running   2          78s
example-zone1-vtgate-bc6cde92-6bd99c6888-vwcj5   1/1     Running   2          78s
vitess-operator-8454d86687-4wfnc                 1/1     Running   0          2m29s
```

## Setup Port-forward

{{< warning >}}
The port-forward will only forward to a specific pod. Currently, `kubectl` does not automatically terminate a port-forward as the pod dissapears due to apply/upgrade operations. You will need to manually restart the port-forward.
{{</ warning >}}

For ease-of-use, Vitess provides a script to port-forward from Kubernetes to your local machine. This script also recommends setting up aliases for `mysql` and `vtctlclient`:

```bash
./pf.sh &
alias vtctlclient="vtctlclient -server=localhost:15999"
alias mysql="mysql -h 127.0.0.1 -P 15306 -u user"
```

Setting up aliases changes `mysql` to always connect to Vitess for your current session. To revert this, type `unalias mysql && unalias vtctlclient` or close your session.

## Create Schema

Load our initial schema:

```bash
vtctlclient ApplySchema -sql="$(cat create_commerce_schema.sql)" commerce
vtctlclient ApplyVSchema -vschema="$(cat vschema_commerce_initial.json)" commerce
```

### Connect to your cluster

You should now be able to connect to the VTGate Server in your cluster with the MySQL client:

```text
~/vitess/examples/operator$ mysql
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 3
Server version: 5.7.9-Vitess MySQL Community Server (GPL)

Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show databases;
+-----------+
| Databases |
+-----------+
| commerce  |
+-----------+
1 row in set (0.00 sec)
```

### Summary

In this example, we deployed a single unsharded keyspace named `commerce`. Unsharded keyspaces have a single shard named `0`. The following schema reflects a common ecommerce scenario that was created by the script:

``` sql
create table product(
  sku varbinary(128),
  description varbinary(128),
  price bigint,
  primary key(sku)
);
create table customer(
  customer_id bigint not null auto_increment,
  email varbinary(128),
  primary key(customer_id)
);
create table corder(
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

Or alternatively, if you would like to teardown your example:

```sh
kubectl delete -f 101_initial_cluster.yaml
```
Congratulations on completing this exercise!
