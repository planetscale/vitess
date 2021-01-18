---
title: Production Planning
weight: 1
aliases: ['/docs/launching/production-planning/','/docs/launching/', '/docs/user-guides/production-planning']
---

## Provisioning

### Minimum Topology

A highly available Vitess cluster requires the following components:

* 2 VTGate Servers
* A redundant Topology Service (e.g. 3 etcd servers)
* 3 MySQL Servers with semi-sync replication enabled
* 3 VTTablet processes
* A Vtctld process

It is common practice to locate the VTTablet process and MySQL Servers on the same host, and Vitess uses the terminology _tablet_ to refer to both. The topology service in Vitess is pluggable, and you can use an existing etcd, ZooKeeper or Consul cluster to reduce the footprint required to deploy Vitess.

_For development environments, it is possible to deploy with a lower number of these components. See `101_initial_cluster.sh` from the [Run Vitess Locally](../../../get-started/local) guide for an example._

### General Recommendations

Vitess components (excluding the `mysqld` server) tend to be CPU-bound processes. They use disk space for storing their logs, but do not store any intermediate results on disk, and tend not to be disk IO bound. It is recommended to allocate 2-4 CPU cores for each VTGate server, and the same number of cores for VTTablet as with `mysqld`. If you are provisioning for a new workload, we recommend projecting that `mysqld` will require 1 core per 1500 QPS. Workloads with well optimized queries should be able to achieve greater than this.

The memory requirements for VTGate and VTTablet servers will depend on QPS and result set sizes, but a typical rule of thumb is to provision a baseline of 1GB per core.

The impact of network latency can be a factor when migrating from MySQL to Vitess. A simple rule of thumb is to estimate 2ms of round trip latency added to each query. Application code paths that make large numbers of database round-trips in a sequential code path will be most affected.  To compensate, you may have to optimize or parallelize some code paths; or run additional threads or workers, which may result in additional memory requirements.

### Planning Shard Size

Vitess recommends provisioning shard sizes to approximately 250GB. This is not a hard-limit, and is driven primarily by the recovery time should an instance fail. With 250GB a full-recovery from backup is expected within less than 15 minutes. For most workloads this results in shards instances with relatively few CPU cores and lighter memory requirements, which tend to be more economical than running large instance sizes.

### Running Multiple Tablets Per Server

If you are using physical servers, Vitess encourages running multiple tablets (shards) per server. Typically the best way to do this is with Kubernetes, but `mysqlctl` also supports launching and managing multiple tablet servers if required.

Assuming tablets are kept to the recommended size of 250GB, they can start with a baseline CPU requirement of 2-4 cores for `mysqld` plus 2-4 cores for the VTTablet process, but this is obviously very workload-dependent.

### Topology Service Provisioning

By design, Vitess tries to contact the topology service as little as possible, and stores very little data in the topology server. For estimating CPU/memory/disk requirements, you can use the minimum requirements recommended by your preferred Topology Service.

## Production testing

Before running Vitess in production, you should become comfortable with the different administrative operations. We recommend to go through the following scenarios on a non-production system.

Here is a short list of all the basic workflows Vitess supports:

* [Reparenting](../../configuration-advanced/reparenting)
* [Backup/Restore](../../operating-vitess/backup-and-restore)
* [Schema Management](../../../schema-management)
* [Resharding](../../../reference/sharding#resharding) / [Horizontal Sharding Tutorial](../../historical/horizontal-sharding)
* [Upgrading](../../operating-vitess/upgrading-vitess)
