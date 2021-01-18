---
title: What Is Vitess
weight: 1
featured: true
---

Vitess is a database solution for deploying, scaling and managing large clusters of open-source database instances. It currently supports MySQL and MariaDB. It's architected to run as effectively in a public or private cloud architecture as it does on dedicated hardware. It combines and extends many important SQL features with the scalability of a NoSQL database. Vitess can help you with the following problems:

1. Scaling a SQL database by allowing you to shard it, while keeping application changes to a minimum.
2. Migrating from baremetal to a private or public cloud.
3. Deploying and managing a large number of SQL database instances.

Vitess includes compliant JDBC and Go database drivers using a native query protocol. Additionally, it implements the MySQL server protocol which is compatible with virtually any other language.

Vitess served all YouTube database traffic for over five years. Many enterprises have now adopted Vitess for their production needs.

## Features

* Performance
    - Connection pooling - Multiplex front-end application queries onto a pool of MySQL connections to optimize performance.
    - Query de-duping – Reuse results of an in-flight query for any identical requests received while the in-flight query was still executing.
    - Transaction manager – Limit number of concurrent transactions and manage deadlines to optimize overall throughput.

* Protection
    - Query rewriting and sanitization – Add limits and avoid non-deterministic updates.
    - Query blacklisting – Customize rules to prevent potentially problematic queries from hitting your database.
    - Query killer – Terminate queries that take too long to return data.
    - Table ACLs – Specify access control lists (ACLs) for tables based on the connected user.

* Monitoring
    - Performance analysis tools let you monitor, diagnose, and analyze your database performance.

* Topology Management Tools
    - Master management tools (handles reparenting)
    - Web-based management GUI
    - Designed to work in multiple data centers / regions

* Sharding
    - Virtually seamless dynamic re-sharding
    - Vertical and Horizontal sharding support
    - Multiple sharding schemes, with the ability to plug-in custom ones

## Comparisons to other storage options

The following sections compare Vitess to two common alternatives, a vanilla MySQL implementation and a NoSQL implementation.

### Vitess vs. Vanilla MySQL

Vitess improves a vanilla MySQL implementation in several ways:

| Vanilla MySQL | Vitess |
|:--|:--|
| Every MySQL connection has a memory overhead that ranges between 256KB  and almost 3MB, depending on which MySQL release you're using. As your  user base grows, you need to add RAM to support additional connections,  but the RAM does not contribute to faster queries. In addition, there is  a significant CPU cost associated with obtaining the connections.              | Vitess creates very lightweight connections.  Vitess' connection pooling feature uses Go's concurrency support to map  these lightweight connections to a small pool of MySQL connections. As  such, Vitess can easily handle thousands of connections.     |
| Poorly written queries, such as those that don't set a LIMIT, can negatively impact database performance for all users.                                                                                                                                                                                                                                                           | Vitess employs a SQL parser that uses a configurable set of rules to rewrite queries that might hurt database performance.                                                                                                                                                       |
| Sharding is a process of partitioning your data to improve scalability  and performance. MySQL lacks native sharding support, requiring you to  write sharding code and embed sharding logic in your application.                                                                                                                                                                 | Vitess supports a variety of sharding schemes. It can also migrate  tables into different databases and scale up or down the number of  shards. These functions are performed non-intrusively, completing most  data transitions with just a few seconds of read-only downtime.  |
| A MySQL cluster using replication for availability has a master database  and a few replicas. If the master fails, a replica should become the  new master. This requires you to manage the database lifecycle and  communicate the current system state to your application.                                                                                                     | Vitess helps to manage the lifecycle of your database scenarios. It  supports and automatically handles various scenarios, including master  failover and data backups.                                                                                                          |
| A MySQL cluster can have custom database configurations for different  workloads, like a master database for writes, fast read-only replicas  for web clients, slower read-only replicas for batch jobs, and so forth.  If the database has horizontal sharding, the setup is repeated for each  shard, and the app needs baked-in logic to know how to find the right  database. | Vitess uses a topology backed by a consistent data store, like etcd or  ZooKeeper. This means the cluster view is always up-to-date and  consistent for different clients. Vitess also provides a proxy that  routes queries efficiently to the most appropriate MySQL instance. |


### Vitess vs. NoSQL

If you're considering a NoSQL solution primarily because of concerns about the scalability of MySQL, Vitess might be a more appropriate choice for your application. While NoSQL provides great support for unstructured data, Vitess still offers several benefits not available in NoSQL datastores:

| NoSQL | Vitess |
|:--|:--|
| NoSQL databases do not define relationships between database tables, and only support a subset of the SQL language. | Vitess is not a simple key-value store. It supports complex query semantics such as where clauses, JOINS, aggregation functions, and more.  |
| NoSQL datastores do not usually support transactions.                                                               | Vitess supports transactions.                                                                                                               |
| NoSQL solutions have custom APIs, leading to custom architectures, applications, and tools.                         | Vitess adds very little variance to MySQL, a database that most people are already accustomed to working with.                              |
| NoSQL solutions provide limited support for database indexes compared to MySQL.                                     | Vitess allows you to use all of MySQL's indexing functionality to optimize query performance.                                               |
