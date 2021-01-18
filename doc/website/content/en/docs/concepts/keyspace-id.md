---
title: Keyspace ID
---

The *keyspace ID* is the value that is used to decide on which shard a given row lives. [Range-based Sharding](../../reference/sharding#key-ranges-and-partitions) refers to creating shards that each cover a particular range of keyspace IDs.

Using this technique means you can split a given shard by replacing it with two or more new shards that combine to cover the original range of keyspace IDs, without having to move any records in other shards.

The keyspace ID itself is computed using a function of some column in your data, such as the user ID. Vitess allows you to choose from a variety of functions ([vindexes](../../reference/vschema)) to perform this mapping. This allows you to choose the right one to achieve optimal distribution of the data across shards.

