---
title: Vindexes
aliases: ['/docs/schema-management/consistent-lookup/','/docs/reference/vindexes/']
---

## A Vindex maps column values to keyspace IDs

A Vindex provides a way to map a column value to a `keyspace ID`. Since each shard in Vitess covers a range of `keyspace ID` values, this mapping can be used to identify which shard contains a row. A variety of vindexes are available to choose from with different trade-offs, and you can choose one that best suits your needs.

The Sharding Key is a concept that was introduced by NoSQL datastores. It is based on the fact that, in NoSQL databases, there is only one access path to the data, which is the Key. However, relational databases are more versatile with respect to the data stored and their relationships. So, sharding a database by only designating a single sharding key is often insufficient.

If one were to draw an analogy, the indexes in a database would be the equivalent of the key in a NoSQL datastore, except that databases allow multiple indexes per table, and there are many types of indexes. Extending this analogy to a sharded database results in different types of cross-shard indexes. In Vitess, these are called Vindexes.


## Advantages

The advantages of Vindexes stem from their flexibility:

* A table can have multiple Vindexes.
* Vindexes can be NonUnique, which allows a column value to yield multiple keyspace IDs.
* Vindexes can be a simple function or be based on a lookup table.
* Vindexes can be shared across multiple tables.
* Custom Vindexes can be created and used, and Vitess will still know how to reshard using such Vindexes.


### The Primary Vindex

The Primary Vindex for a table is analogous to a database primary key. Every sharded table must have one defined. A Primary Vindex must be unique: given an input value, it must produce a single keyspace ID. At the time of an insert to the table, the unique mapping produced by the Primary Vindex determines the target shard for the inserted row. Conceptually, this is equivalent to a NoSQL Sharding Key, and we often informally refer to the Primary Vindex as the Sharding Key.

However, there is a subtle difference:  NoSQL datastores allow a choice of the Sharding Key, but the Sharding Strategy or Function is generally hardcoded in the engine. In Vitess, the choice of Vindex allows control of how a column value maps to a keyspace ID. In other words, a Primary Vindex in Vitess not only defines the Sharding Key, but also decides the Sharding Strategy.

Uniqueness for a Primary Vindex does not mean that the column has to be a primary key or unique key in the MySQL schema for the underlying shard. You can have multiple rows that map to the same keyspace ID. The Vindex uniqueness constraint only ensures that all rows for a keyspace ID end up in the same shard.

Vindexes come in many varieties. Some of them can be used as Primary Vindex, and others have different purposes. We will describe their properties in the [Predefined Vindexes](#predefined-vindexes) section.


### Secondary Vindexes

Secondary Vindexes are additional vindexes against other columns of a table offering optimizations for WHERE clauses that do not use the Primary Vindex. Secondary Vindexes return a single or a limited set of `keyspace IDs` which will allow VTGate to only target shards where the relevant data is present. In the absence of a Secondary Vindex, VTGate would have to send the query to all shards (called a scatter query).

It is important to note that Secondary Vindexes are only used for making routing decisions. The underlying database shards will most likely need traditional indexes on those same columns, to allow efficient retrieval from the table on the underlying MySQL instances.


### Unique and NonUnique Vindex

A Unique Vindex is a vindex that yields at most one keyspace ID for a given input. Knowing that a Vindex is Unique is useful because VTGate can push down certain complex queries into VTTablet if it knows that the scope of that query can be limited to a single shard. Uniqueness is also a prerequisite for a Vindex to be used as Primary Vindex.

A NonUnique Vindex is analogous to a database non-unique index. It is a secondary index for searching by an alternate WHERE clause. An input value could yield multiple keyspace IDs, and rows could be matched from multiple shards. For example, if a table has a `name` column that allows duplicates, you can define a cross-shard NonUnique Vindex for it, and this will allow an efficient search for users that match a certain `name`.


### Functional and Lookup Vindex

A **Functional Vindex** is a vindex where the column value to keyspace ID mapping is pre-established, typically through an algorithmic function. In contrast, a **Lookup Vindex** is a vindex that provides the ability to create an association between a value and a keyspace ID, and recall it later when needed. Lookup Vindexes are sometimes also informally referred to as cross-shard indexes.

Typically, the Primary Vindex for a table is Functional. In some cases, it is the identity function where the input value yields itself as the keyspace id. However, other algorithms like a hashing function can also be used.

A Lookup Vindex is implemented as a MySQL lookup table that maps a column value to the keyspace id. This is usually needed when database user needs to efficiently find a row using a WHERE clause that does not contain the Primary Vindex. At the time of insert, the computed keyspace ID of the row is stored in the lookup table against the column value.


### Lookup Vindex types

The lookup table that implements a Lookup Vindex can be sharded or unsharded.  Note that the lookup row is most likely not going to be in the same shard as the keyspace id it points to.

Vitess allows for the transparent population of these lookup table rows by assigning an owner table, which is the main table that requires this lookup. When a row is inserted into this owner table, the lookup row for it is created in the lookup table. The lookup row is also deleted upon a delete of the corresponding row in the owner table. These essentially result in distributed transactions, which traditionally require 2PC to guarantee atomicity.

Consistent lookup vindexes use an alternate approach that makes use of careful locking and transaction sequences to guarantee consistency without using 2PC. This gives the best of both worlds, with the benefit of a consistent cross-shard vindex without paying the price of 2PC.

There are currently two vindex types in Vitess for consistent lookup:
* `consistent_lookup_unique`
* `consistent_lookup`


#### Shared Vindexes

Relational databases encourage normalization, which allows the splitting of data into different tables to avoid duplication in the case of one-to-many relationships. In such cases, a key is shared between the two tables to indicate that the rows are related, a.k.a. `Foreign Key`.

In a sharded environment, it is often beneficial to keep those rows in the same shard. If a Lookup Vindex was created on the foreign key column of each of those tables, the backing tables would actually be identical. In such cases, Vitess allows sharing a single Lookup Vindex for multiple tables. One of these tables is designated as the owner of the Lookup Vindex, and is responsible for creating and deleting these associations. The other tables just reuse these associations.

An existing `lookup_unique` vindex can be trivially switched to a `consistent_lookup_unique` by changing the vindex type in the VSchema. This is because the data is compatible. Caveat: If you delete a row from the owner table, Vitess will not perform cascading deletes. This is mainly for efficiency reasons;  the application is likely capable of doing this more efficiently.

As for a `lookup` vindex, it can be changed it to a `consistent_lookup` only if the `from` columns can uniquely identify the owner row. Without this, many potentially valid inserts would fail.

Functional Vindexes can be also be shared. However, there is no concept of ownership because the column to keyspace ID mapping is pre-established.


### Lookup Vindex guidance

The guidance for implementing lookup vindexes has been to create a two-column table. The first column (`from` column) should match the type of the column of the main table that needs the vindex. The second column (`to` column) should be a `BINARY` or a `VARBINARY` large enough to accommodate the keyspace id.

This guidance remains the same for unique lookup vindexes.

For non-unique lookup Vindexes, the lookup table should consist of multiple columns. The first column continues to be the input for computing the keyspace IDs. Beyond this, additional columns are needed to uniquely identify the owner row. This should typically be the primary key of the owner table. But it can be any other column that can be combined with the `from` column to uniquely identify the owner row. The last column remains the keyspace ID like before.

For example, if a user table had the columns `(user_id, email)`, where `user_id` was the primary key and `email` needed a non-unique lookup vindex, the lookup table would have the columns `(email, user_id, keyspace_id)`.


### Indepedence

The previously described properties are mostly independent of each other. Combining them gives rise to the following valid categories:

* **Functional Unique**: The most popular category because it is the one best suited to be a Primary Vindex.
* **Functional NonUnique**: There are currently no use cases that need this category.
* **Lookup Unique Owned**: Used for optimizing high QPS read queries that do not use the Primary Vindex columns in their WHERE clause. There is a price to pay: an extra write to the lookup table for insert and delete operations, and an extra lookup for read operations. However, it may be worth it to avoid high QPS read queries to be sent to all shards. The overheard of maintaining the lookup table is amortized as the number of shards grow.
* **Lookup Unique Unowned**: Can be used as an optimization as described in the Shared Vindexes section.
* **Lookup NonUnique Owned**: Used for high QPS queries on columns that are non-unique.
* **Lookup NonUnique Unowned**: You would rarely have to use this category because it is unlikely that you will be using a column as foreign key that is not unique within a shard. But it is theoretically possible.

Of the above categories, `Functional Unique` and `Lookup Unique Unowned` Vindexes can be a Primary Vindex. This is because those are the only ones that are unique and have the column to keyspace ID mapping pre-established. This is required because the Primary Vindex is responsible for assigning the keyspace ID for a row when it is created.

However, it is generally not recommended to use a Lookup Vindex as a Primary Vindex because it is too slow for resharding. If absolutely unavoidable, it is recommended to add a `keyspace ID` column to the tables that need this level of control of the row-to-shard mapping. While resharding, Vitess can use that column to efficiently compute the target shard. Vitess can also be configured to auto-populate that column on inserts. This is done using the reverse map feature explained [below](#insert).


### How Vindexes are used

#### Cost

Vindexes have costs. For routing a query, the applicable Vindex with the lowest cost is chosen. The current general costs for the different Vindex Types are as follows:

Vindex Type | Cost
----------- | ----
Identity | 0
Functional | 1
Lookup Unique | 10
Lookup NonUnique | 20

#### Select

In the case of a simple select, Vitess scans the WHERE clause to match references to Vindex columns and chooses the best one to use. If there is no match and the query is simple without complex constructs like aggregates, etc., it is sent to all shards.

Vitess can handle more complex queries. For now, refer to the [design doc](https://github.com/vitessio/vitess/blob/master/doc/V3HighLevelDesign.md) for background information on how it handles them.

#### Insert

* The Primary Vindex is used to generate a keyspace ID.
* The keyspace ID is validated against the rest of the Vindexes on the table. There must exist a mapping from the column value(s) for these Secondary Vindexes to the keyspace ID.
* If a column value was not provided for a Vindex and the Vindex is capable of reverse mapping a keyspace ID to an input value, that function is used to auto-fill the column. If there is no reverse map, it is an error.

#### Update

The WHERE clause is used to route the update. Updating the value of a Vindex column is supported, but with a restriction: the change in the column value should not result in the row being moved from one shard to another. A workaround is to perform a delete followed by insert, which works as expected.

#### Delete

If the table owns lookup vindexes, then the rows to be deleted are first read and the associated Vindex entries are deleted. Following this, the query is routed according to the WHERE clause.

### Predefined Vindexes

Vitess provides the following predefined Vindexes:

Name | Type | Description | Primary | Reversible | Cost | Data types |
---- | ---- | ----------- | ------- | ---------- | ---- | ---------- |
binary | Functional Unique | Identity | Yes | Yes | 0 | Any |
binary\_md5 | Functional Unique | MD5 hash | Yes | No | 1 | Any |
consistent\_lookup | Lookup NonUnique | Lookup table non-unique values | No | No | 20 | Any |
consistent\_lookup\_unique | Lookup Unique | Lookup table unique values | If unowned | No | 10 | Any |
hash | Functional Unique | DES null-key hash | Yes | Yes | 1 | 64 bit or smaller numeric or equivalent type |
lookup | Lookup NonUnique | Lookup table non-unique values | No | No | 20 | Any |
lookup\_unique | Lookup Unique | Lookup table unique values | If unowned | No | 10 | Any |
null | Functional Unique | Always map to keyspace ID 0 | Yes | No | 100 | Any |
numeric | Functional Unique | Identity | Yes | Yes | 0 | 64 bit or smaller numeric or equivalent type |
numeric\_static\_map | Functional Unique | JSON file statically mapping input string values to keyspace IDs | Yes | No | 1 | Any |
region\_experimental | Functional Unique | Multi-column prefix-based hash for use in geo-partitioning | Yes | No | 1 | String and numeric type |
region\_json | Functional Unique | Multi-column prefix-based hash combined with a JSON map for key-to-region mapping, for use in geo-partitioning | Yes | No | 1 | String and numeric type |
reverse\_bits | Functional Unique | Bit reversal | Yes | Yes | 1 | 64 bit or smaller numeric or equivalent type |
unicode\_loose\_md5 | Functional Unique | Case-insensitive (UCA level 1) MD5 hash | Yes | No | 1 | String or binary types |
unicode\_loose\_xxhash | Functional Unique | Case-insensitive (UCA level 1) xxHash64 hash | Yes | No | 1 | String or binary types |
xxhash | Functional Unique | xxHash64 hash | Yes | No | 1 | Any |

Consistent lookup vindexes, as described above, are a new category of Vindexes that are meant to replace the existing lookup Vindexes implementation. For the time being, they have a different name to allow for users to switch back and forth.

Custom Vindexes can also be created as needed. At the moment there is no formal plugin system for custom Vindexes, but the interface is well-defined, and thus custom implementations including code performing arbitary lookups in other systems can be accomodated.

\
\
There are also the following legacy (deprecated) Vindexes. **Do not use these**:

Name | Type | Primary | Reversible | Cost
---- | ---- | ------- | ---------- | ----
lookup\_hash | Lookup NonUnique | No | No | 20
lookup\_hash\_unique | Lookup Unique | If unowned | No | 10
lookup\_unicodeloosemd5\_hash | Lookup NonUnique | No | No | 20
lookup\_unicodeloosemd5\_hash\_unique | Lookup Unique | If unowned | No | 10
