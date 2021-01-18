---
title: VStream
---


VStream is a change notification service accessible via VTGate. The purpose of
VStream is to provide equivalent information to the MySQL binary logs from the
underlying MySQL shards of the Vitess cluster.  gRPC clients, including Vitess
components like VTTablets, can subscribe to a VStream to receive change events
from other shards.  The VStream pulls events from one or more VStreamer
instances on VTTablet instances, which in turn pulls events from the binary
log of the underlying MySQL instance.  This allows for efficient execution of
functions such as VReplication where a subscriber can indirectly receive
events from the binary logs of one or more MySQL instance shards, and then
apply it to a target instance. An user can leverage VStream to obtain in-depth
information about data change events for given Vitess keyspace, shard, and
position.  A single VStream can also consolidate change events from multiple
shards in a keyspace, making it a convenient tool to feed a CDC (Change Data
Capture) process downstream from your Vitess datastore.


For reference, please refer to the diagram below:

![VStream diagram](../img/VStream.svg)

Note: A VStream is distinct from a VStreamer. The former is located on the VTGate and the latter is located on the VTTablet.
