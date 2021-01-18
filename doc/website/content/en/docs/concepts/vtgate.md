---
title: VTGate 
---

VTGate is a lightweight proxy server that routes traffic to the correct [VTTablet](../tablet) servers and returns consolidated results back to the client. It speaks both the MySQL Protocol and the Vitess gRPC protocol. Thus, your applications can connect to VTGate as if it is a MySQL Server.

When routing queries to the appropriate VTTablet servers, VTGate considers the sharding scheme, required latency and the availability of tables and their underlying MySQL instances.

**Related Vitess Documentation**

* [Execution Plans](../execution-plans)
