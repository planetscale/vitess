---
title: Vitess sees the wrong tablet as master
description: Debug common issues with Vitess
weight: 10
---

## Vitess sees the wrong tablet as master

If you do a failover manually (not through Vitess), you'll need to tell Vitess which tablet corresponds to the new master MySQL. Until then, writes will fail since they'll be routed to a read-only replica (the old master). Use the [`TabletExternallyReparented`](../../reference/programs/vtctl/#tabletexternallyreparented) command to tell Vitess the new master tablet for a shard.

Tools like [Orchestrator](https://github.com/github/orchestrator) can be configured to call this automatically when a failover occurs. See our sample [orchestrator.conf.json](https://github.com/vitessio/vitess/blob/1129d69282bb738c94b8af661b984b6377a759f7/docker/orchestrator/orchestrator.conf.json#L131) for an example of this.
