---
title: Vitess Roadmap
description: Upcoming features planned for development
weight: 2
---

As an open source project, Vitess is developed by a community of contributors. Many of the contributors run Vitess in production, and add features to address their specific pain points. As a result of this, we can not guarantee features listed here will be implemented in any specific order.

{{< info >}}
If you have a specific question about the Roadmap, we recommend posting in our [Slack channel](https://vitess.slack.com), click the Slack icon in the top right to join. This is a very active community forum and a great place to interact with other users.
{{< /info >}}

## Short Term

- Improve Documentation
- Improve Usability
- Support more MySQL Syntax (improve compatibility as a drop-in replacement)
  - Certify popular frameworks like Ruby on Rails, Hibernate etc.
- Vitess-native unplanned failovers
- Pluggable durability policies
- Nightly benchmarking (regression testing)
- Schema changes through vitess
  - gh-ost and pt-osc integration
- VReplication
  - VExec tool for management

## Medium Term

- Vttablet to manage more than one MySQL schema
- Rewrite of vtctld UI including visualization of VReplication
- VReplication throttling
- Binlog server
- Topology Service: Reduce dependencies on the topology service. i.e. Vitess should be operable normally even if topology service is down for several hours. Topology service should be used only for passive discovery.
- Support for PostgreSQL: Vitess should be able to support PostgreSQL for both storing data, and speaking the protocol in VTGate.
