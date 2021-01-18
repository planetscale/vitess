---
title: Presentations and Videos
weight: 1
aliases: ['/resources/presentations.html']
---

## CNCF Webinar 2020

Lizz van Dijk demonstrates how to migrate from a regular MySQL release to Vitess.

{{< youtube id="W8VbiXo39Ik" autoplay="false" >}}

## MySQL Pre-FOSDEM Day 2020

Lizz van Dijk presents an introduction to Vitess for MySQL users.

<iframe src = "/ViewerJS/#../files/2020-lizz-prefosdem.pdf" width='600' height='450' allowfullscreen webkitallowfullscreen></iframe>

## QCon 2019

Sugu Sougoumarane gives an overview of the salient features of Vitess.

[Video and Slides](https://www.infoq.com/presentations/vitess/)

## KubeCon San Diego 2019

KubeCon featured several Vitess talks, including:

* [Scaling Resilient Systems: A Journey into Slack's Database Service](https://youtu.be/aTItjMJE17c) - Rafael Chacon & Guido Iaquinti, Slack
* [How to Migrate a MySQL Database to Vitess](https://youtu.be/OCS45iy5v1M) - Sugu Sougoumarane & Morgan Tocker, PlanetScale
* [Building a Database as a Service on Kubernetes](https://youtu.be/469NOldFOgw) - Abhi Vaidyanatha & Lucy Burns, PlanetScale
* [Vitess: Stateless Storage in the Cloud](https://youtu.be/z63dtNj6ctY) - Sugu Sougoumarane, PlanetScale
* [Geo-partitioning with Vitess](https://youtu.be/-Hz6LFJu1cY) - Deepthi Sigireddi & Jitendra Vaidya, PlanetScale
* [Gone in 60 Minutes: Migrating 20 TB from AKS to GKE in an Hour with Vitess](https://youtu.be/KpygSD-v_ws) - Derek Perkins, Nozzle

Vitess was also featured during the [CNCF project updates keynote](https://www.youtube.com/watch?v=two3TzF9mVY&feature=youtu.be&t=105)!

## Highload 2019

Sugu Sougoumarane presents an overview of Vitess at Highload in Moscow.

{{< pdf src="/ViewerJS/#../files/2019-sugu-highload.pdf" >}}

## Utah Kubernetes Meetup 2019

Jiten Vaidya shows how you can extend Vitess to create jurisdiction-aware database clusters.

{{< pdf src="/ViewerJS/#../files/2019-jiten-utah.pdf" >}}

## CNCF Meetup Paris 2019

Sugu Sougoumarane and Morgan Tocker present a three hour Vitess workshop on Kubernetes.

{{< pdf src="/ViewerJS/#../files/2019-paris-cncf.pdf" >}}

## Percona Live Europe 2019

**My First 90 Days with Vitess**

Morgan Tocker talks about his adventures in Vitess, after having come from a MySQL background.

{{< pdf src="/ViewerJS/#../files/2019-morgan-percona-eu.pdf" >}}


**Sharded MySQL on Kubernetes**

Sugu Sougoumarane presents an overview of running sharded MySQL on Kubernetes.

{{< pdf src="/ViewerJS/#../files/2019-sugu-percona-eu.pdf" >}}

## Vitess Meetup 2019 @ Slack HQ

**Vitess: New and Coming Soon!**

Deepthi Sigireddi shares new features recently introduced in Vitess, and what's on the roadmap moving forward.

{{< pdf src="/ViewerJS/#../files/2019-deepthi-vitess-meetup.pdf" >}}


**Deploying multi-cell Vitess**

Rafael Chacon Vivas describes how Vitess is used in Slack.

{{< pdf src="/ViewerJS/#../files/2019-rafael-vitess-meetup.pdf" >}}


**Vitess at Pinterest**

David Weitzman provides an overview of how Vitess is used at Pinterest.

{{< youtube id="1cWWlaqlia8" autoplay="false" >}}


**No more Regrets**

Sugu Sougoumarane demonstrates new features coming to VReplication.

{{< youtube id="B1Nrtptjtcs" autoplay="false" >}}

## Cloud Native Show 2019

**Vitess at scale - how Nozzle.io runs MySQL on Kubernetes**

Derek Perkins joins the Cloud Native show and explains how Nozzle uses Vitess.

[Listen to Podcast](https://anchor.fm/cloud-native-show/episodes/Vitess-at-scale---how-Nozzle-io-runs-MySQL-on-Kubernetes-e4m5lo)

## CNCF Webinar 2019

**Vitess: Sharded MySQL on Kubernetes**

Sugu Sougoumarane provides an overview of Vitess for Kubernetes users.

{{< youtube id="E6H4bgJ3Z6c" autoplay="false" >}}

## Kubecon China 2019

**How JD.Com runs the World's Largest Vitess**

Xuhaihua and Jin Ke Xie present on their experience operating the largest known Vitess cluster, two years in.

{{< youtube id="qww4UVNG3Io" autoplay="false" >}}

## RootConf 2019

**OLTP or OLAP: why not both?**

Jiten Vaidya from PlanetScale explains how you can use both OLTP and OLAP on Vitess.

{{< youtube id="bhzJJF82mFc" autoplay="false" >}}

## Kubecon 19 Barcelona

**Vitess Deep Dive**

Jiten Vaidya and Dan Kozlowski from PlanetScale deep dive on Vitess.

{{< youtube id="OZl4HrB9p-8" autoplay="false" >}}

## Percona Live Austin 2019

**Vitess: Running Sharded MySQL on Kubernetes**

Sugu Sougoumarane shows how you can run sharded MySQL on Kubernetes.

{{< youtube id="v7oxiVmGXp4" autoplay="false" >}}


**MySQL, Kubernetes, Business & Enterprise**

David Cohen (Intel), Steve Shaw (Intel) and Jiten Vaidya (PlanetScale) discuss Open Source cloud native databases.

[View Talk Abstract and Slides](https://www.percona.com/live/19/sessions/an-open-source-cloud-native-database-cndb)

## Velocity New York 2018

**Smooth scaling: Slack’s journey toward a new database**

Slack has experienced tremendous growth for a young company, serving over nine million weekly active customers. But with great growth comes greater growth pains. Slack’s rapid growth over the last few years outpaced the scaling capacity of its original sharded MySQL database, which negatively impacted the company’s customers and engineers.

Ameet Kotian explains how a small team of engineers embarked on a journey for the right database solution, which eventually led them to Vitess, a powerful open source database cluster solution for MySQL. Vitess combines the features of MySQL with the scalability of a NoSQL database. It has been serving Youtube’s traffic for numerous years and has a strong community.

Although Vitess meets a lot of Slack’s needs, it’s not an out-of-the-box solution. Ameet shares how the journey to Vitess was planned and executed, with little customer impact, in the face of piling operational challenges, such as AWS issues, MySQL replication, automatic failovers, deployments strategies, and so forth. Ameet also covers Vitess’s architecture, trade-offs, and what the future of Vitess looks like at Slack.

Ameet Kotkian, senior storage operations engineer at Slack, shows us how Slack uses Vitess.

<iframe src = "/ViewerJS/#../files/20181002-ameet-velocity-slides.pdf" width='600' height='450' allowfullscreen webkitallowfullscreen></iframe>

## Percona Live Europe 2017

**Migrating to Vitess at (Slack) Scale**

Slack is embarking on a major migration of the MySQL infrastructure at the core of our service to use Vitess' flexible sharding and management instead of our simple application-based shard routing and manual administration. This effort is driven by the need for an architecture that scales to meet the growing demands of our largest customers and features under the pressure to maintain a stable and performant service that executes billions of MySQL transactions per hour. This talk will present the driving motivations behind the change, why Vitess won out as the best option, and how we went about laying the groundwork for the switch. Finally, we will discuss some challenges and surprises (both good and bad) found during our initial migration efforts, and suggest some ways in which the Vitess ecosystem can improve that will aid future migration efforts.

Michael Demmer shows us how, at [Percona Live Europe 2017](https://www.percona.com/live/e17/sessions/migrating-to-vitess-at-slack-scale).

{{< pdf src="/ViewerJS/#../files/2017-demmer-percona.pdf" >}}

## Vitess Deep Dive sessions

Start with session 1 and work your way through the playlist. This series focuses on the V3 engine of VTGate.

{{< youtube id="6yOjF7qhmyY" autoplay="false" >}}

## Percona Live 2016

[Sugu](https://github.com/sougou) and [Anthony](https://github.com/enisoc) showed what it looks like to use Vitess now that Keyspace IDs can be completely hidden from the application. They gave a live demo of resharding the Guestbook sample app, which now knows nothing about shards, and explained how new features in VTGate make all of this possible.

{{< pdf src="/ViewerJS/#../files/percona-2016.pdf" >}}

## CoreOS Meetup, January 2016

Vitess team member [Anthony Yeh](https://github.com/enisoc)'s talk at
the [January 2016 CoreOS Meetup](http://www.meetup.com/coreos/events/228233948/)
discussed challenges and techniques for running distributed databases
within Kubernetes, followed by a deep dive into the design trade-offs
of the [Vitess on Kubernetes](https://github.com/vitessio/vitess/tree/master/examples/kubernetes)
deployment templates.

{{< pdf src="/ViewerJS/#../files/coreos-meetup-2016-01-27.pdf" >}}

## Oracle OpenWorld 2015

Vitess team member [Anthony Yeh](https://github.com/enisoc)'s talk at Oracle OpenWorld 2015 focused on what the [Cloud Native Computing](http://cncf.io) paradigm means when applied to MySQL in the cloud. The talk also included a deep dive into [transparent, live resharding](../../sharding), one of the key
features of Vitess that makes it well-adapted for a Cloud Native environment.

{{< pdf src="/ViewerJS/#../files/openworld-2015-vitess.pdf" >}}

## Percona Live 2015

Vitess team member [Anthony Yeh](https://github.com/enisoc)'s talk at Percona Live 2015 provided an overview of Vitess as well as an explanation of how Vitess has evolved to live in a containerized world with Kubernetes and Docker.

{{< pdf src="/ViewerJS/#../files/percona-2015-vitess-and-kubernetes.pdf" >}}

## Google I/O 2014 - Scaling with Go: YouTube's Vitess

In this talk, [Sugu Sougoumarane](https://github.com/sougou) from the Vitess team talks about how Vitess solved YouTube's scalability problems as well as about tips and techniques used to scale with Go.<br><br>

{{< youtube id="midJ6b1LkA0" autoplay="false" >}}
