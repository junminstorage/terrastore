# Terrastore #

Terrastore is a modern document store which provides advanced scalability and elasticity features without sacrificing consistency.

**Based on Terracotta**
> Terrastore is based on **[Terracotta](http://www.terracotta.org)**, so it relies on an industry-proven, fast (and cool) clustering technology.

**Ubiquitous**
> Terrastore is accessed through the universally supported **HTTP** protocol.

**Distributed**
> Terrastore is a distributed document store supporting single-cluster and multi-cluster deployments.

**Elastic**
> Terrastore is elastic: you can add and remove nodes **dynamically** to/from your running cluster(s) with no downtime and no changes at all to your configuration.

**Scalable at the data layer**
> Terrastore automatically scales your data: documents are **partitioned** and distributed among your nodes, with **automatic** and **transparent** re-balancing when nodes join and leave.

**Scalable at the computational layer**
> Terrastore automatically scales your computations: query and update operations are **distributed** to the nodes which actually holds the queried/updated data,              minimizing network traffic and spreading computational load.

**Consistent**
> Terrastore provides **per-document consistency** features: you're guaranteed to always get the latest value of a single document, with _read committed_ isolation for concurrent modifications. More complex consistency/transactional requirements, such as multi-document consistency, are not supported in order to guarantee scalability.

**Schemaless**
> Terrastore provides a collection-based key/value interface managing **JSON** documents with no pre-defined schema: just create your buckets and put everything you want into.

**Easy operations**
> Install a fully working cluster in just **a few commands and no XML to edit**.

**Focused on data management and processing features**
> Terrastore, being based on a rock solid technology such as Terracotta, will focus more and more on advanced features and extensibility. Right now, Terrastore provides support for:
    * **Custom data partitioning**.
    * **Event processing**.
    * **Push-down predicates**.
    * **Range queries**.
    * **Map/Reduce querying and processing**.
    * **Server-side update functions**.

## Release history ##

  * **September 18, 2011** : Version 0.8.2.
  * **February 11, 2011** : Version 0.8.1.
  * **December 13, 2010** : Version 0.8.0.
  * **September 23, 2010** : Version 0.7.1.
  * **September 12, 2010** : Version 0.7.0.
  * **July 25, 2010** : Version 0.6.0.
  * **May 28, 2010** : Version 0.5.1.
  * **May 21, 2010** : Version 0.5.0.
  * **February 27, 2010** : Version 0.4.2.
  * **February 13, 2010** : Version 0.4.1.
  * **January 29, 2010** : Version 0.4.
  * **January 06, 2010** : Version 0.3.1.
  * **December 29, 2009** : Version 0.3.
  * **December 17, 2009** : Version 0.2.
  * **November 25, 2009** : Version 0.1.