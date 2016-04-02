# Terrastore Search Integration #



## Overview ##

Terrastore-Search is a sub-project aimed at integrating Terrastore distributed store with [Elastic Search](http://www.elasticsearch.com/) distributed search engine, in order to provide the powerful storage and processing capabilities of Terrastore **with** the powerful near real-time full-text search capabilities of Elastic Search.

More specifically, Terrastore-Search provides:
  * Transparent, near real-time, indexing of Terrastore documents inside Elastic Search.
  * Embedding of Elastic Search inside Terrastore server, both as a complete self-contained server (to avoid running separated instances), or as a member of a larger Elastic Search cluster.

## Installation ##

Terrastore-Search must be installed into **every** Terrastore server node in your cluster.
<br />
So, the first step you need to execute is to [download](http://code.google.com/p/terrastore/downloads/list) and [install](Operations.md) the latest Terrastore distribution, setting up as many server nodes as you want.
<br />
Then, to install Terrastore-Search into a Terrastore server node you only have to:
  * Install [Apache Ant 1.7](http://ant.apache.org).
  * [Download](http://code.google.com/p/terrastore/downloads/list) the latest Terrastore-Search distribution and unpack.
  * Install Terrastore-Search by executing the Ant-based script:
```
$> ant -f  terrastore-search-install.xml install -Dterrastore.home=/path_to_terrastore_server
```

## Configuration ##

Terrastore-Search can be easily configured by editing the _path\_to\_terrastore\_server_/extensions/terrastore-search.properties file.<br />
It is configured with the following base properties:
  * node.data (true) : configure the embedded Elastic Search node to actually store indexed data.
  * http.enabled (true) : enable the HTTP interface.
  * http.port (9000) : define the exposed HTTP port.
  * transport.tcp.port (9001) : define the TCP port used for internal communication.
  * network.host (127.0.0.1) : define the exposed host address.
You can change them to suit your needs: for a complete list and explanation of configuration parameters, take a look at Elastic Search [docs](http://www.elasticsearch.com/docs/).

The default Terrastore-Search index configuration will create a single index named _search_, and a different index type per bucket: you can change this as well, by editing the _path\_to\_terrastore\_server_/extensions/terrastore-search-extension.xml configuration file.

## Usage ##

Terrastore-Search is completely transparent: everytime you add, update or remove a document into Terrastore, changes will be reflected into Elastic Search index.

The embedded Elastic Search instance can be queries through the following address (provided you kept all default configurations): `http://127.0.0.1:9000/search/bucket_name`

So, you can try and test your Terrastore / Elastic Search cluster with the [curl](http://curl.haxx.se/) command line tool:

  * Add a document:
```
curl -v -X PUT -H "Content-Type: application/json" -d "{\"key\" : \"value\"}" http://127.0.0.1:8080/bucket/test
```
  * Search for the document:
```
curl -v -X GET -H "Content-Type: application/json" http://127.0.0.1:9000/search/bucket/_search?q=key:value
```

## Reliable indexing ##

Terrastore uses under the hood in-memory queues to propagate changes toward Elastic Search: while being extremely fast and completely transparent, this means that in case of sudden crashes, you may lose the latest bunch of index operations.<br />
You can setup reliable event delivery, and hence reliable indexing, by changing the Terrastore event bus configuration as explained [here](http://code.google.com/p/terrastore/wiki/Developers_Guide#Events).