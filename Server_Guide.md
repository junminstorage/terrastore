# Server Guide #



Terrastore is composed by two different kind of processes:

  * **Terrastore master(s)** : providing cluster management and storage services.
  * **Terrastore server(s)** : providing actual access to Terrastore data and operations.

An operational **Terrastore cluster** is made up of one Terrastore active master plus optionally one or more passive masters, and at least one Terrastore server, as illustrated below:

![http://wiki.terrastore.googlecode.com/hg/images/cluster.png](http://wiki.terrastore.googlecode.com/hg/images/cluster.png)

Client requests go through server nodes and get eventually routed in between, depending on document partitioning, while under the hood documents get actually write/read to/from the active master.

In order to achieve enhanced performance and scalability, you can join multiple clusters together in a so called **Terrastore ensemble**, as illustrated below:

![http://wiki.terrastore.googlecode.com/hg/images/ensemble.png](http://wiki.terrastore.googlecode.com/hg/images/ensemble.png)

The ensemble is a federation of different clusters linked together and working as a whole: clients will be able to contact any server node without caring about the cluster it actually belongs to, because requests will be transparently routed and documents transparently picked up from the correct cluster/server.

Let's see how to set up and manage a Terrastore cluster in all of its different parts.

## Prerequisites ##

Terrastore, and its installation tool, require the following:

  * Unix operating system or a [Cygwin](http://www.cygwin.com/) environment on Windows systems.
  * [Java 6 runtime environment](http://java.sun.com/).
  * [Apache Ant](http://ant.apache.org) 1.7 (or higher).

## Set up a Terrastore master node ##

Setting up the Terrastore master node(s) is the first mandatory step to run a Terrastore cluster.
The Terrastore installation tool gives you two different configurations:

  * Single-master installation: the typical development configuration, providing one single master node for the whole cluster.
  * Active/Passive master installation: the typical production configuration, providing one active master and one hot-standby ready to take over in case of failures.

### Set up a single Terrastore master node ###

In order to install and run a single Terrastore master node, just do the following:

  * Download the [latest Terrastore distribution](http://code.google.com/p/terrastore/downloads/list) and unpack.
  * Install your Terrastore master:
    * $> ant -f terrastore-install.xml single-master -Dinstall.dir=/path\_to\_master
  * Start your master:
    * $path\_to\_master/bin> sh start.sh

The master will listen on the 9510 port.

### Set up two Terrastore master nodes in active/passive ###

In order to install and run two Terrastore master nodes in active/passive configuration, just do the following:

  * Download the [latest Terrastore distribution](http://code.google.com/p/terrastore/downloads/list) and unpack.
  * Install the two Terrastore masters (on different hosts):
    * $> ant -f terrastore-install.xml ha-master-1 -Dinstall.dir=/path\_to\_master-1
    * $> ant -f terrastore-install.xml ha-master-2 -Dinstall.dir=/path\_to\_master-2
  * Start your active master:
    * $path\_to\_master-1/bin> sh start.sh
  * Start your passive master:
    * $path\_to\_master-2/bin> sh start.sh

The master 1 will listen on the 9510 port, while the master 2 will listen on the 9511 port.
Please note that once installed, the two masters can be run in whatever order: the first master to be run will start as the active one, while the second as the passive one.

### Configure server-to-master reconnection timeout ###

By default, masters do not allow automatic server reconnection: this means that in case of network failures preventing server-to-master communication, failing servers will shutdown gracefully and the whole cluster will be immediately operational thanks to the other working servers.

However, if you know in advance that your network may experience relatively short network outages, you can configure automatic server-to-master reconnection by installing master(s) with the following parameter enabled:

  * server.reconnection.timeout : this is the time (in milliseconds) servers will be allowed to reconnect prior to shutting down.

For example, to configure your cluster with a server-to-master reconnection timeout of 3 seconds:

```
$> ant -f terrastore-install.xml single-master -Dinstall.dir=/wherever -Dserver.reconnection.timeout=3000 
```

No special parameter needs to be set while installing Terrastore servers, because reconnection is controlled by the master.

As a final note, be careful: during the reconnection window, the cluster may block its
operations to allow servers to reconnect and preserve their consistent state: so **do not** set an high server-to-master reconnection timeout.

### Other options for master installation ###

The Terrastore installation tool provides the following additional properties to further control master configuration (default value is in parenthesis):

  * master.host (0.0.0.0) : master host address to bind to in single master mode.
  * master.host.1 (0.0.0.0) : first master host address to bind to in active/passive (ha) master mode.
  * master.host.2 (0.0.0.0) : second master host address to bind to in active/passive (ha) master mode.
  * master.server.port (9510) : port used for master-server communication in single master mode.
  * master.jmx.port (9520) : port used for jmx monitoring (through the Terracotta console) in single master mode.
  * master.server.port.1 (9510) : port used by the first master for master-server communication in active/passive (ha) master mode.
  * master.jmx.port.1 (9520) : port used by the first master for jmx monitoring (through the Terracotta console) in active/passive (ha) master mode.
  * master.ha.port.1 (9530) : port used by the first master for master-master communication during active election in active/passive (ha) master mode.
  * master.server.port.2 (9511) : port used by the second master for master-server communication in active/passive (ha) master mode.
  * master.jmx.port.2 (9521) : port used by the second master for jmx monitoring (through the Terracotta console) in active/passive (ha) master mode.
  * master.ha.port.2 (9531) : port used by the second master for master-master communication during active election in active/passive (ha) master mode.

## Set up a Terrastore server node ##

Terrastore server nodes serve requests to clients and provide all actual services.<br />
While you need at least only one server node, adding more server nodes will improve availability, data-access performance and computational power.

### Add a Terrastore server node ###

The Terrastore install tool will help you install and add a node to your cluster:

  * Download the [latest Terrastore distribution](http://code.google.com/p/terrastore/downloads/list) and unpack.
  * Install your server:
    * $> ant -f terrastore-install.xml server -Dinstall.dir=/path\_to\_server
  * Run:
    * $path\_to\_server/bin> sh start.sh --master _host_:_port_

The _--master_ command-line argument is the only mandatory parameter, pointing to your (currently active) master node, or to a comma-separated list of master hosts (both active and passives); addresses must always be in the form _host_:_port_.

All other command-line arguments are optional, as explained in sections below.

### Setup network settings ###

Terrastore server nodes provide the following network settings controlling public communication to clients (http host and port), internal server-to-server communication (node host and port), and communication timeouts:

  * --httpHost (127.0.0.1) : The server http host address to listen to for client requests.
  * --httpPort (8205) : The server http port to listen to for client requests.
  * --nodeHost (same as httpHost) : The address to listen to for other server nodes requests.
  * --nodePort (8226) : The port to listen to for other server nodes requests.
  * --nodeTimeout (10000) : Timeout in milliseconds for node-to-node communication.
  * --reconnectTimeout (10000) : Timeout in milliseconds for server-to-master reconnection when switching from active to passive in case of failures of the former.

### Setup thread and concurrency management ###

Terrastore server nodes provides different thread pools to deal with public http request receiving, and internal request processing:

  * --httpThreads (100) : Number of http threads servicing requests.
  * --workerThreads (100) : Number of worker threads used by the node to execute operations outside of the http request thread.

Moreover, you can setup the server concurrency level, that is, the max number of concurrent write operations that can happen inside a single node: it defaults to 1024, and can be changed with the _--concurrencyLevel_ command-line argument.

### Setup document and/or communication compression ###

Terrastore server nodes are able to compress both documents, in order to efficiently store more data in memory and use less disk space, and server-to-server communication,  in order to optimize network bandwidth usage:

  * --compressDocs (false) : Set to _true_ to enable document compression.
  * --compressCommunication (false) : set to _true_ to enable server-to-server communication compression.

The compression algorithm is LZF.

### Setup request failover ###

Terrastore server nodes, when internally routing requests to other nodes, can failover requests in case of communication failures, retrying the same request to the same or to another node depending on the actual partitioning; this feature is disabled by default, but you can enable it as follows:

  * --failoverRetries (0) : Number of retries In case of communication errors between cluster nodes (such as request timeouts).
  * --failoverInterval (0) : Number of milliseconds to wait between failover retries.

### Setup Cross Origin Resource Sharing support ###

[Cross Origin Resource Sharing](http://www.w3.org/TR/cors/) (CORS) is a W3C specification describing a mechanism to allow client-side remote access to server resources: Terrastore CORS implementation allows browser-based applications to access Terrastore servers even from remote machines.

Terrastore CORS implementation works with most modern browsers: it has been currently tested with FireFox 3.5+, Safari 5+ and Chrome.

By default, CORS is **enabled** on Terrastore for all hosts: so, your client-side browser applications will be able to access remote Terrastore servers with no additional configurations.

To disable/restrict CORS support, either for security or performance reasons, just startup your Terrastore servers with the _--allowedOrigins_ command-line argument, either followed by the "!" symbol to completely disable CORS support, or by the comma-separated list of URLs allowed to access Terrastore resources, i.e. "--allowedOrigins http://192.168.1.1:80,http://192.168.1.2:80".

To enable access for all clients, you can also pass the asterisk wildcard.

### Setup event management ###

Terrastore server nodes generate events every time a document changes or is removed, and send them to an event bus so that they can be asynchronously processed by registered listeners.

The default event bus implementation is memory-based, meaning that pending events may be lost in case of server failure: it is possible to specify the implementation to use by setting the _--eventBus_ command-line argument; currently, there's an ActiveMQ-based implementation that you can configure by specifying _amq:broker-url_, where _broker-url_ is the actual URL of an external ActiveMQ broker.

### Setup the ensemble configuration file ###

Terrastore server nodes, when joined to a cluster belonging to an ensemble, must be configured with a proper ensemble file specified with the _--ensemble_ command line argument.

For more information about Terrastore ensemble configuration, see sections below.

### Configure server logging ###

Terrastore server logging can be configured by simply tweaking the _terrastore-logback.xml_ file (under the Terrastore home directory), based on [Logback](http://logback.qos.ch/).

## Set up a Terrastore ensemble ##

A Terrastore ensemble provides greater performance and scalability by joining multiple clusters together and spreading data (and computations) over all of them.

In order to set up an ensemble, you just have to configure multiple masters and attached servers, each one by repeating the steps described above.<br />
Every master (and optional passives) with attached servers represents a different "cluster", identified by a _local cluster name_, unique among clusters and shared by attached servers.<br />
In order to discover each other and properly partition data, all servers (in each cluster) must know their own **and** all other cluster names: so, the ensemble clusters layout is currently static, that is, you cannot dynamically add and remove clusters (but you can always add and remove servers inside clusters).<br />
Moreover, each server must know the address of a _seed_ server from each external cluster, used to acquire the external cluster view at start time.<br />
After startup time, external cluster views are acquired by pulling them from the first available node in the cluster, with a frequency determined by the configured discovery algorithm.

Those simple requirements translate into the following Json-based ensemble configuration file:

```
{
"localCluster" : "local cluster name",
"discovery" : {"type" : "fixed", "interval" : milliseconds},
"clusters" : ["local cluster name", "first external cluster", "..."],
"seeds" : {"first external cluster" : "host:node_port", "..." : "..."}
}
```

Where:

  * _localCluster_ is the unique cluster name.
  * _discovery_ is of type _fixed_ and defines the interval time (in milliseconds) for pulling membership from other clusters.
  * _clusters_ is an array of all cluster names (comprising the local one).
  * _seeds_ contains a seed server address for every external cluster.

Is is also possible to use an adaptive discovery algorithm which adjust the frequency based on previous view changes:

```
{
"localCluster" : "local cluster name",
"discovery": {"type" : "adaptive" , "interval" : milliseconds, "baseline" : milliseconds, "increment" : milliseconds, "limit" : milliseconds}
"clusters" : ["local cluster name", "first external cluster", "..."],
"seeds" : {"first external cluster" : "host:node_port", "..." : "..."}
}
```

Where _discovery_ is of type _adaptive_, which starting from a fixed _interval_ (i.e. 3000) adjusts the frequency from a _baseline_ (i.e. 30000) value up to a given _limit_ (i.e. 60000) using the given _increment_ (i.e. 5000).

The ensemble configuration file must be passed (through the startup script) to **every** server, with values properly filled depending on the cluster it belongs to.

## Backup ##

Terrastore documents can be exported and imported by simply issuing export/import requests to any server node.

The export/import is performed per-bucket: however, you can export documents from a given bucket and import them back to a different one.

Documents will be exported to and imported from files remotely located under the _backups_ directory of the server node receiving the export/import request, in order to avoid transferring (possibly large) files: so, if you perform a backup export operation on a given server node, you can import that same backup only by issuing an import request to that same node.

For more information about backup management APIs, go to the [HTTP API guide](HTTP_Client_API.md) and/or [Java API guide](Java_Client_API.md).

## Shutdown ##

All Terrastore master nodes, as well as all Terrastore server nodes, can be safely shutdown by issuing a [ctrl-c](http://en.wikipedia.org/wiki/Control-C) command.