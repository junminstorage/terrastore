# Operations #



A Terrastore cluster is composed by two different kind of processes:

  * **Terrastore master(s)** : providing cluster management and storage services.
  * **Terrastore server(s)** : providing actual access to Terrastore data and operations.

An operational Terrastore cluster is made up of at least one Terrastore master, and at least one Terrastore server.<br />
But in order to achieve enhanced performance and scalability, you can join multiple clusters together in a so called Terrastore ensemble.

Let's see how to set up and manage a Terrastore cluster in all of its different parts.

## Prerequisites ##

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

Once installed, the two masters can be run in whatever order: the first master to be run will start as the active one, while the second as the passive one.

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

The Terrastore installation tool provides the following additional properties to further control master configuration (default value is in parenthesis)

  * master.host (default network interface address) : master host address in single master mode.
  * master.host.1 (default network interface address) : first master host address in active/passive (ha) master mode.
  * master.host.2 (default network interface address) : second master host address in active/passive (ha) master mode.
  * master.server.port (9510) : port used for master-server communication.
  * master.jmx.port (9520) : port used for jmx monitoring (through the Terracotta console).
  * master.ha.port (9530) : port used for master-master communication during active election.

## Set up a Terrastore server node ##

Terrastore server nodes serve requests to clients and provide all actual services.<br />
While you need at least only one server node, adding more server nodes will improve availability, data-access performance and computational power.

### Add a Terrastore server node ###

The Terrastore install tool, again, will help you install and add a node to your cluster:

  * Download the [latest Terrastore distribution](http://code.google.com/p/terrastore/downloads/list) and unpack.
  * Install your server:
    * $> ant -f terrastore-install.xml server -Dinstall.dir=/path\_to\_server
  * Run:
    * $path\_to\_server/bin> sh start.sh --master _host_:9510 --httpHost _address_ --httpPort _port_ --nodeHost _address_ --nodePort _port_ --nodeTimeout _timeout_ --httpThreads _threads_ --workerThreads _threads_ --failoverRetries _retries_ --failoverInterval _interval_ --allowedOrigins _hosts_ --ensemble _ensembleConfig_

The _--master_ command-line argument is the only mandatory parameter, pointing to your (currently active) master node, or to a comma-separated list of master hosts; addresses must be in the form _host_:_port_, where _9510_ (above) is the standard master port.

All other arguments are optional (default value is in parenthesis):
  * httpHost (127.0.0.1) : The server http host address to listen to for client requests.
  * httpPort (8080) : The server http port to listen to for client requests.
  * nodeHost (same as httpHost) : The address to listen to for other nodes requests.
  * nodePort (8226) : The port to listen to for other nodes requests.
  * nodeTimeout (1000) : Timeout in milliseconds for node-to-node communication.
  * httpThreads (100) : Number of http threads servicing requests.
  * workerThreads (depends on available processors) : Number of worker threads used by the node to execute operations outside of the http request thread.
  * failoverRetries (0) : Number of retries In case of communication errors between cluster nodes (such as request timeouts).
  * failoverInterval (0) : Number of milliseconds to wait between failover retries.
  * allowedOrigins (none): Comma-separated list of client hosts to allow access to, for Cross Origin Resource Sharing support, see below.
  * ensemble (none) : Terrastore ensemble configuration file, see sections below.

You should set at least the _--httpHost_ parameter, unless you want a cluster working only on your local node.

### Setup Cross Origin Resource Sharing support ###

[Cross Origin Resource Sharing](http://www.w3.org/TR/cors/) (CORS) is a W3C specification describing a mechanism to allow client-side remote access to server resources: Terrastore CORS implementation allows browser-based applications to access Terrastore servers even from remote machines.

Terrastore CORS implementation works with most modern browsers: it has been currently tested with FireFox 3.5+, Safari 5+ and Chrome.

By default, CORS is **enabled** on Terrastore for all hosts: so, your client-side browser applications will be able to access remote Terrastore servers with no additional configurations.

To disable/restrict CORS support, either for security or performance reasons, just startup your Terrastore servers with the _--allowedOrigins_ command-line parameter, either followed by the "!" symbol to completely disable CORS support, or by the comma-separated list of URLs allowed to access Terrastore resources, i.e. "--allowedOrigins http://192.168.1.1:80,http://192.168.1.2:80".

To enable access for all clients, you can also pass the asterisk wildcard.

### Server logging configuration ###

Terrastore server logging can be configured by simply tweaking the _terrastore-logback.xml_ file (under the Terrastore home directory), based on [Logback](http://logback.qos.ch/).

## Set up a Terrastore ensemble ##

A Terrastore ensemble provides greater performance and scalability by joining multiple clusters together and spread data (and computations) over all of them.

In order to set up an ensemble, you just have to configure multiple masters and attached servers, each one by repeating the steps described above.<br />
Every master (and optional passives) with attached servers represents a different "cluster", identified by a _local cluster name_, unique among clusters and shared by attached servers.<br />
In order to discover each other and properly partition data, all servers (in each cluster) must know their own **and** all other cluster names: so, the ensemble clusters layout is currently static, that is, you cannot dynamically add and remove clusters (but you can always add and remove servers inside clusters).<br />
Moreover, each server must know the address of a _seed_ server from each external cluster, used to acquire membership at start time.<br />
These simple requirements translate in the following Json-based ensemble configuration file:
```
{
"localCluster" : "local cluster name",
"discoveryInterval" : time_in_milliseconds_to_refresh_membership,
"clusters" : ["local cluster name", "first external cluster", "..."],
"seeds" : {"first external cluster" : "host:node_port", "..." : "..."}
}
```
Where:
  * _localCluster_ is the unique cluster name.
  * _discoveryInterval_ is the interval time (in milliseconds) for pulling membership from other clusters.
  * _clusters_ is an array of all cluster names (comprising the local one).
  * _seeds_ contains a seed server address for every external cluster.
The ensemble configuration file must be passed (through the startup script) to every server, with values properly filled depending on the cluster it belongs to.

## Backup ##

Terrastore documents can be exported and imported by simply issuing export/import requests to any server node.

The export/import is performed per-bucket: however, you can export documents from a given bucket and import them back to a different one.

Documents will be exported to and imported from files remotely located under the _backups_ directory of the server node receiving the export/import request, in order to avoid transferring (possibly large) files: so, if you perform a backup export operation on a given server node, you can import that same backup only by issuing an import request to that same node.

For more information about backup management APIs, go to the [HTTP API guide](HTTP_Client_API.md).

## Shutdown ##

All Terrastore master nodes, as well as all Terrastore server nodes, can be safely shutdown by issuing a [ctrl-c](http://en.wikipedia.org/wiki/Control-C) command.