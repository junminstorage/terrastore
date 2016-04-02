# Getting Started #



A Terrastore distributed cluster is composed by two different kind of processes:

  * **Terrastore master(s)**, providing cluster management and storage services.
  * **Terrastore server(s)**, providing actual access to Terrastore data and operations.

Clients will only need to interact with Terrastore servers.

A **Terrastore base installation** is composed by a single cluster made up of one active master, one or more passive masters (optional), and one or more attached servers.<br />
For enhanced performance and scalability, you can configure a **Terrastore multi-cluster ensemble**, composed by multiple clusters, each one with its own active master (and optional set of passive masters), and one or more attached servers.

## Terrastore Quickstart installation ##

Terrastore quickstart installation is the fastest way to get a base Terrastore cluster up and running and make you started.<br />
Once you have installed Java 1.6 and Apache Ant, and unpacked the [latest Terrastore distribution](http://code.google.com/p/terrastore/downloads/list), just run the following command:
```
$> ant -f terrastore-install.xml quickstart -Dquickstart.dir=/install_dir
```
In one single command, it will install into the provided directory a cluster made up of one master and two servers, and start it!<br />
Servers will listen on port 8000 and 8001.<br />
Finally, if you issue the quickstart command multiple times against the same directory, it will reuse the previously installed cluster and start it again.

## Terrastore Base installation ##

To install a single Terrastore cluster with a single active master (the typical development configuration) and two Terrastore servers, you will only need to:

  * Install Java 1.6.
  * Install Apache Ant.
  * Download the [latest Terrastore distribution](http://code.google.com/p/terrastore/downloads/list) and unpack.
  * Install your **Terrastore master**:
```
$> ant -f terrastore-install.xml single-master -Dinstall.dir=/path_to_master
```
  * Install your first **Terrastore server**:
```
$> ant -f terrastore-install.xml server -Dinstall.dir=/path_to_server_1
```
  * Install your second **Terrastore server**:
```
$> ant -f terrastore-install.xml server -Dinstall.dir=/path_to_server_2
```
  * Start master and servers:
```
$path_to_master/bin> sh start.sh

$path_to_server_1/bin> sh start.sh --master _host_:9510 --httpHost _host_ --httpPort _httpPort_ --nodePort _nodePort_

$path_to_server_2/bin> sh start.sh --master _host_:9510 --httpHost _host_ --httpPort _httpPort_ --nodePort _nodePort_
```

The 9510 port is the default master port (you can change that when installing the master). The _host_ and _httpPort_ will be used by clients to communicate with your server, while the _nodePort_ will be used for internal cluster communication (so it may be hidden from external clients).

## Terrastore Ensemble installation ##

To install a Terrastore multi-cluster ensemble you just need to repeat the steps above for each cluster you want to be part of the ensemble. So, provided you want two clusters, each one with a master and a server:

  * Install the Terrastore master for your **first cluster**:
```
$> ant -f terrastore-install.xml single-master -Dinstall.dir=/path_to_master_1
```
  * Install the attached Terrastore server:
```
$> ant -f terrastore-install.xml server -Dinstall.dir=/path_to_server_1
```
  * Install the Terrastore master for your **second cluster**:
```
$> ant -f terrastore-install.xml single-master -Dinstall.dir=/path_to_master_2
```
  * Install the attached Terrastore server:
```
$> ant -f terrastore-install.xml server -Dinstall.dir=/path_to_server_2
```

Now, you have to configure the ensemble by writing a simple Json-based configuration file which must be passed to **every server**.<br />
The configuration file parameters slightly differ depending on the cluster the server belongs to.<br />
Here is how the ensemble configuration for the first server (hence first cluster) would look like:
```
{
"localCluster" : "cluster1",
"discovery" : {"type" : "fixed", "interval" : 5000},
"clusters" : ["cluster1", "cluster2"],
"seeds" : {"cluster2" : "192.168.1.11:6000"}
}
```
And here is how the ensemble configuration for the second server (hence second cluster) would look like:
```
{
"localCluster" : "cluster2",
"discovery" : {"type" : "fixed", "interval" : 5000},
"clusters" : ["cluster1", "cluster2"],
"seeds" : {"cluster1" : "192.168.1.10:6000"}
}
```
As you may see, they only differ for the _local cluster name_, which uniquely identify the name of the cluster they belong to, and the _seeds_ section, which refers _host_ and _node port_ of whatever node belonging to external clusters used to bootstrap the discovery process.

Now you can start your masters and servers (addresses are taken from configuration above):

```
$path_to_master_1/bin> sh start.sh

$path_to_master_2/bin> sh start.sh

$path_to_server_1/bin> sh start.sh --master 192.168.1.1:9510 --httpHost 192.168.1.10 --httpPort 8080 --nodePort 6000 --ensemble cluster1.json

$path_to_server_2/bin> sh start.sh --master 192.168.1.2:9510 --httpHost 192.168.1.11 --httpPort 8080 --nodePort 6000 --ensemble cluster2.json
```

## Get started with Curl ##

You can easily try your Terrastore cluster with the excellent [curl](http://curl.haxx.se/) command line tool:

  * Add a document:
```
curl -v -X PUT -H "Content-Type: application/json" -d "{\"test\" : \"test\"}" http://192.168.1.10:8080/bucket/test
```
  * Get the stored document (on another node):
```
curl -X GET -H "Content-Type: application/json" http://192.168.1.11:8080/bucket/test
```

## Get started with Java Client APIs ##

Terrastore provides easy and intuitive Java client APIs with base mapping capabilities between your Java objects and JSON documents.<br />
[Download](http://code.google.com/p/terrastore/downloads/list) the latest Java client distribution and give it a try:

  * Connect to a Terrastore server:
```
TerrastoreClient client = new TerrastoreClient("http://192.168.1.10:8080", new HTTPConnectionFactory());
```
  * Add a document provided as a Java object, which will be automatically turned into JSON by using [Jackson](http://jackson.codehaus.org/) under the cover (you can customize the conversion process too, by providing custom descriptors):
```
client.bucket("bucket").key("key").put(myDocument);
```
  * Get the stored document as a Java object:
```
MyObject myDocument = client.bucket("bucket").key("key").get(MyObject.class);
```

## Want to know more? ##

Jump to the [Server Guide](Server_Guide.md) for more detailed information about how to set up your Terrastore cluster, or go straight to the [HTTP API](HTTP_Client_API.md) section!