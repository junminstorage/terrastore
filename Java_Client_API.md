# Java Client API #



## Installation ##

If your application is based on [Maven](http://maven.apache.org/), you can use the Terrastore repository in order to properly configure the Java Client dependency.

First, add the following repository declaration:
```
<repository>
    <id>terrastore-repo</id>
    <name>Terrastore Repository</name>
    <url>http://m2.terrastore.googlecode.com/hg/repo</url>
</repository>
```

Then add the Java Client dependency:
```
<dependency>
    <groupId>terrastore</groupId>
    <artifactId>terrastore-javaclient</artifactId>
    <version>PROPER_VERSION</version>
</dependency>
```

If you don't use Maven, just manually download the Java Client jar and related dependencies.

## Introduction ##

Terrastore provides a ready-to-use Java API for accessing and managing buckets and related key/value entries (also referred as documents).

This API essentially wraps the Terrastore HTTP client API, providing a smooth fluent-style API.<br />
Refer to the documentation of the [Terrastore HTTP client API](HTTP_Client_API.md) for a more detailed explanation of all operations.

The API is exposed through the `terrastore.client.TerrastoreClient` object and relies on the Jackson Java library to serialize/deserialize your objects/entities to and from JSON.
The default serialization method can be overridden for specific object types by implementing the `terrastore.client.mapping.JsonObjectDescriptor` interface, which are then injected into your TerrastoreClient upon construction.

TerrastoreClient instances are thread-safe, so you do not need to have more than one instance.

## Creating a TerrastoreClient instance ##

TerrastoreClient instances can be created by either passing a single Terrastore server URL or a `terrastore.client.connection.HostManager` object defining the actual server you'll be connecting to. <br />
In both cases, you will have to supply a mandatory `terrastore.client.connection.ConnectionFactory` instance, defining how to actually connect to servers, and an optional list of `terrastore.client.mapping.JsonObjectDescriptor`s, defining the serialization strategy for your Java objects sent over the wire to Terrastore.

To use the TerrastoreClient with the default object serialization mechanism, use:

```
TerrastoreClient client = new TerrastoreClient("http://localhost:8080", new HTTPConnectionFactory());
```

Here, `terrastore.client.connection.resteasy.HTTPConnectionFactory` is the ConnectionFactory implementation based on [RESTEasy](http://www.jboss.org/resteasy), while the default serialization strategy relies on [Jackson](http://jackson.codehaus.org/).


If you need different or more complex serialization strategies, use:

```
TerrastoreClient client = new TerrastoreClient("http://localhost:8080", new HTTPConnectionFactory(), jsonObjectDescriptors);
```

The jsonObjectDescriptors argument expects a `List<? extends JsonObjectDescriptor<?>>`, in other words a `List<JsonObjectDescriptor>`, that contains your custom object descriptors for serialization and deserialization.

But, you may also want to provide an HostManager implementation to define more complex/resilient host management strategies. <br />
We provide the `terrastore.client.connection.OrderedHostManager` implementation, which relies on a list of server hosts and connects to the first working one, switching to the next one in case of failures, here's how to set it up:

```
TerrastoreClient client = new TerrastoreClient(
  new OrderedHostManager(Arrays.asList(new String[]{"http://192.168.1.1:8000", "http://192.168.1.2:8000"})), 
  new HTTPConnectionFactory());
```

Please note it doesn't provide transparent failover: so, if any operation fails, you have to retry it and the client will switch hosts and try the next one.

## Bucket management ##

All keys/values in Terrastore resides in buckets, which can be roughly looked upon as namespaces, collections or tables depending on where you come from.

Buckets are implicitly created when needed, which means that you never create your buckets before storing values in it. That happens automatically the first time a value is written to it.

### Listing available buckets ###

You can retrieve a set of all currently existing bucket names on the Terrastore server by calling:

```
Set<String> availableBuckets = client.buckets().list()
```

### Removing Buckets ###

A Bucket, and documents stored inside, can be removed by calling:

```
client.bucket("bucketname").remove();
```

The remove-command is idempotent, which means that it can safely be called multiple times without the existence of an actual bucket to remove.

### Getting all key/values from a bucket ###

You can retrieve the entire contents of a bucket by calling:

```
Map<String, MyEntity> bucketContents = client.bucket("bucketname").values().get(MyEntity.class);
```

It is, however, not recommended to perform this operation on large datasets, as it would be rather costly. But you can specify a limit as to how many documents to retrieve by adding a limit to the query:

```
client.bucket("bucketname").values().limit(50).get(MyEntity.class);
```

## Document management ##

Documents are stored in buckets, using keys to identify them. You can essentially view buckets as a somewhat more advanced HashMap, where keys and documents exist as Key/Value pairs.

### Adding and updating documents ###

Adding and updating document is done using the same syntax. To store or update a document/object under the key "abc" in the bucket "myentities" use:

```
client.bucket("myentities").key("abc").put(myEntityInstance);
```

If they key "abc" does not previously exist in this bucket, it will be created. If such a key already were to exist then the previous document stored under it would be replaced by the myEntityInstance value.

You can also put several documents in bulk:

```
client.bucket("myentities").bulk().put(new Values(myEntities));
```

Moreover, you can also execute a conditional update, which will put the document inside the bucket only if the already existent one satisfies a given predicate condition:

```
client.bucket("myentities").key("abc").conditional("your_condition").put(myEntityInstance);
```

Predicate conditions are explained below in the section about querying documents.

### Retrieving documents ###

Just as you would retrieve a single value from a Map, you retrieve documents from Terrastore by referring to the key under which it is stored. To retrieve the document we stored in the previous section we would use:

```
MyEntity myEntityInstance = client.bucket("myentities").key("abc").get(MyEntity.class);
```

The _get_ method takes a class reference to be able to tell the client which java type to deserialize the retrieved document to.

You can also get several documents in bulk:

```
client.bucket("myentities").bulk().get(Sets.hash("key1", "key2", "key3"), MyEntity.class)
```

Moreover, you can also execute a conditional get, which will get the document from the bucket only if it satisfies a given predicate condition:

```
client.bucket("myentities").key("abc").conditional("your_condition").get(MyEntity.class);
```

Predicate conditions are explained below in the section about querying documents.

### Removing a document ###

Keeping in line with the Map-like interface, documents (along with their keys) are removed using:

```
client.bucket("myentities").key("abc").remove();
```

### Removing multiple documents using ranges ###

Multiple documents can be deleted by specifying instead their key range.

If we imagine that we have a bucket with documents stored under five keys named "key1" to"key5", we can remove the keys and documents for "key3" through "key5" by executing:

```
Set<String> removedKeys = client.bucket("myentities").range("lexical-asc").from("key3").to("key5").remove(MyEntity.class);
```

Terrastore will then use the "lexical-asc" comparator algorithm to compare the keys and determine that "key3", "key4" and "key5" are within the specified range, then remove them and return the set of actually removed keys.

## Querying for documents ##

Apart from retrieving a single document by referring to its key, there are a number of  other ways that a document or sets of documents can be retrieved.<br />
Documents can be queried by using ranges of keys or specifying criterias on the internal structure of documents using predicates.

### Retrieving documents using predicates ###

By performing a predicate query on a bucket, you can retrieve all documents that matches a specific criteria; Terrastore currently supports [JXPath](http://commons.apache.org/jxpath/) and JavaScript predicates.<br />

Here are a few examples using JXPath predicates, but the same applies to other kind of predicates: just change the predicate string.

A simple first example would  be to query for all documents that has a field named "firstName":

```
Map<String, Person> personsWithFirstName = client.bucket("persons").predicate("jxpath:/firstName").get(Person.class);
```

This statement will retrieve all matching documents and return a Map of String(document key) and Person(the actual document/object).

If we want to be more specific, we can query for all documents that has a field named "firstName" and whose value is "Harry" by stating:

```
Map<String, Person> personsNamedHarry = client.bucket.("persons").predicate("jxpath:/firstName[.='Harry']").get(Person.class);
```

### Retrieving documents using ranges ###

Range queries operate on the keys of a bucket, allowing you to query for all documents whose keys are within a certain range.

If we imagine that we have a bucket with documents stored under five keys named "key1" to"key5", we can retrieve the keys and documents for "key3" through "key5" by executing:

```
Map<String, MyEntity> docs = client.bucket("myentities").range("lexical-asc").from("key3").to("key5").get(MyEntity.class);
```

Terrastore will then use the "lexical-asc" comparator algorithm to compare the keys and determine that "key3", "key4" and "key5" are within the specified range.

### Retrieving documents using ranges and predicates ###

Range queries and predicate queries can be combined by specifying both a range and a predicate.

If we imagine a bucket containing documents stored under keys "person\_a" to "person\_z", the following query would retrieve all documents stored under keys between "person\_f" to "person\_s" that has a value for the field "firstName":

```
Map<String, Person> persons = client.bucket("persons").range("lexical-asc").from("person_f").to("person_s").predicate("jxpath:/firstName").get(Person.class);
```

## Map/Reduce queries ##

### Building a query ###

Map/Reduce queries are performed using server-side functions and aggregators to process and aggregate data. Unlike most other operations in the Terrastore Java Client API, such queries are constructed separately and passed in to client API as a whole.
Consider the following:

```
MapReduceQuery query = new MapReduceQuery()
    .task(new Task().mapper("size").reducer("size").timeout(10000));
```

This statement is specifying a Map/Reduce query that will use the server-side function "size" to map documents, and then the server-side aggregator "size" to reduce the mapped documents. The query also specifies a time-out of 10000 milliseconds. Any execution of a query that exceeds its timeout limit will be tentatively aborted.

The _size_-function used in this example is included in the Terrastore distribution.

### Using ranges ###

Ranges can be also be used in combination with Map/Reduce in order to perform queries on a subset of documents contained within a bucket.
In this case they are executed prior to the map-step of the query, with the effect that the map-function is never applied documents falling outside of the specified range.

Ranges are applied directly to the query, as follows:

```
MapReduceQuery query = new MapReduceQuery()
    .range(new Range().from("bbbcccddd").to("eeefffggg").comparator("lexical-asc"))
    .task(new Task().mapper("size").reducer("size").timeout(10000));
```

Just as with regular Range-queries, specifying an higher boundary of the range with to() is optional.

### Executing a query ###

To execute a query, we call the mapReduce-method of a bucket, like so:

```
String result = client.bucket("articles").mapReduce(query).execute(String.class);
```

Just like any operation that retrieves data, the argument to the execute()-method takes the expected return type.
Passing String.class, as in this example, will return the JSON document as a String, but any type of bean that matches the expected JSON format will do.

Note that the output format of Map/Reduce queries will depend entirely on the output of the reduce-function.

## Server-side updates ##

Server-side updates provide a way to atomically execute update operations on a whole document, such as changing only a bunch of document properties (without overwriting the whole document), or performing calculations over document data.

### Executing server-side update functions ###

You can execute update functions on your documents by providing the key of the document to update, the name of the server-side update function, the update parameters and a timeout after which the update operation will be killed in order to avoid locking the document for too much.<br />
Terrastore provides the following server-side update functions: _counter_, to implement atomic counters, and _js-updater_ to pass a dynamic javascript function.

Here is an example using the _counter_ update function:

```
Map<String, Object> counters = new HashMap<String, Object>();
counters.put("friendsNr", "1");

Person personWithUpdatedNumberOfFriends = client.bucket("persons").key("1").update("counter").parameters(counters).timeOut(1000L).executeAndGet(Person.class));
```

### Executing server-side merge updates ###

Merge updates let clients update documents by providing a "merge descriptor", as described in the [Developers guide](Developers_Guide.md).

Here is an example:

```
Map<String, Object> newFields = new HashMap<String, Object>();
newFields.put("age", 26);
newFields.put("address", "Terrastore City");

MergeDescriptor descriptor = new MergeDescriptor().add(newFields);

Person personWithNewFields = client.bucket("persons").key("1").merge(descriptor).executeAndGet(Person.class));
```

## Backup Import/Export ##

Terrastore provides a way to easily backup the contents of a whole bucket into the filesystem of the server node you requested the backup to, and later import the backup again.

### Export ###

In order to execute a backup export, you only need to provide the filename of the backup and a secret key used to avoid unintentionally calling the backup import/export:

```
client.bucket("persons").backup().file("persons.bak").secretKey("SECRET-KEY").executeExport();
```

The command above will create a _persons.bak_ file into the _backups_ directory of the node which received the request, containing **all** bucket documents.

### Import ###

In order to execute a backup import, you only need to provide the filename of the backup and a secret key used to avoid unintentionally calling the backup import/export:

```
client.bucket("persons").backup().file("persons.bak").secretKey("SECRET-KEY").executeImport();
```

The command above will read a _persons.bak_ file from the _backups_ directory of the node which received the request, and import **all** contained documents, transparently distributing them among the cluster nodes.

## Statistics ##

Terrastore statistics inform users about several aspects of the Terrastore cluster and contained data.

### Cluster statistics ###

You can retrieve Terrastore cluster statistics, providing information about clusters and related servers, as follows:

```
ClusterStats stats = client.stats().cluster();
```