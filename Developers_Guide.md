# Developers Guide #



## Events ##

Terrastore provides an event publishing and processing framework, made up of the following components:
  * An event bus, completely managed by Terrastore itself and used for publishing events and delivering them for later processing.
  * One or more event listeners, developed by Terrastore users with the aim of providing custom event processing logic.

### Event publishing ###

Terrastore publishes the following events:
  * Value changed, published when a document is:
    * Put.
    * Updated via an update function.
  * Value removed, published when a document is:
    * Removed.

Events are atomically published with the related action which caused the event, as listed above: Terrastore guarantees that events related to the same key (into the same bucket) will be published and processed in FIFO order, in order to preserve per-document consistency.

Event publishing and delivering happens through the previously cited event bus; Terrastore provides two different event bus implementations:
  * Memory-based event bus, the default one: events are synchronously published in memory queues and asynchronously processed by configured listeners.
  * ActiveMQ-based event bus: events are synchronously published to an external [ActiveMQ](http://activemq.apache.org) message broker on message queues named after the terrastore._bucket_ pattern, and asynchronously processed by configured listeners.

In order to switch from the default memory bus to the ActiveMQ one, you only have to provide the following startup parameters to each Terrastore server: "--eventBus amq:_broker-url_", replacing _broker-url_ with the URL of the ActiveMQ broker you want to publish to. Please note that you must configure the new event bus on **all** your Terrastore servers.

### Event processing ###

Events are processed by user-defined event listeners.
Event listeners can be **passive**, meaning that they only process events without actually changing any contents, or they can be **active**, meaning that they generate asynchronous _actions_ aimed at changing database contents.

An event listener receives events related to buckets it _observes_: once an event is received, the listener can access the event contents, such as the old document value (if any) and the new one (again, if any), do its own processing, such as connecting with external systems, and rising asynchronous actions through the _action executor_.
Actions can either put or remove documents, eventually rising other events and creating a chain of events processing and storage.

### Writing custom event listeners ###

In order to implement and configure an event listener, just follow the simple steps below.

First, implement the Terrastore-specific _terrastore.event.EventListener_ interface:
```
/**
 * Observe buckets and react to events related to key/value pairs.
 */
public interface EventListener extends Serializable {

    /**
     * Determine if this listener is interested into events happening in the given bucket.
     *
     * @param bucket The bucket to observe.
     * @return True if interested (observing) the given bucket, false otherwise.
     */
    public boolean observes(String bucket);

    /**
     * React when a given value changes.
     *
     * @param event The {@link Event} object carrying information about the current event.
     * @param executor The {@link ActionExecutor} for eventually creating and executing {@link Action}s.
     */
    public void onValueChanged(Event event, ActionExecutor executor);

    /**
     * React when a given value is removed.
     *
     * @param event The {@link Event} object carrying information about the current event.
     * @param executor The {@link ActionExecutor} for eventually creating and executing {@link Action}s.
     */
    public void onValueRemoved(Event event, ActionExecutor executor);

    /**
     * Callback to initialize things on listener registration.
     */
    public void init();

    /**
     * Callback to cleanup things on {@link EventBus} shutdown.
     */
    public void cleanup();
}
```

Then annotate your listener with _terrastore.annotation.AutoDetect_:
```
@AutoDetect(name="MyEventListener", order="1")
public class MyEventListener implements EventListener {
    // ...
}
```
The _name_ attribute must be a unique listener name, while _order_ defines listeners precedence (lower order means higher precedence).

Finally, deploy your event listener as a jar into the Terrastore server _libs_ directory and restart the server.

## Data partitioning ##

Terrastore documents are partitioned among clusters and related server nodes using by default a consistent hashing scheme based on keys: while this is great to automatically keep balanced data distribution, users may want to implement custom partitioning strategies to suit their own data locality needs, for example to assign more data to bigger machines, or to place data near other external systems needing to access that same data.

### Writing custom partition strategies ###

Terrastore implements a two-level data partitioning scheme: data is first assigned to a single cluster (if more are provided through an ensemble), and then partitioned among the owning cluster servers.
So, Terrastore provides two different interfaces for custom partitioning strategies:
```
public interface CustomEnsemblePartitionerStrategy {

    /**
     * Get the {@link Cluster} object where to place the given bucket.
     *
     * @param bucket The bucket to place.
     * @return The {@link Cluster} object where to place the given bucket.
     */
    public Cluster getClusterFor(String bucket);

    /**
     * Get the {@link Cluster} object where to place the given key (under the given bucket).
     *
     * @param bucket The bucket holding the key to place.
     * @param key The key to place
     * @return The {@link Cluster} object where to place the given key.
     */
    public Cluster getClusterFor(String bucket, String key);
}
```
The interface above needs to be implemented only if you need a cluster ensemble **and** you want to provide custom partitioning for that: it just requires to create and return a _Cluster_ object named after the cluster you want to assign a bucket and/or key to.
For intra-cluster partitioning, this is the interface to implement:
```
public interface CustomClusterPartitionerStrategy {

    /**
     * Get the {@link Node} object where to place the given bucket.<br>
     * Be aware: the returned node must belong to the given cluster.
     *
     * @param cluster The name of the cluster holding the node.
     * @param bucket The bucket to place.
     * @return The {@link Node} object where to place the given bucket.
     */
    public Node getNodeFor(String cluster, String bucket);

    /**
     * Get the {@link Node} object where to place the given key (under the given bucket).<br>
     * Be aware: the returned node must belong to the given cluster.
     *
     * @param cluster The name of the cluster holding the node.
     * @param bucket The bucket holding the key to place.
     * @param key The key to place
     * @return The {@link Node} object where to place the given key.
     */
    public Node getNodeFor(String cluster, String bucket, String key);
}
```
Here, you have to create and return a _Node_ object configured with host and **node** port of the server you want to assign the bucket and/or key to.

You don't need to implement both _CustomEnsemblePartitionerStrategy_ and _CustomClusterPartitionerStrategy_ interfaces: just implement the one(s) for which you want to provide custom partitioning.
Once implemented, just annotate them with _terrastore.annotation.AutoDetect_, put them in a jar into **every** Terrastore server _libs_ directory, restart the servers and they will be automatically wired and deployed!

## Data management ##

Terrastore provides advanced querying mechanisms:

  * Conditional Get.
  * Predicate Query.
  * Range Query.
  * Map/Reduce.

As well as advanced update mechanisms:

  * Update functions.
  * Merge update.

### Conditional Get ###

Conditional get operations are used to read a given document, identified by its bucket/key pair, only if it satisfies a given _condition_.

The condition may be based on either the document key, content or both.
Developers can use Terrastore built-in conditions, or easily implement their own as described below.

### Predicate Query ###

Predicate queries are used to read all documents from a bucket that satisfy a given _condition_: while this kind of query requires a full bucket scan, each server node only scans its own documents, in parallel with other nodes, so you should run predicate queries on either limited data sets, or huge data sets distributed among several server nodes (the more, the better).

The condition may be based on either the document key, content or both.
Developers can use Terrastore built-in conditions, or easily implement their own as described below.

### Range Query/Delete ###

Range queries are used to read a set of documents from a bucket, calculated by comparing keys with a dynamic user-specified _comparator_; moreover, users can specify an optional _condition_ that must be satisfied by each document.
Key ranges are pre-computed, using the specified comparator, on the node which received the request: so they work on a snapshot of the actual keys, which may contain stale data: in order to control data freshness, users must specify a time-to-live, defining the snapshot max age. The first range query will be slower (because of the snapshot computation), but subsequent ones will be much faster unless the snapshot expires (due to the user-specifed time-to-live) and so gets recomputed.

The comparator is always based on keys.
The condition may be based on either the document key, content or both.
Developers can use Terrastore built-in comparators and conditions, or easily implement their own as described below.

Range deletes are based on the same exact concepts above: the only difference, as the name implies, is that they perform a delete operation over documents in range.

### Map/Reduce ###

Map/Reduce is an advanced data querying/aggregation mechanism widely used in modern distributed systems due to its parallel processing capabilities.
A map/reduce query is basically performed as a two-step task: a parallel map phase, executed on each node and taking as input a <key,value> pair and returning multiple <key,value> pairs as intermediate values; and a reduce phase, taking all intermediate values and returning a final aggregated value.

Terrastore adapts the two phases above to its own data and distribution model as follows.
When the map/reduce query is received by a server node, which we call the _originator_ node, it gets split into several **mapper** _functions_ sent to each node holding the set of documents to be queried/processed: mapper functions are executed in parallel on each node, taking as input each document to query and returning a generic map of <key,value> pairs; once finished the map phase, each node runs a (parallel, again) **combiner** phase, where partial mapper outputs are combined by an _aggregator_ function.
Finally, each node result is returned back to the originator node which will run the final **reducer** phase, where all outputs will be aggregated by an _aggregator_ function which can be the same as the combiner one, or a different one.

Map/Reduce queries are performed over all documents belonging to a bucket, or over a specified range of documents, using a given _comparator_.

Developers can use Terrastore built-in comparators, functions and aggregators, or easily implement their own as described below.

### Update functions ###

Update functions perform a server-side _function_ over a document identified by its bucket/key pair: every change performed by the function over the document content will be atomic and isolated from other updates.
Due to is atomicity properties, each update must terminate in a given amount of time specified by the user: otherwise it will be discarded.

Developers can use Terrastore built-in functions, or easily implement their own as described below.

### Merge update ###

Merge updates perform a server-side merge over a document identified by its bucket/key pair, which is atomic and isolated from other updates.
The merge operation is described by a user-provided merge document, whose special syntax specifies all update operations to perform, as follows:

  * The "`*`" field contains an object with fields/values to replace. Example: {"field":"value1"} -> {"`*`":{"field":"value2"}} -> {"field":"value2"}
  * The "+" field contains an object with fields/values to add. Example: {"field1":"value1"} -> {"+":{"field2":"value2"}} -> {"field1":"value1","field2":"value2"}
  * The "-" field contains an array with field names to remove. Example: {"field":"value"} -> {"-":["field"]} -> {}
  * Fields whose name refers to an array will add contained items if the first item is a "+" sign. Example: {"field":["value1"]} -> {"field":["+","value2"]} ->  {"field":["value1","value2"]}
  * Fields whose name refers to an array will remove contained items if the first item is a "-" sign (only works with strings, meaning you can't remove other type of items from arrays). Example: {"field":["value"]} -> {"field":["-","value"]} ->  {"field":[.md](.md)}
  * Fields whose name refers to an object will evaluate this rules over the contained object. Example: {"field":{"field1":"value1"}} -> {"field":{"+":{"field2":"value2"}}} -> {"field":{"field1":"value1","field2":"value2"}}

Merge updates are fast and memory efficient, so they're the suggested way to perform updates when the data to be updated is large and/or can be described through the merge syntax above.

### Building blocks ###

Terrastore query and update operations are based on the following building blocks:

  * Comparators.
  * Conditions.
  * Functions.
  * Aggregators.

Terrastore provides its own built-in implementations, and developers can easily write their own without having to build everything from sources.

#### Built-in implementations ####

##### If condition #####

The _If_ condition provides an easy way to express common conditional operations.

The _absent_ condition provides a way to check for document presence, usually for conditional put operations:
```
PUT /myBucket/myDocument?predicate=if:absent()
```

The _matches_ condition provides a way to check if a document attribute matches a given value, for example in conditional get operations:
```
GET /myBucket/myDocument?predicate=if:matches(author,Sergio Bossa)
```

##### JXPath condition #####

The _JXPath_ condition selects documents whose contents satisfy a given XPath condition, and is invoked by specifying the _jxpath_ expression type.
Here is an example, applying the condition to all documents in the _myBucket_ bucket:
```
GET /myBucket/predicate?predicate=jxpath:/author[.='Sergio Bossa']
```
The condition above would satisfy the following json document:
```
{"project" : "Terrastore", "author" : "Sergio Bossa"}
```
For more information about JXPath, see http://commons.apache.org/jxpath.

##### Javascript condition #####

The _Javascript_ condition selects documents whose key or json value satisfies a given Javascript conditional expression, and is invoked by specifying the _js_ expression type.
Here is an example with a condition applied on the key of all documents in the _myBucket_ bucket:
```
GET /myBucket/predicate?predicate=js:key.indexOf('myKey')==0
```
The condition above would satisfy any document whose key starts with _myKey_.
Also, you can apply a condition on the json document value:
```
GET /myBucket/predicate?predicate=js:value.author=='Sergio Bossa'
```
The condition above would satisfy the following json document:
```
{"project" : "Terrastore", "author" : "Sergio Bossa"}
```

##### Merge update function #####

The _Merge_ update function merges a stored document with a user-specified document, and is invoked by specifying the _merge_ function name.
Here is an example:
```
POST /myBucket/myKey/update?function=merge&timeout=10000
{"value2" : "merged"}
```
The function above, applied to the following document:
```
{"value1" : "original"}
```
Would result in the following updated document:
```
{"value1" : "original", "value2" : "merged"}
```

##### Counter update function #####

The _Counter_ update function atomically increments/decrements one or more counter values, and is invoked by specifying the _counter_ function name.
Here is an example, incrementing a string counter and decrementing a number counter:
```
POST /myBucket/myKey/update?function=counter&timeout=10000
{"stringCounter" : "1", "numberCounter" : -1}
```
The function above, applied to the following document:
```
{"stringCounter" : "1", "numberCounter" : 1}
```
Would result in the following updated document:
```
{"stringCounter" : "2", "numberCounter" : 0}
```

##### Javascript update function #####

The _Javascript_ update function applies a user-specified Javascript function to a given document, and is invoked by specifying the _js_ function name.
The Javascript function must be passed inside the json parameters under the _function_ key, and must have the following signature (where _key_ is the document key, _value_ is the document json structure and params is the json structure passed by the user for parameters):
```
function(key, value, params)
```
Moreover, it must return a valid json structure.
Here is an example:
```
POST /myBucket/myKey/update?function=js&timeout=10000
{"function" : "function(key, value, params) {value.author = 'Sergio Bossa'; return value;}"}
```
The function above, applied to the following document:
```
{"project" : "Terrastore", "author" : ""}
```
Would result in the following updated document:
```
{"project" : "Terrastore", "author" : "Sergio Bossa"}
```

##### SizeMapper and SizeAggregator #####

The _SizeMapper_ and _SizeAggregator_ are used for calculating a bucket size (or the size of a range of documents inside a bucket) with a map/reduce query.

Here is an example of map/reduce request, with related descriptor:
```
POST /myBucket/mapReduce
{"task" : {"mapper" : "size", "reducer" : "size", "timeout" : 60000}}
```
As you can see, _SizeMapper_ and _SizeAggregator_ are invoked by using _size_ as mapper and reducer name.

##### KeysMapper and KeysAggregator #####

The _KeysMapper_ and _KeysAggregator_ are used for aggregating (and returning) all keys of a given bucket (or of a range of documents inside a bucket) with a map/reduce query.

Here is an example of map/reduce request, with related descriptor:
```
POST /myBucket/mapReduce
{"task" : {"mapper" : "keys", "reducer" : "keys", "timeout" : 60000}}
```
As you can see,  _KeysMapper_ and _KeysAggregator are invoked by using_keys_as mapper and reducer name._

##### Javascript-based map/reduce #####

Javascript can also be used to provide user-specified mapper, combiner and reducer functions to perform a completely client-driven map/reduce process.

All Javascript functions are specified inside the map/reduce descriptor.

The Javascript mapper function must be specified with the _js-mapper_ name, must be passed inside the json parameters under the _mapper_ key, and must have the following signature (where _key_ is the document key, _value_ is the document json structure and params is the json structure passed by the user for parameters):
```
function(key, value, params)
```

The Javascript combiner function, optional, must be specified with the _js-combiner_ name, must be passed inside the json parameters under the _combiner_ key, and must have the following signature (where _values_ is the list of json documents resulted from the local node map phase and params is the json structure passed by the user for parameters):
```
function(values, params)
```

The Javascript reducer function must be specified with the _js-reducer_ name, must be passed inside the json parameters under the _reducer_ key, and must have the following signature (where _values_ is the list of json documents resulted from the global map phase and params is the json structure passed by the user for parameters):
```
function(values, params)
```

Here is a sample map/reduce json descriptor:
```
{"task" : 
{"mapper" : "js-mapper" , 
"combiner" : "js-combiner" , 
"reducer" : "js-reducer" , 
"timeout" : 100000 , 
"parameters" : 
{"mapper" : "function(key,value,params) {//...}" , 
"combiner" : "function(values,params) {//...}" ,
"reducer" : "function(values,params) {//...}"}}}
```

#### Custom implementations ####

##### Writing custom comparators #####

Comparators are used to define key order in queries: they can be dynamically referred at runtim, providing a powerful way to execute dynamic queries.

In order to define a custom comparator, just follow the simple steps below.

Implement the Terrastore-specific _terrastore.store.operators.Comparator_ interface (which just extends the standard _java.util.Comparator_ interface):
```
/**
 * Interface to implement for comparing keys.
 */
public interface Comparator extends java.util.Comparator<String> {

    public int compare(String key1, String key2);
}
```

Annotate your comparator with _terrastore.annotation.AutoDetect_:
```
@AutoDetect(name="custom-comparator")
public class MyComparator implements Comparator {
    // ...
}
```
The _custom-comparator_ name will be the value to use in the client API to dynamically refer to the comparator.

Finally, deploy your comparator as a jar into the Terrastore server _libs_ directory and restart the server.

##### Writing custom conditions #####

Conditions are used to define a "condition expression" for conditionally selecting documents, for example in range queries.

In order to define a custom condition, just follow the simple steps below.

Implement the Terrastore-specific _terrastore.store.operators.Condition_ interface:
```
/**
 * Interface to implement for evaluating conditions on bucket values.
 */
public interface Condition {

    /**
     * Evaluate this condition on the given value, represented as a map of name -> value pairs (associative array).
     *
     * @param key The key of the value.
     * @param value The value to evaluate condition on.
     * @param expression The condition expression.
     * @return True if satisfied, false otherwise.
     */
    public boolean isSatisfied(String key, Map<String, Object> value, String expression);
}
```

Annotate your condition with _terrastore.annotation.AutoDetect_:
```
@AutoDetect(name="custom-condition")
public class MyCondition implements Condition {
    // ...
}
```
The _custom-condition_ name will be the value to use in the client API to refer to the condition type.

Finally, deploy your condition as a jar into the Terrastore server _libs_ directory and restart the server.

##### Writing custom functions #####

Custom functions are used to implement atomic updates, or mapper phases.

In order to define a custom function, just follow the simple steps below.

Implement the Terrastore-specific _terrastore.store.operators.Function_ interface:
```
/**
 * Interface to implement for applying functions to bucket values.
 */
public interface Function {

    /**
     *  Apply this function to the given value, represented as a map of name -> value pairs (associative array).
     *
     * @param key The key of the value.
     * @param value The value to apply the function to.
     * @param parameters The function parameters.
     * @return The result of the function as an associative array.
     */
    public Map<String, Object> apply(String key, Map<String, Object> value, Map<String, Object> parameters);
}
```

Annotate your function with _terrastore.annotation.AutoDetect_:
```
@AutoDetect(name="custom-function")
public class MyFunction implements Function {
    // ...
}
```
The _custom-function_ name will be the value to use in the client API to refer to the function name.

Finally, deploy your function as a jar into the Terrastore server _libs_ directory and restart the server.

##### Writing custom aggregators #####

Custom aggregators are used to implement combiner/reducer phases.

In order to define a custom aggregator, just follow the simple steps below.

Implement the Terrastore-specific _terrastore.store.operators.Aggregator interface:
```
//**
 * Interface to implement for aggregating values from mapper/combiner phases, into a final aggregated result.
 */
public interface Aggregator {

    /**
     * Apply this the aggregator to the given list of values, each represented as a map of name -> value pairs.
     * The input/output values are maps containing primitive values (such as integers, strings and alike),
     * nested maps and lists of primitive and nested map values.
     *
     * @param values The values to aggregate.
     * @return The aggregation result.
     */
    public Map<String, Object> apply(List<Map<String, Object>> values);
}
```_

Annotate your function with _terrastore.annotation.AutoDetect_:
```
@AutoDetect(name="custom-aggregator")
public class MyAggregator implements Aggregator {
    // ...
}
```
The _custom-aggregator_ name will be the value to use in the client API to refer to the function name.

Finally, deploy your function as a jar into the Terrastore server _libs_ directory and restart the server.

## Notes about autowiring ##

Using the _AutoDetect_ annotation for autowiring requires to:
  * Implement a no-arg constructor.
  * Keep the implementation stateless or at least thread-safe (discouraged, stateless is preferred).

## Terrastore APIs ##

In order to start developing with Terrastore, you need the latest and greatest Terrastore API jar.

If you're using Maven, add the following repository declaration:
```
<repository>
    <id>terrastore-repo</id>
    <name>Terrastore Repository</name>
    <url>http://m2.terrastore.googlecode.com/hg/repo</url>
</repository>
```

Then add the Terrastore API dependency:
```
<dependency>
    <groupId>terrastore</groupId>
    <artifactId>terrastore</artifactId>
    <classifier>api</classifier>
    <version>PROPER_VERSION</version>
    <scope>provided</scope>
</dependency>
```

If you don't use Maven, just get the API jar from Terrastore main distribution bundle.

## Terrastore embedded testing ##

Starting from version 0.7.1, Terrastore main jar provides the _terrastore.test.embedded.TerrastoreEmbeddedServer_ class which you can use to start and stop an in-memory Terrastore server and run your tests against it: the embedded server will listen to the provided host and port for your http requests and behave as a standard Terrastore server (but with no cluster/distribution capabilities).

In order to use the embedded server, just import the Terrastore main jar in your pom:
```
<dependency>
    <groupId>terrastore</groupId>
    <artifactId>terrastore</artifactId>
    <version>PROPER_VERSION</version>
</dependency>
```