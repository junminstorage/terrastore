# HTTP Client API #



Terrastore provides a collection-like interface for accessing and managing collections of documents identified by key.<br />
Collections, called _bucktes_, are dynamically created the first time you insert a document into a non existent one, and can be later removed.<br />
Documents, identified by string keys, are schema-less JSON structures: this means you can put any kind of JSON-compliant document into your collections, even different kind of documents in the same collection: the only divergence from the standard JSON format is that documents cannot start with an array.

Documents are accessed through a native JSON-over-HTTP interface, that you can interact with by using any HTTP client, such as the excellent [curl](http://curl.haxx.se/) command line client.<br />
A detailed description of the HTTP API follows.

## Bucket management ##

### Clear bucket ###

```
DELETE /bucket_name
```

Input:

  * _bucket\_name_ : name of the bucket whose contents must be cleared.

Return code:

  * 204 No Content

Errors:

  * 503 Service Unavailable

### Get all bucket names ###

```
GET /
Content-Type: application/json
```

Return value:

  * A JSON document containing an array of strings representing bucket names.

Return code:

  * 200 OK

### Get all key/values from bucket ###

```
GET /bucket_name?limit=max_elements
Content-Type: application/json
```

Input:

  * _bucket\_name_ : name of the bucket to add.
  * _limit_ : max number of elements to retrieve.

Return value:

  * A JSON document containing all key/value entries.

Return code:

  * 200 OK

## Document management ##

### Add/Replace document ###

```
PUT /bucket_name/document_key
Content-Type: application/json
```

Input:

  * _bucket\_name_ : name of the bucket where to add the document.
  * _document\_key_ : key of the document to add (or update).

Return code:

  * 204 No Content

Errors:

  * 503 Service Unavailable

### Remove document ###

```
DELETE /bucket_name/document_key
```

Input:

  * _bucket\_name_ : name of the bucket where to remove the document.
  * _document\_key_ : key of the document to remove.

Return code:

  * 204 No Content

Errors:

  * 503 Service Unavailable

### Get document ###

```
GET /bucket_name/document_key
Content-Type: application/json
```

Input:

  * _bucket\_name_ : name of the bucket where to get the document.
  * _document\_key_ : key of the document to get.

Return value:

  * The JSON document stored at the given key.

Return code:

  * 200 OK

Errors:

  * 404 Not Found
  * 503 Service Unavailable

## Backup management ##

### Export ###

It is possible to export all documents contained in a bucket to a file remotely located on the Terrastore server node **receiving the backup export request**, under the _backups_ directory.
To perform a backup export, just issue the following request to a Terrastore server node:

```
POST /bucket_name/export?destination=file_name&secret=secret_key
```

Input:

  * _bucket\_name_ : name of the bucket whose documents must be exported.
  * _file\_name_ : name of the backup file, located on the Terrastore server node.
  * _secret\_key_ : a string shared among clients and servers, to pass along in order to avoid executing exports by accident (default value is _SECRET-KEY_).

Return code:

  * 204 No Content

### Import ###

It is possible to import all documents contained in a backup file, located under the _backups_ directory of any Terrastore server node **receiving the backup import request**, into any existent bucket.
To perform a backup import, just issue the following request to the Terrastore server node hosting the backup file:

```
POST /bucket_name/import?source=file_name&secret=secret_key
```

Input:

  * _bucket\_name_ : name of the bucket receiving the documents.
  * _file\_name_ : name of the backup file, located on the Terrastore server node.
  * _secret\_key_ : a string shared among clients and servers, to pass along in order to avoid executing imports by accident (default value is _SECRET-KEY_).

Return code:

  * 204 No Content

## Statistics retrieval ##

### Cluster Statistics ###

```
GET /_stats/cluster
Content-Type: application/json
```

Return value:

  * A JSON document containing information about current cluster(s) status and topology.

Return code:

  * 200 OK

## Data Querying and Processing ##

### Bulk Put ###

Bulk Put is used to load several document in a single put operation, resulting in a faster operation than putting everything one-by-one.

Here is how to execute a bulk put:

```
POST /bucket_name/bulk/put
Content-Type: application/json
<documents>
```

Input:

  * _bucket\_name_ : name of the bucket where to put the documents.
  * _documents_ : the JSON structure containing all documents to put with related key.

Return value:

  * The list of actually inserted keys.

Return code:

  * 200 OK

Errors:

  * 404 Not Found
  * 503 Service Unavailable

### Bulk Get ###

Bulk Get is used to get several document in a single operation, resulting in a faster operation than getting everything one-by-one.

Here is how to execute a bulk get:

```
POST /bucket_name/bulk/get
Content-Type: application/json
<keys>
```

Input:

  * _bucket\_name_ : name of the bucket where to get the documents from.
  * _keys_ : the JSON list containing all keys to get.

Return value:

  * The documents retrieved from the bucket.

Return code:

  * 200 OK

Errors:

  * 404 Not Found
  * 503 Service Unavailable

### Conditional Put ###

Conditional Put operation let clients adding a new document or replacing an existent one if this satisfies a given predicate function.

Here is how to express a conditional Put operation:

```
PUT /bucket_name/document_key?predicate=type:expression
Content-Type: application/json
```

Input:

  * _bucket\_name_ : name of the bucket where to add the document.
  * _document\_key_ : key of the document to add (or update).
  * _predicate_ : predicate type and expression to evaluate.

Return code:

  * 204 No Content

Errors:

  * 409 Conflict
  * 503 Service Unavailable

### Conditional Get ###

Conditional Get operation let clients getting a document only if this satisfies a given predicate function.

Here is how to express a conditional Get operation:

```
GET /bucket_name/document_key?predicate=type:expression
Content-Type: application/json
```

Input:

  * _bucket\_name_ : name of the bucket where to get the document.
  * _document\_key_ : key of the document to get.
  * _predicate_ : predicate type and expression to evaluate.

Return value:

  * The JSON document stored at the given key and satisfying the given predicate.

Return code:

  * 200 OK

Errors:

  * 404 Not Found
  * 503 Service Unavailable

### Predicate queries ###

Predicate queries let clients retrieve all documents (inside a specific bucket) which satisfy a given predicate expression.<br />
The predicate expression must adhere to the following syntax:
```
type:expression
```
Where _type_ is the predicate type, and _expression_ is the actual predicate expression.<br />
For more information, take a look at the [Developers guide](Developers_Guide.md).

Here is how to execute a predicate query:

```
GET /bucket_name/predicate?predicate=type:expression
Content-Type: application/json
```

Input:

  * _bucket\_name_ : name of the bucket containing the documents to query.
  * _predicate_ : type and expression for the predicate to evaluate.

Return value:

  * A JSON document containing all key/value entries satisfying the predicate.

Return code:

  * 200 OK

### Range queries ###

Range queries let clients retrieve ordered ranges of documents, by comparing keys and verifying they belong to the given range.<br />
Moreover, range queries can be combined with predicates (expressed as explained in the previous paragraph) in order to retrieve only those documents satisfying a specific condition.<br />
For more information, take a look at the [Developers guide](Developers_Guide.md).

Here is how to execute a basic range query:

```
GET /bucket_name/range?comparator=comparator_name&startKey=start_key&endKey=end_key&timeToLive=snapshot_age
Content-Type: application/json
```

Input:

  * _bucket\_name_ : name of the bucket containing the documents to query.
  * _comparator\_name_ : name of the server-side comparator to use for determining the key range and its order.
  * _start\_key_ : first key in range (inclusive).
  * _end\_key_ : last key in range (inclusive).
  * _timeToLive_ : snapshot max age in milliseconds (see [Developers guide](Developers_Guide.md)).

Here is how to add a predicate expression:

```
GET /bucket_name/range?comparator=comparator_name&startKey=start_key&endKey=end_key&predicate=type:expression&timeToLive=snapshot_age
Content-Type: application/json
```

Input (other than the one specified above):

  * _predicate_ : predicate type and expression to evaluate.

Moreover, you can specify a _limit_ parameter as well: if you specify it together with start and end key, the selected range will contain a max number of elements up to the specified limit, or you can omit the end key, in which case all elements up to the specified limit and starting from the specified key will be selected.

```
GET /bucket_name/range?comparator=comparator_name&startKey=start_key&endKey=end_key&limit=max_elements&timeToLive=snapshot_age
Content-Type: application/json
```

Input (other than the one specified above):

  * _limit_ : max number of elements in range.

Return value:

  * A JSON document containing all key/value entries in the given range, optionally satisfying the given predicate expression (if any).

Return code:

  * 200 OK

### Range deletes ###

Range deletes let clients delete documents by a specified sorted range.<br />
Everything described for range queries in the previous paragraph applies also to range deletes.<br />
Here is the full specification of a range delete request:

```
DELETE /bucket_name/range?comparator=comparator_name&predicate=type:expression&startKey=start_key&endKey=end_key&limit=max_elements&timeToLive=snapshot_age
Content-Type: application/json
```

As you can see, only the HTTP verb changes, from GET to DELETE.

Once executed, the range delete returns a JSON array containing actually deleted keys.

### Map/Reduce ###

Map/Reduce is a well known parallel processing paradigm, widely used to query and aggregate distributed data.<br />
Terrastore map/reduce implementation is divided in three phases: the _mapper_ phase, where a map function is distributed among cluster nodes and applied to local document in order to process and extract relevant information, the _combiner_ phase applied locally on each node to aggregate partial map results, and the _reduce_ phase applied on the originator node to compute the final results from all the sub-results produced by each distributed node.<br />
For more information, take a look at the [Developers guide](Developers_Guide.md).

Here is how to execute a map/reduce query/aggregation:

```
POST /bucket_name/mapReduce
Content-Type: application/json
<descriptor>
```

Input:

  * _bucket\_name_ : name of the bucket containing the document to apply the map/reduce to.
  * _descriptor_ : a JSON document describing the map/reduce process to apply.

Here is the structure of the _descriptor_ document:

```
{"task" : {"mapper" : "mapper_name", "combiner" : "combiner_name", "reducer" : "reducer_name", "parameters" : "parameters", "timeout" : function_timeout}, "range" : {"startKey" : "start_key", "endKey" : "end_key", "comparator" : "comparator_name", "timeToLive" : timeToLive}}
```

Where:

  * _task_ is the actual description of the map/reduce process.
    * _mapper_ is the name of the mapper function.
    * _combiner_ is the name of the combiner function (optional, same as the reducer if not specified).
    * _reducer_ is the name of the reducer function.
    * _parameters_ is a JSON structure containing mapper parameters.
    * _function\_timeout_ is the max time in milliseconds phases are allowed to run prior to be tentatively aborted.
  * _range_ defines the set of documents to process (optional, default to the whole bucket).
    * _start\_key_ is the first key in range (inclusive).
    * _end\_key_ is the last key in range (inclusive).
    * _comparator\_name is the name of the comparator defining the key range.
    *_timeToLive_is the snapshot max age in milliseconds (see [Developers guide](Developers_Guide.md))._

Return value:

  * The map/reduce result.

Return code:

  * 200 OK

### Server-side update functions ###

Server-side update functions are used to update a document by applying a configured function and returning a JSON structure which will be used as the new, updated, value: for more information, take a look at the [Developers guide](Developers_Guide.md).

Here is how to execute an update function:

```
POST /bucket_name/document_key/update?function=function_name&timeout=timeout_value
Content-Type: application/json
<parameters>
```

Input:

  * _bucket\_name_ : name of the bucket containing the document to update.
  * _document\_key_ : key of the document to update.
  * _function\_name_ : name of the server-side function to execute.
  * _timeout\_value_ : timeout (in milliseconds) for function execution; functions lasting more than the given timeout will be aborted.
  * _parameters_ : the JSON structure containing function parameters.

Return value:

  * The updated JSON document.

Return code:

  * 200 OK

Errors:

  * 404 Not Found
  * 503 Service Unavailable

### Merge updates ###

Merge updates are used to apply updates by sending a merge document describing data to change, as described in the [developers guide](Developers_Guide.md).<br />
Merge updates can describe addition, removal and replacement of document fields, even if contained into nested documents, and they're generally faster than server-side update functions: so they're the preferred way to perform updates on stored documents.

Here is how to execute a merge update:

```
POST /bucket_name/document_key/merge
Content-Type: application/json
<merge_descriptor>
```

Input:

  * _bucket\_name_ : name of the bucket containing the document to update.
  * _document\_key_ : key of the document to update.
  * _merge\_descriptor_ : the JSON structure representing the merge descriptor.

Return value:

  * The updated JSON document.

Return code:

  * 200 OK

Errors:

  * 404 Not Found
  * 503 Service Unavailable

## Errors ##

All Terrastore errors are reported in a simple JSON format:

```
{"message":"error_message", "code": "http_error_code"}
```