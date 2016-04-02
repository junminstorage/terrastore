# Data Processing #



Terrastore supports advanced data processing mechanisms by providing built-in **predicate conditions** and **atomic update functions**.

## Predicate conditions ##

Predicate conditions are used to query single documents, document ranges as well as all documents belonging to a given bucket, by selecting those which satisfy a predicate applied to their contents.<br />
Terrastore built-in predicate conditions are explained below.

### JXPath condition ###

The _JXPath_ condition selects documents whose contents satisfy a given XPath condition, and is invoked by specifying the _jxpath_ expression type.<br />
Here is an example, applying the condition to all documents in the _myBucket_ bucket:
```
GET /myBucket/predicate?predicate=jxpath:/author[.='Sergio Bossa']
```
The condition above would satisfy the following json document:
```
{"project" : "Terrastore", "author" : "Sergio Bossa"}
```
For more information about JXPath, see http://commons.apache.org/jxpath.

### Javascript condition ###

The _Javascript_ condition selects documents whose key or json value satisfies a given Javascript conditional expression, and is invoked by specifying the _js_ expression type.<br />
Here is an example with a condition applied on the key of all documents in the _myBucket_ bucket:
```
GET /myBucket/predicate?predicate=js:key.indexOf('myKey')==0
```
The condition above would satisfy any document whose key starts with _myKey_.<br />
Also, you can apply a condition on the json document value:
```
GET /myBucket/predicate?predicate=js:value.author=='Sergio Bossa'
```
The condition above would satisfy the following json document:
```
{"project" : "Terrastore", "author" : "Sergio Bossa"}
```

## Update functions ##

Update functions are used to atomically execute complex update logic on document values.<br />
Terrastore built-in update functions are explained below.

### Merge update function ###

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

### Counter update function ###

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

### Javascript update function ###

The _Javascript_ update function applies a user-specified Javascript function to a given document, and is invoked by specifying the _js_ function name.<br />
The Javascript function must be passed inside the json parameters under the _update_ key, and must have the following signature (where _key_ is the document key, _value_ is the document json structure and params is the json structure passed by the user for parameters):
```
function update(key, value, params)
```
Moreover, it must return a valid json structure.<br />
Here is an example:
```
POST /myBucket/myKey/update?function=js&timeout=10000
{"update" : "function update(key, value, params) {value.author = 'Sergio Bossa'; return value;}"}
```
The function above, applied to the following document:
```
{"project" : "Terrastore", "author" : ""}
```
Would result in the following updated document:
```
{"project" : "Terrastore", "author" : "Sergio Bossa"}
```

## Custom conditions and functions ##

You can obviously develop your own conditions and functions: just take a look at the [developers guide](Developers_Guide.md)!