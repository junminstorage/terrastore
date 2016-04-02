# Project Ideas #



Are you **passionate** about open source and open communities?

Would you like to **contribute** to some cool open source project but you don't have the time to delve into its implementation details?

This is **your** place.

Here is a list of Terrastore-related project ideas: everyone who knows how to interact with Terrastore (and there are a few guides about that) will be able to work on them!

If you'd like to take over one of the following, feel free to let the community know by writing on the [mailing list](http://groups.google.com/group/terrastore-discussions).

Enjoy!

## Language bindings ##

Terrastore currently provides only [Java bindings](http://code.google.com/p/terrastore/downloads/list).

It would be great to have more language bindings in order to be able to use Terrastore from your language of choice without having to hand-write the HTTP calls: Scala, Ruby, PHP, Python, even a _better_ Java API ... it's up to you!

## ESB integration ##

Terrastore could be integrated as a service endpoint/component into well known open source ESB solutions such as [Apache Camel](http://camel.apache.org/) or [Mule](http://www.mulesoft.org/display/MULE/Home).

The service component implementation should be able to put data into Terrastore, as well as to get and execute query operations.

## Filesystem implementation ##

This is for you geeks out there: implementing a distributed filesystem on top of Terrastore buckets/documents structure and [FUSE](http://fuse.sourceforge.net/) bindings.

## Distributed Lucene ##

Another (Java) geeks task: implementing a distributed [Lucene](http://lucene.apache.org/java/docs/) directory, storing indexes on Terrastore.