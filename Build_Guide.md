## Building Terrastore ##



The following sections will show you how to check out and build Terrastore server and Terrastore java client sources, so that you'll be able to modify the code, test it and get your own distribution up and running.

### Building Terrastore Server from sources ###

Building from sources requires the following software installed on your computer:
  * [Mercurial](http://mercurial.selenic.com/), for distributed version control.
  * [Maven2](http://maven.apache.org/), for the build process.
  * [Java6](http://java.sun.com/).

First, check out sources from the main repository:
```
$> hg clone https://terrastore.googlecode.com/hg/ terrastore
```
Alternatively, you could also _clone_ the main repository, as explained [here](http://code.google.com/p/terrastore/source/createClone).

Then, do a simple build by running the following commands:
```
$> cd terrastore
$> mvn clean install
```

If you want to run unit and integration tests:
```
$> mvn install -Ptest
$> mvn install -Pintegration-test
```

Finally, if you want to assemble a distribution package (as the one provided in the _Downloads_ section):
```
$> mvn assembly:assembly
```

### Building Terrastore Java Client from sources ###

Building from sources requires the following software installed on your computer:
  * [Mercurial](http://mercurial.selenic.com/), for distributed version control.
  * [Maven2](http://maven.apache.org/), for the build process.
  * [Java6](http://java.sun.com/).

First, check out sources:
```
$> hg clone https://javaclient.terrastore.googlecode.com/hg/ terrastore-javaclient  
```
Alternatively, you could also _clone_ the main repository, as explained [here](http://code.google.com/p/terrastore/source/createClone?repo=javaclient).

Then, do a simple build by running the following commands:
```
$> cd terrastore-javaclient
$> mvn clean install
```

If you want to run unit tests:
```
$> mvn install -Ptest
```
Please note that unit tests require an up and running Terrastore cluster: take a look at the sources for additional details.

Finally, if you want to assemble a distribution package (as the one provided in the _Downloads_ section):
```
$> mvn assembly:assembly
```

### Building Terrastore Search from sources ###

Building from sources requires the following software installed on your computer:
  * [Mercurial](http://mercurial.selenic.com/), for distributed version control.
  * [Maven2](http://maven.apache.org/), for the build process.
  * [Java6](http://java.sun.com/).

First, check out sources:
```
$> hg clone https://search.terrastore.googlecode.com/hg/ terrastore-search  
```
Alternatively, you could also _clone_ the main repository, as explained [here](http://code.google.com/p/terrastore/source/createClone?repo=search).

Then, do a simple build by running the following commands:
```
$> cd terrastore-search
$> mvn clean install
```

Finally, if you want to assemble a distribution package (as the one provided in the _Downloads_ section):
```
$> mvn assembly:assembly
```