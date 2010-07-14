Terrastore
-----

Terrastore is a modern document store which provides advanced scalability and elasticity features without sacrificing consistency.
Web: http://code.google.com/p/terrastore/

Basic Operations Guide
-----

A Terrastore cluster is composed by two different kind of processes: Terrastore master(s), and Terrastore server(s).
Both can be easily installed through the terrastore-install tool. 

* Prerequisites:

Java 6.
Apache Ant.

* Single-master Terrastore cluster installation:

$> ant -f terrastore-install.xml single-master -Dinstall.dir=/path_to_master
$> ant -f terrastore-install.xml server -Dinstall.dir=/path_to_server

The last command can be repeated for every server to add. 

* Active/Passive-Master Terrastore cluster installation:

$> ant -f terrastore-install.xml ha-master-1 -Dinstall.dir=/path_to_master-1
$> ant -f terrastore-install.xml ha-master-2 -Dinstall.dir=/path_to_master-2
$> ant -f terrastore-install.xml server -Dinstall.dir=/path_to_server 

The last command can be repeated for every server to add.
The active master will be the first to be started.

* Starting master(s) and server(s):

$path_to_master/bin> sh start.sh
$path_to_server/bin> sh start.sh --httpPort 8080 --nodePort 6000 --master host:9510

The default master port is 9510 (which can be changed through the
terrastore-install tool as well).

