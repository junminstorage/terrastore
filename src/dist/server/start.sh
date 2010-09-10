#!/bin/bash

case "`uname`" in
  CYGWIN*) cygwin=true ;;
esac

if [ "${TERRASTORE_HOME}" = "" ]
then
    export TERRASTORE_HOME=`dirname "$0"`/..
fi

is_terrastore_master="false"
for arg in "$@"
do
    if [ "$is_terrastore_master" = "true" ]
    then
        terrastore_master=$arg
        is_terrastore_master="false"
    elif [ "$arg" = "--master" ]
    then
        is_terrastore_master="true"
    fi
done
if [ "$terrastore_master" = "" ]
then
    echo "ERROR: You should set the Terrastore master host by passing the --master argument."
    exit 1
fi

CLASSPATH=${TERRASTORE_HOME}/extensions
for file in ${TERRASTORE_HOME}/libs/*.jar
do
  CLASSPATH=$CLASSPATH:$file
done

if [ "$cygwin" != "" ]
then
  CLASSPATH=`cygpath --windows --path "$CLASSPATH"`
  TERRASTORE_HOME=`cygpath --windows --path "$TERRASTORE_HOME"`
fi

echo "Starting Terrastore Server ..."

export JAVA_OPTS="$JAVA_OPTS -XX:+UseParallelGC -Dlogback.configurationFile=${TERRASTORE_HOME}/terrastore-logback.xml"

${JAVA_HOME}/bin/java ${JAVA_OPTS} -classpath "$CLASSPATH" terrastore.startup.Startup "$@"
