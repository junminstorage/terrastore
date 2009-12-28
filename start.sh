#!/bin/bash

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

for file in ${TERRASTORE_HOME}/libs/*.jar
do
  CLASSPATH=$file:$CLASSPATH
done

export TC_INSTALL_DIR=${TERRASTORE_HOME}/terrastore-master-libs
export TC_CONFIG_PATH=$terrastore_master
. ${TC_INSTALL_DIR}/bin/dso-env.sh -q
export JAVA_OPTS="$TC_JAVA_OPTS $JAVA_OPTS"

java ${JAVA_OPTS} -cp $CLASSPATH terrastore.startup.Startup $@
