#!/bin/bash

if [ "${TERRASTORE_HOME}" = "" ]; then
    export TERRASTORE_HOME=`dirname "$0"`/..
fi

if [ "${TERRASTORE_MASTER}" = "" ]; then
    echo "ERROR: TERRASTORE_MASTER environment variable is not found."
    exit 1
fi

for file in ${TERRASTORE_HOME}/libs/*.jar;
do
  CLASSPATH=$file:$CLASSPATH
done

export TC_INSTALL_DIR=${TERRASTORE_HOME}/terrastore-master-libs
export TC_CONFIG_PATH=${TERRASTORE_MASTER}:9510
. ${TC_INSTALL_DIR}/bin/dso-env.sh -q
export JAVA_OPTS="$TC_JAVA_OPTS $JAVA_OPTS"

java ${JAVA_OPTS} -cp $CLASSPATH terrastore.startup.Startup $@