#!/bin/bash

if [ "${TERRASTORE_HOME}" = "" ]; then
    echo "ERROR: TERRASTORE_HOME environment variable is not found."
    exit 1
fi

if [ "${TERRACOTTA_HOME}" = "" ]; then
    echo "ERROR: TERRACOTTA_HOME environment variable is not found."
    exit 1
fi

if [ "${TERRACOTTA_SERVER}" = "" ]; then
    echo "ERROR: TERRACOTTA_SERVER environment variable is not found."
    exit 1
fi

for file in ${TERRASTORE_HOME}/libs/*.jar;
do
  CLASSPATH=$file:$CLASSPATH
done

export TC_INSTALL_DIR=${TERRACOTTA_HOME}
export TC_CONFIG_PATH=${TERRACOTTA_SERVER}
. ${TC_INSTALL_DIR}/bin/dso-env.sh -q
export JAVA_OPTS="$TC_JAVA_OPTS $JAVA_OPTS"

java ${JAVA_OPTS} -cp $CLASSPATH terrastore.startup.Startup $@