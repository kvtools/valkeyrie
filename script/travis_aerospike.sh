#!/bin/bash

if [  $# -gt 0 ] ; then
    AEROSPIKE_VERSION="$1"
else
    AEROSPIKE_VERSION="4.2.0.5"
fi

# install Aerospike
wget "https://www.aerospike.com/artifacts/aerospike-server-community/${AEROSPIKE_VERSION}/aerospike-server-community-${AEROSPIKE_VERSION}-ubuntu14.04.tgz" -O aerospike-server.tgz
mkdir aerospike
tar xzf aerospike-server.tgz --strip-components=1 -C aerospike
dpkg -i aerospike/aerospike-server-*.deb
rm -rf aerospike-server.tgz aerospike

# check
asd --version
