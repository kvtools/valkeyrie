#!/bin/bash

if [  $# -gt 0 ] ; then
    AEROSPIKE_VERSION="$1"
else
    AEROSPIKE_VERSION="4.2.0.5"
fi

# install Aerospike
wget "https://www.aerospike.com/artifacts/aerospike-server-community/${AEROSPIKE_VERSION}/aerospike-server-community-${AEROSPIKE_VERSION}.tar.gz" -O aerospike-server.tgz
tar xzf aerospike-server.tgz

touch /aerospike-server/share/udf/lua/external/dummy.lua
aerospike-server/bin/aerospike init --home ./aerospike-server

# check
aerospike-server/bin/asd --version
