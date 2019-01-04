#!/bin/bash

if [  $# -gt 0 ] ; then
    DYNAMODB_VERSION="$1"
else
    DYNAMODB_VERSION="latest"
fi


## Installing awscli & prep
pip install --user awscli

mkdir -p ~/.aws/

cat << EOF > ~/.aws/config
[default]
aws_access_key_id=foo
aws_secret_access_key=bar
region=localhost
EOF


## Pulling mock dynamodb
docker pull amazon/dynamodb-local:$DYNAMODB_VERSION


## Scripts
mkdir dynamodb

cat << EOF >./dynamodb/start.sh
#!/bin/sh
docker run -d -p 8000:8000 amazon/dynamodb-local:$DYNAMODB_VERSION
EOF

cat << EOF >./dynamodb/create_table.sh
#!/bin/sh
aws dynamodb create-table --table-name traefik \
  --attribute-definitions AttributeName=Key,AttributeType=S \
  --key-schema AttributeName=Key,KeyType=HASH \
  --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
  --endpoint-url http://localhost:8000
EOF

chmod +x ./dynamodb/*.sh
