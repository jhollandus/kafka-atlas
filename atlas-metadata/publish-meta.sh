#!/bin/bash 

if [ $# -eq 0 ]; then
  echo "Must supply Atlas host and port [host:port]"
  exit 1
fi

HOST="$1"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

#stty -echo
#read -p "Password: " PASS; echo
#stty echo
PASS=admin

DEFDEL=$(mktemp)

echo '{ "enumDefs": [ ], "structDefs": [ ], "classificationDefs": [ ], "entityDefs": [' >> $DEFDEL
curl -u "admin:$PASS" -H "content-type: application/json" "$HOST/api/atlas/v2/types/entitydef/name/kafka_topic" >> $DEFDEL
echo '  ] }' >> $DEFDEL
curl -u "admin:$PASS" -X DELETE -H "content-type: application/json" "$HOST/api/atlas/v2/types/typedefs" -d "@$DEFDEL"

for json in $(find "$DIR" -iname '*create.json'); do
  curl -u "admin:$PASS" -H "content-type: application/json" "$HOST/api/atlas/v2/types/typedefs" -d "@$json"
done
