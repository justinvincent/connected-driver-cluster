#!/bin/bash

maprcli table delete -path /obd/obd_transformed;
maprcli table delete -path /obd/obd_raw_table;
maprcli table delete -path /obd/obd_messages;
maprcli stream delete -path /obd/obd_msg_stream
hadoop fs -rm -R /obd/obd_checkpoints
maprcli volume remove -name obd;

curl -i -X DELETE https://connected-driver-69921.firebaseio.com/cars.json
curl -i -X DELETE https://connected-driver-69921.firebaseio.com/messages.json

maprcli volume create -name obd -path /obd -type rw -advisoryquota 2G -quota 10G;
maprcli table create -path /obd/obd_raw_table -tabletype json;
maprcli table create -path /obd/obd_transformed -tabletype json;
maprcli table create -path /obd/obd_messages -tabletype json;
maprcli stream create -path /obd/obd_msg_stream -consumeperm u:mapr -produceperm u:mapr;
maprcli stream topic create -path /obd/obd_msg_stream -topic obd_msg
hadoop fs -mkdir /obd/obd_checkpoints
#mapr dbshell '"insert /obd/obd_messages --value \'{"_id":"99","2HMDJJFB2JJ000017", "date":"12-13-2019", "message":"Engine Status: HEALTHY", "severity":"0")\'"'

exit 0

