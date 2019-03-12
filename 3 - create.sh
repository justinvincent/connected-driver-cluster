maprcli table create -path /obd/obd_raw_table -tabletype json;
maprcli table create -path /obd/obd_transformed -tabletype json;
maprcli table create -path /obd/obd_messages -tabletype json;

maprcli stream create -path /obd/obd_msg_stream -consumeperm u:mapr -produceperm u:mapr;
maprcli stream topic create -path /obd/obd_msg_stream -topic obd_msg

hadoop fs -mkdir /obd/obd_checkpoints

exit 0

