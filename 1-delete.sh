maprcli table delete -path /obd/obd_transformed;
maprcli table delete -path /obd/obd_raw_table;
maprcli table delete -path /obd/obd_messages;

maprcli stream delete -path /obd/obd_msg_stream

hadoop fs -rm -R /obd/obd_checkpoints

curl -i -X DELETE https://mapr-connected-driver.firebaseio.com/cars.json
curl -i -X DELETE https://mapr-connected-driver.firebaseio.com/messages.json

exit 0
