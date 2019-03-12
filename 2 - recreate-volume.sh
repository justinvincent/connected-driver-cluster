maprcli volume remove -name obd;
maprcli volume create -name obd -path /obd -type rw -advisoryquota 2G -quota 10G;

exit 0