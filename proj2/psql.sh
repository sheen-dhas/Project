#!/bin/sh
chown gpadmin /gpdata/segments/gpseg1
su gpadmin
. /usr/local/greenplum-db/greenplum_path.sh
/usr/local/greenplum-db/bin/gpstart -d "/gpdata/segments/gpseg1"

##ls /*

#ls /gpdata/master/*
#ls /gpdata/segments/*
#/data/master/

#gpstart -d /home
#psql -c "select current_date" 
