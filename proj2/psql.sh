#!/bin/sh
. /usr/local/greenplum-db/greenplum_path.sh
/usr/local/greenplum-db/bin/gpstate

ls /data/master/

#gpstart -d /home
#psql -c "select current_date" 
