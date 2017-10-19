#!/bin/sh
. /usr/local/greenplum-db/greenplum_path.sh
gpstart -d /home
psql -c "select current_date" 
