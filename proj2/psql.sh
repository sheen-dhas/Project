#!/bin/sh
. /usr/local/greenplum-db/greenplum_path.sh
gpstate
psql -c "select current_date" 
