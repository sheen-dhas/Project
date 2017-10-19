#!/bin/sh
. /usr/local/greenplum-db/greenplum_path.sh
gpstart
psql -c "select current_date" 
