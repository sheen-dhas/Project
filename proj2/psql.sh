#!/bin/sh
. /usr/local/greenplum-db/greenplum_path.sh
psql -c "select current_date" 
