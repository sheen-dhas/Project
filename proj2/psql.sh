#!/bin/sh

##ls /*

##ls /run/*
##ls /etc/*
##ls /var/*

##. /run/postgresql "select * from weather"

cd /home
mkdir postgres
chmod 777 postgres

cd /usr/local
mkdir postgres
chmod 777  postgres
chown postgres postgres

cd /usr/lib/postgresql/10/bin/
su - postgres -c '/usr/lib/postgresql/10/bin/initdb -D /usr/local/postgres'

su - postgres -c '/usr/lib/postgresql/10/bin/pg_ctl start -D /usr/local/postgres -l serverlog'

su - postgres -c '/usr/lib/postgresql/10/bin/psql -c "CREATE schema trial" '

su - postgres -c '/usr/lib/postgresql/10/bin/psql -c "CREATE TABLE trial.weather (city varchar(80))" '

su - postgres -c '/usr/lib/postgresql/10/bin/psql -c "insert into trial.weather values ('a')" '

su - postgres -c '/usr/lib/postgresql/10/bin/psql -c "select * from trial.weather" '

exit;
##ls

##pg_ctl start

##./postgres 
##su -c 'su postgres'
##pg_ctl start -f a.out
##createdb mydb

##ls /run/*

##cd /usr/bin/
##psql -c "\d"


##mydb

##. /run/postgresql "CREATE TABLE weather (city varchar(80))"

##. /usr/local/greenplum-db/greenplum_path.sh
##/usr/local/greenplum-db/bin/gpstart -d "/gpdata/segments/gpseg1"
##/usr/local/greenplum-db/bin/gpstart-R -d "/gpdata/segments/gpseg1"

##ls /*

#ls /gpdata/master/*
#ls /gpdata/segments/*
#/data/master/

#gpstart -d /home
#psql -c "select current_date" 
