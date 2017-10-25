
#!/bin/sh
#!/usr/bin/expect -f
##apt-get install ssh

$FILENAME

apk update

apk add openssh

apk add sshpass

apk add ftp

ftp -v -n "10.63.33.203" << cmd
user "gpadmin" "gpadmin"
lcd /home

ls -1 create.sql $local_path

put create.sql
quit
cmd 

ls -1 *.txt $local_path > $FILENAME

put $FILENAME
quit
cmd

sshpass -p 'gpadmin' ssh -o "StrictHostKeyChecking no" gpadmin@10.63.33.203 <<!
psql -c "CREATE schema trial"
psql -c "CREATE TABLE trial.weather (city int, name varchar(10)) DISTRIBUTED BY (city)"
psql -c "insert into trial.weather (city,name) values (1,'aaa')"
psql -c "insert into trial.weather (city,name) values (2,'a1')"
!

