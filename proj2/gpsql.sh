
#!/bin/sh
#!/usr/bin/expect -f
##apt-get install ssh

apk update

apk add openssh

apk add sshpass

apk add scp

scp -p 'gpadmin' /create.sql gpadmin@10.63.33.203:/home

sshpass -p 'gpadmin' ssh -o "StrictHostKeyChecking no" gpadmin@10.63.33.203 <<!
psql -c "CREATE schema trial"
psql -c "CREATE TABLE trial.weather (city int, name varchar(10)) DISTRIBUTED BY (city)"
psql -c "insert into trial.weather (city,name) values (1,'aaa')"
psql -c "insert into trial.weather (city,name) values (2,'a1')"
!
