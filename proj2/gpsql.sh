
#!/bin/sh

##apt-get install ssh

apk update

apk add openssh

ssh -o "StrictHostKeyChecking no" gpadmin@10.63.33.203
<<!
gpadmin
psql -c "CREATE schema trial"
##psql -c "CREATE TABLE trial.weather (city integer, name varchar(10))"
##psql -c "insert into trial.weather (city,name) values (1,'aaa')"
##psql -c "insert into trial.weather (city,name) values (2,'a1')"
!


