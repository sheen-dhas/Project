
#!/bin/sh

sudo apt-get install ssh

ssh gpadmin@10.63.33.203 
<<!
gpadmin
psql -c "CREATE schema trial"
##psql -c "CREATE TABLE trial.weather (city integer, name varchar(10))"
##psql -c "insert into trial.weather (city,name) values (1,'aaa')"
##psql -c "insert into trial.weather (city,name) values (2,'a1')"
!


