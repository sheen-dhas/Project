
#!/bin/sh

cd /usr/lib/apt/methods

ls

cd /usr/lib/apt/methods

su - ubuntu <<!
bf4c7266d2e6c38fc3504a06
!

ssh gpadmin@10.63.33.203 
<<!
gpadmin
psql -c "CREATE schema trial"
##psql -c "CREATE TABLE trial.weather (city integer, name varchar(10))"
##psql -c "insert into trial.weather (city,name) values (1,'aaa')"
##psql -c "insert into trial.weather (city,name) values (2,'a1')"
!


