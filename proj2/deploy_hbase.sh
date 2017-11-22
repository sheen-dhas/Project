
#!/bin/sh
#!/usr/bin/expect -f
##apt-get install ssh

ls


cp hbase_commands.txt /home
chmod 777 hbase_commands.txt

cp run_hbase.sh /home
chmod 777 run_hbase.sh

apk update

apk add openssh

apk add sshpass

apk add scp


sshpass -p 'Welcome@321' scp -o "StrictHostKeyChecking no" hbase_commands.txt hduser1@10.63.33.207:/home/hduser1


sshpass -p 'Welcome@321' ssh -o "StrictHostKeyChecking no" hduser1@10.63.33.207 <<!
ls /home/hduser1/*
. /home/hduser1/run_hbase.sh
!