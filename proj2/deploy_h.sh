#!/bin/sh
#!/usr/bin/expect -f
##apt-get install ssh

ls

cp run.sh /home
chmod 777 run_hbase.sh

cp list.txt /home
chmod 777 list.txt

cp hbase_commands.txt /home
chmod 777 hbase_commands.txt

cp 646f8e796f044f1d8e459f2d28f4caab /home
chmod 777 646f8e796f044f1d8e459f2d28f4caab

apk update

apk add openssh

apk add sshpass

apk add scp

sshpass -p 'Welcome@321' scp -o "StrictHostKeyChecking no" run_hbase.sh hduser1@10.63.33.207:/home/hduser1
sshpass -p 'Welcome@321' scp -o "StrictHostKeyChecking no" list.txt hduser1@10.63.33.207:/home/hduser1
sshpass -p 'Welcome@321' scp -o "StrictHostKeyChecking no" hbase_commands.txt hduser1@10.63.33.207:/home/hduser1
sshpass -p 'Welcome@321' scp -o "StrictHostKeyChecking no" 646f8e796f044f1d8e459f2d28f4caab hduser1@10.63.33.207:/home/hduser1


sshpass -p 'Welcome@321' ssh -o "StrictHostKeyChecking no" hduser1@10.63.33.207 <<!
ls /home/hduser1/*
. /home/hduser1/run_hbase.sh
!
