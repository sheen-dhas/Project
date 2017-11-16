#!/bin/sh
#!/usr/bin/expect -f
##apt-get install ssh

ls

cp create.sql /home
chmod 777 create.sql

apk update

apk add openssh

apk add sshpass

apk add scp

sshpass -p 'hduser1' scp -o "StrictHostKeyChecking no" run_h.sh hduser1@10.63.33.207:/home/hduser1

sshpass -p 'hduser1' ssh -o "StrictHostKeyChecking no" hduser1@10.63.33.207 <<!
chmod 777 run_h.sh
ls /home/hduser1/*
. /home/hduser1/run_h.sh
!
