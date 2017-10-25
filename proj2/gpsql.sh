
#!/bin/sh
#!/usr/bin/expect -f
##apt-get install ssh

apk update

apk add openssh

apk add sshpass

sshpass -p 'gpadmin' ssh -o "StrictHostKeyChecking no" gpadmin@10.63.33.203 <<!
psql -c "CREATE schema trial"
!

