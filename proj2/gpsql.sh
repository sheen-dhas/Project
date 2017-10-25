
#!/bin/sh
#!/usr/bin/expect -f
##apt-get install ssh

apk update

apk add openssh

spawn ssh -o "StrictHostKeyChecking no" gpadmin@10.63.33.203
expect "assword:"
send "gpadmin\r"
interact
psql -c "CREATE schema trial"


