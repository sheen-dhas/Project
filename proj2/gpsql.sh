
#!/bin/sh

##apt-get install ssh

apk update

apk add openssh

ssh -o "StrictHostKeyChecking no" gpadmin@10.63.33.203
expect "assword:"
send "gpadmin\r"
interact
psql -c "CREATE schema trial"


