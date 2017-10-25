
#!/bin/sh

##apt-get install ssh

apk update

apk add openssh

echo gpadmin |ssh -o "StrictHostKeyChecking no" gpadmin@10.63.33.203
psql -c "CREATE schema trial"


