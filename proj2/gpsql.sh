
#!/bin/sh

cd /usr/lib/apt/methods

ssh gpadmin@10.63.33.203 <<!
gpadmin
psql -c "SELECT datname FROM pg_database WHERE datistemplate = false"
!


