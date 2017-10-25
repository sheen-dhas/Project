#!/bin/sh

ls /*

cd /usr/bin/

mongod --fork --logpath /var/log/mongodb/mongod.log

mongo --eval "printjson(db.serverStatus())"
