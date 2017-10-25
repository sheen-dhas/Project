#!/bin/sh

cp mongo.js /home
chmod 777 mongo.js

cd /usr/bin/

mongod --fork --logpath /var/log/mongodb/mongod.log

##mongo --eval "printjson(db.serverStatus())"

mongo < /home/mongo.js
