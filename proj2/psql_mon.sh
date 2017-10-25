#!/bin/sh

ls /*

cd /usr/bin/

./mongod

mongo --eval "printjson(db.serverStatus())"
