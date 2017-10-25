use db;

db.createCollection("emp");
db.emp.insert({id:1});
db.getCollection("emp").find()
