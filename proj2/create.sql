create database test;

create table test.trial
(
id integer,
name character varying(20)
)
WITH (APPENDONLY=true, COMPRESSLEVEL=5, ORIENTATION=row, COMPRESSTYPE=zlib, 
  OIDS=FALSE
)
DISTRIBUTED BY (id);
