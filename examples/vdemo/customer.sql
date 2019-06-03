create table customer(cid int, name varchar(128), balance bigint, primary key(cid));
create table orders(oid int, cid int, mname varchar(128), pid int, price int, primary key(oid));
