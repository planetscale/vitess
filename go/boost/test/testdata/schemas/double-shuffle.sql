CREATE TABLE Car (cid int, pid int, PRIMARY KEY(cid));
CREATE TABLE Price (pid int, price int, PRIMARY KEY(pid));

/* DML */

SELECT /*vt+ VIEW=CarPrice PUBLIC */ Car.cid, Price.price FROM Car
    JOIN Price ON Car.pid = Price.pid WHERE Car.cid = ?;
