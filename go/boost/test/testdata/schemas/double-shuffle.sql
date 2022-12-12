CREATE TABLE Car (cid int, pid int, PRIMARY KEY(cid));
CREATE TABLE Price (pid int, price int, PRIMARY KEY(pid));

SELECT /*vt+ VIEW=CarPrice */ Car.cid, Price.price FROM Car
    JOIN Price ON Car.pid = Price.pid WHERE Car.cid = ?;
