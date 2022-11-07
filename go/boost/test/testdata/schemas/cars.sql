CREATE TABLE Car (id int, brand varchar(255), PRIMARY KEY(id));

/* DML */
SELECT /*vt+ VIEW=CountCars PUBLIC */ COUNT(*) FROM Car WHERE Car.brand = ?;
