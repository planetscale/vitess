CREATE TABLE Car (id int, brand varchar(255), PRIMARY KEY(id));

SELECT /*vt+ VIEW=CountCars */ COUNT(*) FROM Car WHERE Car.brand = ?;
