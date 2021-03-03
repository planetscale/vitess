drop table if exists pii;
CREATE TABLE `pii` (
  `id` int(11) NOT NULL,
  `name` varchar(50) DEFAULT NULL COMMENT 'pii-name',
  `email` varchar(100) DEFAULT NULL COMMENT 'pii-email',
  `phone` varchar(20) DEFAULT NULL COMMENT 'pii-phone',
  `gender` char(1) DEFAULT NULL COMMENT 'pii-gender',
  `ssn` varchar(10) DEFAULT NULL COMMENT 'pii-ssn',
  `salary` int(11) DEFAULT NULL COMMENT 'pii-salary',
  `address` varchar(100) DEFAULT NULL COMMENT 'pii-address',
  `dob` date DEFAULT NULL COMMENT 'pii-date',
  `val1` varchar(100) DEFAULT NULL COMMENT 'pii-hash',
  `val2` varbinary(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

insert into pii(id, name, email, phone, gender, ssn, salary, address, dob, val1, val2)
values 
(1, 'John Smith', 'johnsmith@gmail.com', '212-123-4567', 'M', '123456789', '100000', '1 Main Street, Stamford, CT', '1990-12-12', 'private val1', 'public info 1'),
(2, 'Jane Doe', 'janedoe@yahoo.com', '543-123-4567', 'F', '987654321', '200000', '10 Point Place, WI', '2010-01-03', 'private  val2', 'public info 2');

create table pii2 like pii;

insert into pii2(id, name, email, phone, gender, ssn, salary, address, dob, val1, val2)
values
(1, 'John Smith', 'johnsmith@gmail.com', '212-123-4567', 'M', '123456789', '100000', '1 Main Street, Stamford, CT', '1990-12-12', 'private val1', 'public info 1'),
(2, 'Jane Doe', 'janedoe@yahoo.com', '543-123-4567', 'F', '987654321', '200000', '10 Point Place, WI', '2010-01-03', 'private  val2', 'public info 2');


