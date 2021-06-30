drop table if exists onlineddl_test;
create table onlineddl_test (
  id int auto_increment,
  latin1_to_utf8mb4 varchar(128) charset latin1 collate latin1_swedish_ci,
  latin1_to_latin2 varchar(128) charset latin1 collate latin1_swedish_ci,
  utf8_to_latin1 varchar(128) charset utf8,
  keep_utf8mb4 varchar(128) charset utf8mb4,
  keep_latin1 varchar(128) charset latin1 collate latin1_swedish_ci,
  primary key(id)
) auto_increment=1;

insert into onlineddl_test values (null, md5(rand()), md5(rand()), md5(rand()), md5(rand()), md5(rand()));
insert into onlineddl_test values (null, 'átesting-l1toutf8mb4-vcopier', 'átesting-l1tol2-vcopier', 'átesting-utf8tol1-vcopier', 'átesting-utf8mb4-vcopier', 'átesting-l1-vcopier');

drop event if exists onlineddl_test;
delimiter ;;
create event onlineddl_test
  on schedule every 1 second
  starts current_timestamp
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  insert into onlineddl_test values (null, md5(rand()), md5(rand()), md5(rand()), md5(rand()), md5(rand()));
  insert into onlineddl_test values (null, 'átesting-l1toutf8mb4-binlog', 'átesting-l1tol2-binlog', 'átesting-utf8tol1-binlog', 'átesting-utf8mb4-binlog', 'átesting-l1-binlog');
  insert into onlineddl_test values (null, 'átesting-l1toutf8mb4-bnull', 'átesting-l1tol2-bnull', 'átesting-utf8tol1-bnull', null, null);
end ;;
