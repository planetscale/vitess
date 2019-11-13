create table id_ks_map(id int, country varbinary(256), keyspace_id varbinary(256), primary key(id));
create table customer_seq(id int, next_id int, cache int, primary key(id)) comment 'vitess_sequence';
insert into customer_seq(id, next_id, cache) values(0, 1, 3);
