create table product(id bigint auto_increment, name varchar(128), primary key(id));
create table customer_seq(id bigint, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into customer_seq(id, next_id, cache) values(0, 1, 3);
create table corder_seq(id bigint, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into corder_seq(id, next_id, cache) values(0, 1, 2);
create table corder_event_seq(id bigint, next_id bigint, cache bigint, primary key(id)) comment 'vitess_sequence';
insert into corder_event_seq(id, next_id, cache) values(0, 1, 2);
create table name_keyspace_idx(name varchar(128), corder_id bigint, keyspace_id varbinary(10), primary key(name, corder_id));
