create table customer(customer_id bigint, name varchar(128), primary key(customer_id));
create table corder(corder_id bigint, customer_id bigint, name varchar(128), primary key(corder_id));
create table corder_event(corder_event_id bigint, corder_id bigint, keyspace_id varbinary(10), primary key(corder_id, corder_event_id));
create table corder_keyspace_idx(corder_id bigint not null auto_increment, keyspace_id varbinary(10), primary key(corder_id));
