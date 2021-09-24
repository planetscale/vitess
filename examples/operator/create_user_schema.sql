create table users (
  user_id bigint not null,
  user_data varbinary(128),
  primary key(user_id)
) ENGINE=InnoDB;
