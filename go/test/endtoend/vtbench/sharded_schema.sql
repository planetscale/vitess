create table oltp_test(
  id bigint not null auto_increment,
  k bigint default 0 not null,
  c char(120) default '' not null,
  pad char(60) default '' not null,
  primary key (id)
) Engine=InnoDB;
