create table product
(
    sku         varbinary(128),
    description varbinary(128),
    price       bigint,
    primary key (sku)
) ENGINE = InnoDB;

create table customer
(
    customer_id bigint not null auto_increment,
    email       varbinary(128),
    primary key (customer_id)
) ENGINE = InnoDB;

create table corder
(
    order_id    bigint   not null auto_increment,
    customer_id bigint,
    email       varbinary(128),
    sku         varbinary(128),
    credit_card varbinary(20),
    price       bigint,
    qty         int,
    created     datetime not null default current_timestamp,
    primary key (order_id)
) ENGINE = InnoDB;

create table sales
(
    sku         varbinary(128),
    num_orders  bigint,
    total_sales bigint,
    total_qty   bigint,
    primary key (sku)
) ENGINE = InnoDB;

create table corder_facts
(
    order_id      bigint,
    customer_id   bigint,
    email         varbinary(128),
    sku           varbinary(128),
    credit_card   varbinary(20),
    price         bigint,
    qty           int,
    total_price   int,
    created_month int,
    created_year  int,
    primary key (order_id)

) ENGINE = InnoDB;


