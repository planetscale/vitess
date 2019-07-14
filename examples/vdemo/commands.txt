Fill data
kmysql <data.sql

Product Materialized view
kvtctl Materialize -create_table -is_reference UserProduct product.product customer.product
kvtctl Expose --auto_route customer UserProduct

Merchant Orders
kvtctl Materialize -create_table -primary_vindex=mname:md5 MerchantOrder customer.orders merchant.orders
kvtctl Expose --auto_route merchant MerchantOrder

Sales schema
kvtctl ApplySchema -sql='create table sales(pid int, kount int, amount int, primary key(pid))' product

Materialize sales
kvtctl Materialize ProductSales 'select pid, count(*) as kount, sum(price) as amount from customer.orders group by pid' product.sales
kvtctl Expose product ProductSales

Migrate orders
kvtctl MigrateReads merchant MerchantOrder rdonly
kvtctl MigrateReads merchant MerchantOrder replica
kvtctl MigrateWrites merchant MerchantOrder
