/*
==========================================================================================================
Gold layer : Star Schema Implementation
      1- create schema
	  2- create fact table (fact_sales) & dim tables(dim_customer , dim_product , dim_location , dim_date)
============================================================================================================
*/

IF NOT EXISTS (select * from sys.schemas where name= 'gold')
	EXEC('Create schema gold');
GO

PRINT'==========================================';
PRINT'Creating view : gold.dim_customers';
PRINT'==========================================';
GO

IF OBJECT_ID ('gold.dim_customers','v') IS NOT NULL 
   DROP VIEW gold.dim_customers;
GO

create view gold.dim_customers as
select
		ROW_NUMBER() over(order by customer_id) as Customer_key,
		customer_id ,
		customer_name,
		segment
from(
		select distinct
				customer_id , customer_name , segment
				from silver.AllSales
				where customer_id is not null )sub_query1;
GO


PRINT'==========================================';
PRINT'Creating view : gold.dim_products';
PRINT'==========================================';
GO

IF OBJECT_ID('gold.dim_products' ,'v') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

create view gold.dim_products as 
select 
	ROW_NUMBER() over(order by product_id) as product_key,
	product_id ,
	product_name,
	category,
	sub_category
from(
		select distinct 
			product_id , product_name , category , sub_category
			from silver.Allsales
			where product_id is not null) sub_query2;
GO


PRINT'==========================================';
PRINT'Creating view : gold.dim_locations';
PRINT'==========================================';
GO

IF OBJECT_ID ('gold.dim_locations','v') IS NOT NULL 
   DROP VIEW gold.dim_locations;
GO

create view gold.dim_locations as 
select 
	ROW_NUMBER() over (order by ISNULL( postal_code , '00000')) as loc_key,
	postal_code,
	city,
	state,
	country,
	region             
from (
		select distinct postal_code , city , 
		state , country , region
		from Silver.AllSales
		where postal_code is not null) sub_query3;
GO



PRINT'==========================================';
PRINT'Creating view : gold.dim_dates';
PRINT'==========================================';
GO

IF OBJECT_ID ('gold.dim_dates','U') IS NOT NULL 
   DROP TABLE gold.dim_dates;


create table gold.dim_dates(
		date_key   int   primary key,
		full_date   date,
		day         int,
		month        int,
		month_name    nvarchar(20),
		quarter       int,
		year          int,
);



DECLARE @start DATE='2020-01-01';
DECLARE @end   DATE='2030-12-31';
DECLARE @date DATE=@start;

while  @date <= @end
BEGIN
	 insert into gold.dim_dates values(
	 cast(format (@date , 'yyyyMMdd') as int),
	 @date,
	 DAY(@date),     MONTH(@date),
	 DATENAME(MONTH , @date),
	 DATEPART(QUARTER , @date),
	 YEAR(@date) )
	 set @date = DATEADD(DAY,1, @date);
END;
GO
	 
PRINT'==========================================';
PRINT'Creating view : gold.fact_sales';
PRINT'==========================================';
GO

IF OBJECT_ID ('gold.fact_sales','v') IS NOT NULL 
   DROP VIEW gold.fact_sales;
GO	

create view gold.fact_sales as 
select   s.order_id as Order_id ,
		 s.order_date   as Order_date,
		 s.ship_date    as Ship_date,
		 d.date_key     as date_key,
		 c.customer_key   as Customer_key,
		 p.Product_key    as Product_key,
		 l.loc_key        as Location_key,
		 s.ship_mode      as Ship_mode,
		 s.Sales         as Sales,
		 s.quantity      as Quantity,
		 s.discount      as Discount,
		 s.profit        as Profit
from silver.AllSales s
left join gold.dim_customers  c
	on s.customer_id = c.customer_id
left join gold.dim_products p 
	on s.product_id = p.product_id
left join  gold.dim_locations l
	on s.postal_code = l.postal_code
left join gold.dim_dates d
	 on s.order_Date = d.full_date;

GO

PRINT'====================================';
PRINT' Gold Layer created Successfully';
PRINT'====================================';
GO
