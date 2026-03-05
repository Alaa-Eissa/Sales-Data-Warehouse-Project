select distinct order_id from All_Raw_sales
select top 10 * from All_Raw_sales

select source_file , count(*) as rows 
from all_raw_sales
group by source_file

select len(sls_order_dt) from silver.crm_sales_details
DROP TABLE All_Raw_sales;
DELETE FROM loaded_files;

select distinct segment from all_raw_sales;
select distinct region from all_raw_sales;
select distinct category from all_raw_sales;
select distinct sub_category from all_raw_sales;

select * from All_Raw_Sales
where order_date < 0

select top 10 * from silver.allsales


USE SalesDWH;

DROP TABLE gold.dim_dates;


SELECT 
    OBJECT_ID('gold.dim_date',  'U') AS dim_date_exists,
    OBJECT_ID('gold.dim_dates', 'U') AS dim_dates_exists;
select count(*) from gold.dim_dates	