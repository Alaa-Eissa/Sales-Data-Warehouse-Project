/*
=====================
create stored procedure : silver.load_silver
======================
*/
EXEC silver.load_silver;


CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
	SET NOCOUNT ON;
    BEGIN TRANSACTION;
    DECLARE @batch_start_time DATETIME = GETDATE();
	DECLARE @start_time DATETIME;
	DECLARE @end_time DATETIME;
	BEGIN TRY
        PRINT '===================================================';
        PRINT ' LOADING SILVER LAYER';
        PRINT '===================================================';

        PRINT '>> Truncating Table: silver.ALLsales';
        TRUNCATE TABLE Silver.Allsales;

        PRINT '>> Inserting Data into: silver.ALLsales';

		INSERT INTO Silver.Allsales(
				order_id , order_date, ship_date, ship_mode,
				customer_id , customer_name,segment , country,
				city, state, postal_code , region,
				product_id, category , sub_category , product_name,
				sales, quantity , discount, profit,
				source_file)
		 SELECT 
				order_id,
				CASE
					-- excel serial number
					WHEN ISNUMERIC(order_date)=1
					THEN CAST(DATEADD(DAY , CAST(order_date as int) -2 , '1900-01-01') as date)
					-- case 23  (yyyy-mm-dd)
					WHEN TRY_CONVERT(DATE , order_date, 23) is not null
					THEN TRY_CONVERT(DATE , order_date,23)
					-- case 101 ( mm/dd/yyyy)
					WHEN TRY_CONVERT(DATE , order_date, 101) is not null
					THEN TRY_CONVERT(DATE , order_date,101)
					-- case 103 ( dd/mm/yyyy)
					WHEN TRY_CONVERT(DATE , order_date, 103) is not null
					THEN TRY_CONVERT(DATE , order_date,103)
					-- case 105 ( dd-mm-yyyy)
					WHEN TRY_CONVERT(DATE , order_date, 105) is not null
					THEN TRY_CONVERT(DATE , order_date,105)
					-- case 111 - yyyy/mm/dd
					WHEN TRY_CONVERT(DATE , order_date, 111) is not null
					THEN TRY_CONVERT(DATE , order_date,111)
					ELSE NULL
				END as order_date,
				CASE
					-- excel serial number
					WHEN ISNUMERIC(ship_date)=1
					THEN CAST(DATEADD(DAY , CAST(ship_date as int) -2 , '1900-01-01') as date)
					-- case 23  (yyyy-mm-dd)
					WHEN TRY_CONVERT(DATE , ship_date, 23) is not null
					THEN TRY_CONVERT(DATE , ship_date,23)
					-- case 101 ( mm/dd/yyyy)
					WHEN TRY_CONVERT(DATE , ship_date, 101) is not null
					THEN TRY_CONVERT(DATE , ship_date,101)
					-- case 103 ( dd/mm/yyyy)
					WHEN TRY_CONVERT(DATE , ship_date, 103) is not null
					THEN TRY_CONVERT(DATE , ship_date,103)
					-- case 105 ( dd-mm-yyyy)
					WHEN TRY_CONVERT(DATE , ship_date, 105) is not null
					THEN TRY_CONVERT(DATE , ship_date,105)
					-- case 111 - yyyy/mm/dd
					WHEN TRY_CONVERT(DATE , ship_date, 111) is not null
					THEN TRY_CONVERT(DATE , ship_date,111)
					ELSE NULL
				END as ship_date,
				CASE 
					WHEN UPPER(TRIM(ship_mode))='STANDARD CLASS' THEN 'Standard Class'
					WHEN UPPER(TRIM(ship_mode))='SECOND CLASS' THEN 'Second Class'
					WHEN UPPER(TRIM(ship_mode))='FIRST CLASS' THEN 'First Class'
					WHEN UPPER(TRIM(ship_mode))='SAME DAY' THEN 'Same Day'
					ELSE TRIM(ship_mode)
				END as ship_mode,
				customer_id,
				TRIM(customer_name) as customer_name,
				CASE 
					WHEN UPPER(TRIM(segment))='CONSUMER' THEN 'Consumer'
					WHEN UPPER(TRIM(segment))='CORPORATE' THEN 'Corporate'
					WHEN UPPER(TRIM(segment))='HOME OFFICE' THEN 'Home Office'
					ELSE 'n/a'
				END as segment,
				TRIM(country) as country,
				TRIM(city) as city,
				TRIM(state) as state,
				TRIM(postal_code) as postal_code,
				CASE 
					WHEN UPPER(TRIM(region))='EAST' THEN 'East'
					WHEN UPPER(TRIM(region))='WEST' THEN 'West'
					WHEN UPPER(TRIM(region))='CENTRAL' THEN 'Central'
					WHEN UPPER(TRIM(region))='SOUTH' THEN 'South'
					ELSE TRIM(region)
				 END as region,
				 product_id,
				 CASE 
					WHEN UPPER(TRIM(category))='FURNITURE' THEN 'Furniture'
					WHEN UPPER(TRIM(category))='TECHNOLOGY' THEN 'Technology'
					WHEN UPPER(TRIM(category))='OFFICE SUPPLIES' THEN 'Office Supplies'
					ELSE 'n/a'
				 END as category,
				 TRIM(sub_category) as sub_category,
				 TRIM(product_name) as product_name,
				 CASE
					WHEN sales is null or sales <0 THEN 0
					ELSE sales
				END as sales,
				quantity,
				CASE
					WHEN discount is null then 0
					ELSE discount
				END as discount,
				CASE 
					WHEN profit is null THEN 0
					ELSE profit
				END as profit,
				Source_file
			   FROM(
					select * , 
					ROW_NUMBER() OVER(PARTITION BY order_id , product_id ORDER BY source_file desc)
					 as flag
					 FROM SalesDWH.dbo.All_Raw_sales
					 where order_id is not null ) t
				 where flag=1
				 PRINT 'Rows Inserted :'+ cast(@@ROWCOUNT AS VARCHAR);
				 PRINT '-------------------------------------------------------------------------------------------------'

				 COMMIT TRANSACTION;
			  
				 PRINT '===================================================';
				 PRINT ' LOADING COMPLETED SUCCESSFULLY';
				 PRINT ' Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, GETDATE()) AS VARCHAR) + ' seconds';
				 PRINT '===================================================';
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT >0
		 ROLLBACK TRANSACTION;

		 PRINT'=======================================================';
		 PRINT'ERROR OCCURED';
		 PRINT'Error :'+ ERROR_MESSAGE();
		 PRINT ' Error Number:  ' + CAST(ERROR_NUMBER() AS VARCHAR);
         PRINT ' Error State:   ' + CAST(ERROR_STATE() AS VARCHAR);
		 PRINT'=======================================================';
		 THROW;
   END CATCH
END;

