/*
========================================
DDL Script : create silver tables
========================================
*/
-- create schema if not exixt
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name='Silver')
	EXEC('CREATE SCHEMA Silver');
GO

-- create table same type as the table in bronze but after cleaning
IF OBJECT_ID ('Silver.AllSales' , 'U') IS NOT NULL
	DROP TABLE Silver.AllSales;
Go

CREATE TABLE  Silver.AllSales(
			order_id NVARCHAR(30),   order_date  DATE ,
			ship_date DATE  ,         ship_mode NVARCHAR(50),
			customer_id  NVARCHAR(20), customer_name NVARCHAR(100),
			segment NVARCHAR(50) ,      country NVARCHAR(100),
			city NVARCHAR(100),         state NVARCHAR(100),
			postal_code NVARCHAR(20),     region NVARCHAR(50),
			product_id  NVARCHAR(30),      category NVARCHAR(100),
			sub_category NVARCHAR(100),    product_name  NVARCHAR(255),
			sales decimal(12,4),          quantity int ,
			discount decimal(5,4),        profit decimal(12,4),
			source_file  NVARCHAR(100),    dwh_create_date  datetime default getdate());
GO

PRINT('Silver.AllSales Table created Successfully');
Go