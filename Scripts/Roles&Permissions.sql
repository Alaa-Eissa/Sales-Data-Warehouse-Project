USE master;
-- create login on server
create login Alaa_Eissa with password='A@2812005';
create login other_user with password='AE9222';
GO

use SalesDWH;
-- create users on database
create user Alaa_Eissa for login Alaa_Eissa;
create user other_user for login other_user;
GO

-- create role
create role SeniorDataAnalyst;

-- grant select on gold views
grant select on gold.dim_customers  to SeniorDataAnalyst;
grant select on gold.dim_products  to SeniorDataAnalyst;
grant select on gold.dim_locations  to SeniorDataAnalyst;
grant select on gold.dim_dates  to SeniorDataAnalyst;
grant select on gold.fact_sales  to SeniorDataAnalyst;
Go

-- add me as a member in the role
Alter Role SeniorDataAnalyst Add Member Alaa_Eissa;
