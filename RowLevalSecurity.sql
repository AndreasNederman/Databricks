-- Databricks notebook source
--Row level security works so the user only sees the data they should see when reading from PowerBi or in Databricks.


--https://docs.databricks.com/security/access-control/table-acls/object-privileges.html#dynamic-view-functions
--You need to login as Admin.
--Go to user setting
--Then go to Admin Console
--After that create a Group and add the people to the group
--The view takes into account of partiotions 

-- COMMAND ----------

--To create a view people can connect to
Create View taxidataset.nyctaxiVendor
AS
SELECT *
FROM taxidataset.nyctaxi
Where is_member(vendor)


-- COMMAND ----------

--to test if your view works
select * FROM taxidataset.nyctaxiVendor