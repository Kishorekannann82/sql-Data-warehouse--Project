/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
exec silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver as
begin
		DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
		BEGIN Try
			SET @batch_start_time=GETDATE();
			PRINT '====================================';
			PRINT ' Loading Silver Layer';
			PRINT '====================================';

			PRINT '------------------------------------'
			PRINT 'Loading CRM Tables';
			PRINT '------------------------------------'
		
			SET @start_time=GETDATE();
		PRINT '>> tRUNCATING tABLE:silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> INSERTING DATA INTO :silver.crm_cust_info';
		--Cleaning the data
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		SELECT
		cst_id,cst_key,TRIM(cst_firstname) AS cst_firstname,--Trimming Data
		TRIM(cst_lastname)AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) ='S' THEN 'Single' 
			 when UPPER(TRIM(cst_marital_status))='M' then 'Married'
			 ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
			 when UPPER(TRIM(cst_gndr))='M' then 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM (
				SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
				FROM bronze.crm_cust_info
				where cst_id is not null
		)T where flag_last =1
			SET @end_time=GETDATE();
			PRINT '___________________________________';
			PRINT 'Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		SET @start_time=GETDATE();
		PRINT '>> tRUNCATING tABLE:silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> INSERTING DATA INTO :silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
					prd_id,
					cat_id,
					prd_key,
					prd_nm,
					prd_cost,
					prd_line,
					prd_start_dt,
					prd_end_dt
				)
				SELECT
					prd_id,
					REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
					SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
					prd_nm,
					ISNULL(prd_cost, 0) AS prd_cost,
					CASE 
						WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
						WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
						WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
						WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
						ELSE 'n/a'
					END AS prd_line, -- Map product line codes to descriptive values
					CAST(prd_start_dt AS DATE) AS prd_start_dt,
					CAST(
						LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
						AS DATE
					) AS prd_end_dt -- Calculate end date as one day before the next start date
				FROM bronze.crm_prd_info;
				SET @end_time=GETDATE();
			PRINT '___________________________________';
			PRINT 'Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

		--crm_sales_details
		SET @start_time=GETDATE();
		PRINT '>> tRUNCATING tABLE:silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> INSERTING DATA INTO :silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details(
			 sls_ord_num,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price 
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
    
			CASE 
				WHEN TRY_CAST(sls_order_dt AS INT) = 0 OR LEN(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
    
			CASE 
				WHEN TRY_CAST(sls_ship_dt AS INT) = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
    
			CASE 
				WHEN TRY_CAST(sls_due_dt AS INT) = 0 OR LEN(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,

			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
    
			sls_quantity,

			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
				THEN NULLIF(sls_sales, 0) / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
      
		FROM bronze.crm_sales_details;
		SET @end_time=GETDATE();
			PRINT '___________________________________';
			PRINT 'Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';

		--Cleaning erp_cust_az12
		PRINT '------------------------------------'
			PRINT 'Loading ERP Tables';
			PRINT '------------------------------------'

			SET @start_time=GETDATE();
		PRINT '>> tRUNCATING tABLE:silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> INSERTING DATA INTO :silver.erp_cust_az12';
		insert into silver.erp_cust_az12(
			cid ,
			bdate,
			gen
		)

		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
				ELSE cid 
			END AS cid,
    
			CASE 
				WHEN bdate > GETDATE() THEN NULL 
				ELSE bdate 
			END AS bdate,
    
 

			CASE 
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a' 
			END AS gen
		FROM bronze.erp_cust_az12;
		SET @end_time=GETDATE();
			PRINT '___________________________________';
			PRINT 'Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';



		-- Cleaning erp_loc_a101
		SET @start_time=GETDATE();
		PRINT '>> tRUNCATING tABLE:silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> INSERTING DATA INTO :silver.erp_loc_a101';

		insert into silver.erp_loc_a101(
			cid,
			cntry
			)
		select replace(cid,'-','') cid,

		case when trim(cntry)='DE' THEN 'Germany'
			when trim(cntry) in ('US','USA') THEN 'United States'
			when trim(cntry) ='' or cntry is null then 'n/a'
			else trim(cntry)
		end as cntry
		from bronze.erp_loc_a101
		SET @end_time=GETDATE();
			PRINT '___________________________________';
			PRINT 'Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';


		--cleaning erp.px_cat_g1v2
		SET @start_time=GETDATE();
		PRINT '>> tRUNCATING tABLE:silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> INSERTING DATA INTO :silver.erp_px_cat_g1v2';

		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		Select id,
			cat,
			subcat,
			maintenance
			from bronze.erp_px_cat_g1v2
			SET @end_time=GETDATE();
			PRINT '___________________________________';
			PRINT 'Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
			PRINT '==========================================='
			SET @batch_end_time=GETDATE();
			PRINT '==================================';
			PRINT 'Loading Silver Layer is Completed';
			PRINT ' Total Load Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
		END TRY
		BEGIN CATCH
			PRINT '================================'
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		END CATCH
end
