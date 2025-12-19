---------------------------------------------------------------------------- TABLE CREATION --------------------------------------------------------------------------------------------

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_first_name NVARCHAR(50),
cst_last_name NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id INT,
cat_id NVARCHAR (50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE());

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME2 DEFAULT GETDATE());

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE());


IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE());

IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);


EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @START_TIME DATETIME ,@END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
BEGIN TRY
			SET @BATCH_START_TIME = GETDATE();
			PRINT '=====================================';
			PRINT 'LOADING SILVER LAYER';
			PRINT '=====================================';

			PRINT '-------------------------------------';
			PRINT 'LOADING CRM SECTION'
			PRINT '-------------------------------------';
			SET @START_TIME = GETDATE();
			PRINT 'TRUNCATING TABLE: silver.crm_cust_info'; 
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT 'INSERTING VALUES INTO: silver.crm_cust_info';
			INSERT Into silver.crm_cust_info 
			(cst_id,
			cst_key,
			cst_first_name,
			cst_last_name,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
			select 
			cst_id,
			cst_key,
			TRIM(cst_first_name) AS cst_first_name,
			TRIM(cst_last_name) AS cst_last_name,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 ELSE 'N/A' 
			END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 ELSE 'N/A' 
			END AS cst_gndr,
			cst_create_date
			from
			(select *,ROW_NUMBER()over(partition by cst_id order by cst_create_date desc) as last_flag
			from bronze.crm_cust_info) as tt where last_flag = 1 AND cst_id is not null;
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<';

			SET @START_TIME = GETDATE();
			PRINT 'TRUNCATING TABLE: silver.crm_prd_info'; 
			TRUNCATE TABLE silver.crm_prd_info;
			PRINT 'INSERTING VALUES INTO: silver.crm_prd_info';
			insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm, 
			prd_cost, 
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
			select 
			prd_id,
			replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			prd_nm,
			isnull(prd_cost,0) as prd_cost,
			case UPPER(TRIM(prd_line))
				when  'M' then 'Mountain'
				when  'R' then 'Road'
				when  'S' then 'Other Sales'
				when  'T' then 'Touring'
				else 'N/A' 
			end as prd_line,
			cast(prd_start_dt as date) as prd_start_dt,
			cast(lead(prd_start_dt)over(partition by prd_key order by prd_start_dt) as date) as prd_end_dt  
			from bronze.crm_prd_info;
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<';

			SET @START_TIME = GETDATE();
			PRINT 'TRUNCATING TABLE: silver.crm_sales_details';
			TRUNCATE TABLE silver.crm_sales_details;
			PRINT 'INSERTING VALUES INTO: silver.crm_sales_details';
			INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			) 
			select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case
				when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
				else cast(cast(sls_order_dt as varchar) as date) 
			end as sls_order_dt,
			case
				when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as varchar) as date) 
			end as sls_ship_dt,
			case
				when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
				else cast(cast(sls_due_dt as varchar) as date) 
			end as sls_due_dt,
			case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
				 then sls_quantity * ABS(sls_price)
				else sls_sales
			end as sls_sales,
			sls_quantity,
			case when sls_price is Null or sls_price <= 0
				 then sls_sales / nullif(sls_quantity,0)
				 else sls_price
			end as sls_price
			from bronze.crm_sales_details
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'


			PRINT '-------------------------------------';
			PRINT 'LOADING ERP SECTION'
			PRINT '-------------------------------------';

			SET @START_TIME = GETDATE();
			PRINT 'TRUNCATING TABLE: silver.erp_cust_az12' 
			TRUNCATE TABLE silver.erp_cust_az12;
			PRINT 'INSERTING VALUES INTO: silver.erp_cust_az12'
			INSERT INTO silver.erp_cust_az12(CID,BDATE,GEN)
			select 
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING (CID,4,LEN(CID))
			ELSE CID
			END AS CID,
			case when bdate > getdate() then null 
			else bdate end as BADTE,
			case when Upper(trim(gen)) 
			in ('F','Female') then 'Female'
			when Upper(trim(gen)) 
			in ('M','Male') then 'Male'
			else 'N/A'
			end as GEN
			from bronze.erp_cust_az12;
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'

			SET @START_TIME = GETDATE();
			PRINT 'TRUNCATING TABLE: SILVER.erp_loc_a101' 
			TRUNCATE TABLE SILVER.erp_loc_a101;
			PRINT 'INSERTING VALUES INTO: SILVER.erp_loc_a101'
			INSERT INTO SILVER.erp_loc_a101(
			CID, CNTRY)
			SELECT REPLACE(CID,'-','') AS CID,
			CASE WHEN UPPER(TRIM(CNTRY)) IN ('US','USA') 
			THEN 'United States'
			when UPPER(TRIM(CNTRY)) = 'DE' 
			THEN 'Germnay'
			when UPPER(TRIM(CNTRY)) = '' OR CNTRY IS NULL 
			THEN 'N/A'
			ELSE TRIM(CNTRY)
			END AS CNTRY
			FROM bronze.erp_loc_a101;
			SET @END_TIME = GETDATE();
			PRINT '>>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND,@START_TIME,@END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '>>>---------------<<<'

			SET @START_TIME = GETDATE();
			PRINT 'TRUNCATING TABLE: silver.erp_px_cat_g1v2' 
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			PRINT 'INSERTING VALUES INTO: silver.erp_px_cat_g1v2'
			INSERT INTO silver.erp_px_cat_g1v2(ID,CAT,SUBCAT,MAINTENANCE)
			SELECT * FROM bronze.erp_px_cat_g1v2;
			SET @END_TIME = GETDATE();

			SET @BATCH_END_TIME = GETDATE();
			PRINT '=====================================';
			PRINT 'LAYER LOADING COMPLETED';
			PRINT 'LOAD DURATION FOR WHOLE LAYER: ' + CAST(DATEDIFF(SECOND,@BATCH_START_TIME,@BATCH_END_TIME) AS NVARCHAR) + 'SECONDS' ;
			PRINT '=====================================';

	END TRY
		BEGIN CATCH
			PRINT '=====================================';
			PRINT 'ERROR OCCURED';
			PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
			PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER()AS NVARCHAR);
			PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE()AS NVARCHAR);
			PRINT '=====================================';
		END CATCH
END
