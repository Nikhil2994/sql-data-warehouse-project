=======================================================
  Stored procedure: Load silver layer ( bronze -- > silver)
=======================================================
  
  
=======================================================
Create or ALter Procedure silver.load_silver AS
Begin
	-----crm_cust_info
	Print '>>Truncate Table: silver.crm_cust_info';
	Truncate table silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	Select 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	case when upper(TRIM(cst_marital_status)) = 'M' then 'Married'
		 when upper(TRIM(cst_marital_status)) = 'S' then 'Single'
		 else 'n/a'
	end as cst_marital_status,
	case when upper(TRIM(cst_gndr)) = 'M' then 'Male'
		 when upper(TRIM(cst_gndr)) = 'F' then 'Female'
		 else 'n/a'
	end as cst_gndr,
	cst_create_date
	from
		(Select * ,
		row_number() over(partition by cst_id order by cst_create_date desc) as flag
		from bronze.crm_cust_info
		where cst_id is not null
		)x
	where flag = 1;

	------------------------------
	-- crm_prd_info
	Print '>>Truncate Table: silver.crm_prd_info';
	Truncate table silver.crm_prd_info;
	insert into silver.crm_prd_info(
		prd_id ,
		cat_id ,
		prd_key ,
		prd_nm ,
		prd_cost ,
		prd_line ,
		prd_start_dt ,
		prd_end_dt 
		)
	Select 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	Case
		when upper(trim(prd_line)) = 'R' then 'Road'
		when upper(trim(prd_line)) = 'M' then 'Mountain'
		when upper(trim(prd_line)) = 'S' then 'Other Sales'
		when upper(trim(prd_line)) = 'T' then 'Touring'
		Else 'n/a'
	End as prd_line,
	cast(prd_start_dt as DATE) as prd_start_dt,
	cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt
	from bronze.crm_prd_info;

	---------------------------------------
	--crm_sales_details
	Print '>>Truncate Table: silver.crm_sales_details';
	Truncate table silver.crm_sales_details;
	insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	Select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	Case 
		 when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
		 else cast(cast(sls_order_dt as varchar) as date) 
	end as sls_order_dt,
	Case 
		 when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
		 else cast(cast(sls_ship_dt as varchar) as date) 
	end as sls_ship_dt,
	Case 
		 when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
		 else cast(cast(sls_due_dt as varchar) as date) 
	end as sls_due_dt,
	Case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_price * sls_quantity
			 then sls_quantity * ABS(sls_price) 
		 else sls_sales
	end as sls_sales,
	sls_quantity,
	Case when sls_price <= 0 or sls_price is null 
			then sls_sales / Nullif(sls_quantity,0)
		 Else sls_price
	end as sls_price
	from bronze.crm_sales_details;

	-------------------------------------------------
	----------- erp_CUST_AZ12
	Print '>>Truncate Table: silver.erp_CUST_AZ12';
	Truncate table silver.erp_CUST_AZ12;
	insert into silver.erp_CUST_AZ12(cid,bdate,gen)
	Select 
	Case when cid like 'NAS%' then substring(cid,4,len(cid))
	   else cid 
	end as cid,
	Case when bdate > getdate() then null
	   else bdate 
	end as bdate,
	Case when upper(trim(gen)) in ('f','FEMALE') then 'Female'
		 when upper(trim(gen)) in ('m','MALE') then 'Male'
		 Else 'n/a'
	end as gen
	from [bronze].[erp_CUST_AZ12]

	--------------------------------------------------
	--------------- erp_LOC_A101
	Print '>>Truncate Table: silver.erp_LOC_A101';
	Truncate table silver.erp_LOC_A101;
	insert into silver.erp_LOC_A101(cid,cntry)	
	select 
	replace(cid,'-','') as cid,
	case when trim(cntry) in ('USA','US') then 'United States'
		when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) = '' or trim(cntry) is null then 'n/a'
		else trim(cntry)
	END as cntry
	from bronze.erp_LOC_A101;

	------------------------------------------------------
	------------------[Silver].[erp_PX_CAT_G1V2]
	Print '>>Truncate Table: silver.erp_PX_CAT_G1V2';
	Truncate table silver.erp_PX_CAT_G1V2;
	Insert into silver.erp_PX_CAT_G1V2(id,cat,subcat,maintenance)
	Select 
	id,
	cat,
	subcat,
	maintenance
	from 
	[bronze].[erp_PX_CAT_G1V2];
End
