=====================================================
------------Silver Data Quality checks ________________________
=====================================================
--- add description  check timestamp ---3:27:29 in bara video
-==============================================  

-- check for Nulls and duplicates in primary key CRM_CUST_INFO
-- expectation: No Result
Select cst_id,count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;


-- Check for unwanted spaces 
-- expectation : No result
Select cst_firstname from silver.crm_cust_info
where cst_firstname!= trim(cst_firstname);


--- Data standardization & consistency 
Select distinct(cst_gndr)
from silver.crm_cust_info;


-------------------------------------------------
-- crm_prd_info
-- Check for duplicates or nulls in Primary key
-- Expectation : No result
Select prd_id from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Check for unwanted spaces
-- Expectation : no result
Select prd_nm from silver.crm_prd_info
where prd_nm != trim(prd_nm);

-- Check for nulls or negative numbers 
-- Expectation: no result 
Select prd_cost from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

-- Data standardization and consistnecy 
Select distinct prd_line from bronze.crm_prd_info;
	
-- CHeck for invalid date orders
Select * from silver.crm_prd_info
where prd_start_dt > prd_end_dt;


-----------------------
--- crm_sales_details

-- Check for duplicates or nulls in Primary key
-- Expectation : No result
Select sls_ord_num from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num);


---- check fr invalid invalid dates
Select nullif(sls_order_dt,0) as sls_order_dt
from bronze.crm_sales_details
where  sls_order_dt <= 0 
or len(sls_order_dt) != 8
or sls_order_dt > 20500101 
or sls_order_dt < 19001010;

------ Check Data consistency : Beween Sales, Quantity, and Price
--->> sales = quantity * price
--->> must not be null,zero or nagative
Select distinct
Case when sls_sales <= 0 or sls_sales is null or sls_sales != sls_price * sls_quantity
         then sls_quantity * ABS(sls_price) 
	 else sls_sales
end as sls_sales,
sls_quantity,
Case when sls_price <= 0 or sls_price is null 
        then sls_sales / Nullif(sls_quantity,0)
     Else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_quantity is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_quantity <= 0;


------------------------------------
------------ erp_CUST_AZ12
------ Identity Out-of-range date
Select bdate from bronze.erp_CUST_AZ12 
where bdate < '1900-01-01' or  bdate > getdate();

------Data Standardiztion & Consistency
Select distinct gen from bronze.erp_CUST_AZ12 

-------------------------------------
-------------erp_LOC_A101
--Data standardization and consistency
Select 
case when trim(cntry) in ('USA','US') then 'United States'
    when trim(cntry) = 'DE' then 'Germany'
    when trim(cntry) = '' or trim(cntry) is null then 'n/a'
    else trim(cntry)
END as cntry
from bronze.erp_LOC_A101
