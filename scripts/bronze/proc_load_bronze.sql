CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
declare @start_time Datetime ,@end_time Datetime,@batch_start_time datetime,@batch_end_time datetime;
begin try

set @batch_start_time=getdate();
print'=======================================';
print'Loading Bronze Layer';
print'=======================================';

set @start_time=GETDATE();
truncate table bronze.crm_cust_info;

bulk insert bronze.crm_cust_info
from 'D:\SQL_project\SQL_wareHouse_project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
FIRSTROW=2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time=GETDATE();
print'>>load duration: '+cast(DateDiff(second,@start_time,@end_time) AS nvarchar)+'second';


set @start_time=GETDATE();
truncate table bronze.crm_prd_info;
print'>>inserting data into:bronze.crm_prd_info'
bulk insert bronze.crm_prd_info
from 'D:\SQL_project\SQL_wareHouse_project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
FIRSTROW=2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time=GETDATE();
print'>>load duration: '+cast(DateDiff(second,@start_time,@end_time) AS nvarchar)+'second';

set @start_time=getdate();
truncate table bronze.crm_sales_details;
print'>>inserting data into:bronze.crm_sales_details'
bulk insert bronze.crm_sales_details
from 'D:\SQL_project\SQL_wareHouse_project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
FIRSTROW=2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time=GETDATE();
print'>>load duration: '+cast(DateDiff(second,@start_time,@end_time) AS nvarchar)+'second';


set @start_time=getdate();
truncate table bronze.erp_loc_a101;
print'>>inserting data into:bronze.erp_loc_a101'
bulk insert bronze.erp_loc_a101
from 'D:\SQL_project\SQL_wareHouse_project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with (
FIRSTROW=2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time=getdate();
print'>>load duration'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'second';

set @start_time=getDate();
truncate table bronze.erp_cust_az12;
print'>>inserting data into:bronze.erp_cust_az12'
bulk insert bronze.erp_cust_az12
from 'D:\SQL_project\SQL_wareHouse_project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
with (
FIRSTROW=2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time=getdate();
print'>>load duration'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'second';

set @start_time=getDate();
truncate table bronze.erp_px_cat_g1v2;
print'>>inserting data into:bronze.erp_px_cat_g1v2'
bulk insert bronze.erp_px_cat_g1v2
from 'D:\SQL_project\SQL_wareHouse_project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with (
FIRSTROW=2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time=getdate();
print'>>load duration'+cast(datediff(second,@start_time,@end_time)as nvarchar)+'second';
print'----------------------'

set @batch_end_time=getdate();
print'Total load duration:'+cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+'second';

end try
begin catch 
  print'=========================================='
  Print'Error occured during loading bronze layer'
  print'Error message'+error_message();
  print'Error message' +cast(Error_number() as nvarchar);
  print'Error message'+cast(error_state() as nvarchar); 
  print'=========================================='

end catch

END
