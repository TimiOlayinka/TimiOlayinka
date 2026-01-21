CREATE TABLE IF NOT EXISTS `{{lake_project_id}}.OperationsCS_DWH.{{pipeline_file}}` 
(
       Inventory_Transaction_ID     INT64
      ,Inventory_Transaction_Key    STRING
      ,FactPreAdviceLine_ID         INT64
      ,PreAdvice_ID                 STRING
      ,PreAdvice_Client_ID          STRING
      ,PreAdvice_Line_ID            INT64
      ,PreAdvice_Key                INT64
      ,Client_Key                   INT64
      ,Product_Key                  INT64
      ,Receipt_Date                 DATE
      ,Receipt_Time                 TIME
      ,Receipt_DateTime             DATETIME
      ,Receipt_Shift_Date           DATE
      ,Shift_Key                    INT64
      ,Location_Key                 INT64
      ,Warehouse_User_Key           INT64
      ,Receipt_Type                 STRING
      ,Receipt_Pallet_ID            STRING
      ,Receipt_Tag_ID               STRING
      ,Receipt_Tag_Box_Type         STRING
      ,Receipt_Quantity             INT64
      ,Product_ID                   STRING
      ,etl_loaded_datetime          TIMESTAMP
) PARTITION BY Receipt_Date
CLUSTER BY PreAdvice_ID, Product_ID