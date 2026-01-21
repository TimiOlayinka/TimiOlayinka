CREATE OR REPLACE VIEW `{{lake_project_id}}.DataLake_Model.OperationsCS_DWH_{{pipeline_file}}` AS (

    SELECT    CAST(Inventory_Transaction_ID AS INT64)           AS Inventory_Transaction_ID
        ,     Inventory_Transaction_Key
        ,     CAST(FactPreAdviceLine_ID AS INT64)               AS FactPreAdviceLine_ID
        ,     PreAdvice_ID
        ,     PreAdvice_Client_ID
        ,     CAST(PreAdvice_Line_ID AS INT64)                  AS PreAdvice_Line_ID
        ,     CAST(PreAdvice_Key AS INT64)                      AS PreAdvice_Key
        ,     CAST(Client_Key AS INT64)                         AS Client_Key
        ,     CAST(Product_Key AS INT64)                        AS Product_Key
        ,     CAST(Receipt_Date AS DATE)                        AS Receipt_Date
        ,     CAST(Receipt_Time AS TIME)                        AS Receipt_Time
        ,     CAST(Receipt_DateTime AS DATETIME)                AS Receipt_DateTime
        ,     CAST(Receipt_Shift_Date AS DATE)                  AS Receipt_Shift_Date
        ,     CAST(Shift_Key AS INT64)                          AS Shift_Key
        ,     CAST(Location_Key AS INT64)                       AS Location_Key
        ,     CAST(Warehouse_User_Key AS INT64)                 AS Warehouse_User_Key
        ,     Receipt_Type
        ,     Receipt_Pallet_ID
        ,     Receipt_Tag_ID
        ,     Receipt_Tag_Box_Type
        ,     CAST(Receipt_Quantity AS INT64)                   AS Receipt_Quantity
        ,     Product_ID
        ,     CAST(CURRENT_DATETIME('UTC') AS TIMESTAMP)        AS etl_loaded_datetime    
    FROM      `{{lake_project_id}}.DataLake_Stg.OperationsCS_DWH_{{pipeline_file}}`
    WHERE     1=1
    QUALIFY   ROW_NUMBER() OVER (PARTITION BY FactPreAdviceLine_ID, Inventory_Transaction_ID,
              Product_Key, Receipt_DateTime, Receipt_Tag_ID, Receipt_Type, PreAdvice_Line_ID) = 1
)