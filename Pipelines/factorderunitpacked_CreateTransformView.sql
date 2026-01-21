CREATE OR REPLACE VIEW `{{lake_project_id}}.DataLake_Model.OperationsCS_DWH_{{pipeline_file}}` 
AS 
(
    SELECT    
        Order_Customer_ID,	
        Order_ID,	
        CAST(Order_Shipped_DateTime AS DATETIME) AS Order_Shipped_DateTime,
        Shipped_Parcel_ID,
        Packing_Warehouse_ID,
        Carrier,	
        Carrier_Service,	
        Order_Type_Service,	
        CAST(Packed_DateTime AS DATETIME) AS Packed_DateTime,
        CURRENT_TIMESTAMP() AS etl_loaded_datetime,
        CURRENT_TIMESTAMP() AS etl_updated_datetime
    FROM `{{lake_project_id}}.DataLake_Stg.OperationsCS_DWH_{{pipeline_file}}`
)