CREATE TABLE IF NOT EXISTS `{{lake_project_id}}.OperationsCS_DWH.{{pipeline_file}}` 
(
      Order_Customer_ID       STRING,	
      Order_ID                STRING,	
      Order_Shipped_DateTime  DATETIME,
      Shipped_Parcel_ID       STRING,
      Packing_Warehouse_ID    STRING,	
      Carrier                 STRING,	
      Carrier_Service         STRING,	
      Order_Type_Service      STRING,	
      Packed_DateTime         DATETIME,
      etl_loaded_datetime     TIMESTAMP,
      etl_updated_datetime    TIMESTAMP
)
PARTITION BY DATE(Packed_DateTime)
CLUSTER BY Order_ID