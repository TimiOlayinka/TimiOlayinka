MERGE INTO `{{lake_project_id}}.OperationsCS_DWH.{{pipeline_file}}` TG 
USING `{{lake_project_id}}.DataLake_Model.OperationsCS_DWH_{{pipeline_file}}` STG
      ON  TG.Order_ID  = STG.Order_ID
      AND TG.Shipped_Parcel_ID = STG.Shipped_Parcel_ID
WHEN MATCHED THEN UPDATE 
SET   
      TG.Order_ID                                               = STG.Order_ID                                                ,
      TG.Order_Customer_ID                                      = STG.Order_Customer_ID                                       ,
      TG.Order_Shipped_DateTime                                 = STG.Order_Shipped_DateTime                                  ,
      TG.Shipped_Parcel_ID                                      = STG.Shipped_Parcel_ID                                       ,
      TG.Packing_Warehouse_ID                                   = STG.Packing_Warehouse_ID                                    ,
      TG.Carrier_Service                                        = STG.Carrier_Service                                         , 
      TG.Carrier                                                = STG.Carrier                                                 , 
      TG.Order_Type_Service                                     = STG.Order_Type_Service                                      , 
      TG.Packed_DateTime                                        = STG.Packed_DateTime                                         , 
      TG.etl_updated_datetime                 	                = STG.etl_updated_datetime
WHEN NOT MATCHED THEN INSERT 
(
      Order_Customer_ID,	
      Order_ID,	
      Order_Shipped_DateTime,
      Shipped_Parcel_ID,
      Packing_Warehouse_ID,
      Carrier,	
      Carrier_Service,	
      Order_Type_Service,	
      Packed_DateTime,
      etl_loaded_datetime,
      etl_updated_datetime
) 
VALUES    
(
      STG.Order_Customer_ID,	
      STG.Order_ID,	
      STG.Order_Shipped_DateTime,
      STG.Shipped_Parcel_ID,
      STG.Packing_Warehouse_ID,
      STG.Carrier,	
      STG.Carrier_Service,	
      STG.Order_Type_Service,	
      STG.Packed_DateTime,
      STG.etl_loaded_datetime,
      STG.etl_updated_datetime
);