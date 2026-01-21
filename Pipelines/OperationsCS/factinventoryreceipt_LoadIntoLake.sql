MERGE INTO `{{lake_project_id}}.OperationsCS_DWH.{{pipeline_file}}`               TG 
USING `{{lake_project_id}}.DataLake_Model.OperationsCS_DWH_{{pipeline_file}}`     SC 

ON  TG.FactPreAdviceLine_ID = SC.FactPreAdviceLine_ID
AND TG.Inventory_Transaction_ID = SC.Inventory_Transaction_ID
AND TG.Product_Key = SC.Product_Key
AND TG.Receipt_DateTime = SC.Receipt_DateTime
AND TG.Receipt_Tag_ID = SC.Receipt_Tag_ID
AND TG.Receipt_Type = SC.Receipt_Type
AND TG.PreAdvice_Line_ID = SC.PreAdvice_Line_ID 

WHEN MATCHED THEN UPDATE 

      SET   TG.Inventory_Transaction_Key    = SC.Inventory_Transaction_Key
      ,     TG.FactPreAdviceLine_ID         = SC.FactPreAdviceLine_ID
      ,     TG.PreAdvice_ID                 = SC.PreAdvice_ID
      ,     TG.PreAdvice_Client_ID          = SC.PreAdvice_Client_ID
      ,     TG.PreAdvice_Key                = SC.PreAdvice_Key
      ,     TG.Client_Key                   = SC.Client_Key
      ,     TG.Shift_Key                    = SC.Shift_Key
      ,     TG.Location_Key                 = SC.Location_Key
      ,     TG.Warehouse_User_Key           = SC.Warehouse_User_Key
      ,     TG.Receipt_Pallet_ID            = SC.Receipt_Pallet_ID
      ,     TG.Receipt_Tag_Box_Type         = SC.Receipt_Tag_Box_Type
      ,     TG.Receipt_Quantity             = SC.Receipt_Quantity
      ,     TG.Product_ID                   = SC.Product_ID

WHEN NOT MATCHED THEN INSERT 
(
            Inventory_Transaction_ID     
      ,     Inventory_Transaction_Key    
      ,     FactPreAdviceLine_ID         
      ,     PreAdvice_ID                 
      ,     PreAdvice_Client_ID          
      ,     PreAdvice_Line_ID                    
      ,     PreAdvice_Key                
      ,     Client_Key                   
      ,     Product_Key                  
      ,     Receipt_Date                 
      ,     Receipt_Time                 
      ,     Receipt_DateTime             
      ,     Receipt_Shift_Date           
      ,     Shift_Key                    
      ,     Location_Key                 
      ,     Warehouse_User_Key           
      ,     Receipt_Type                 
      ,     Receipt_Pallet_ID            
      ,     Receipt_Tag_ID               
      ,     Receipt_Tag_Box_Type         
      ,     Receipt_Quantity             
      ,     Product_ID                   
      ,     etl_loaded_datetime          
) VALUES    (
            SC.Inventory_Transaction_ID     
      ,     SC.Inventory_Transaction_Key    
      ,     SC.FactPreAdviceLine_ID         
      ,     SC.PreAdvice_ID                 
      ,     SC.PreAdvice_Client_ID          
      ,     SC.PreAdvice_Line_ID                   
      ,     SC.PreAdvice_Key                
      ,     SC.Client_Key                   
      ,     SC.Product_Key                  
      ,     SC.Receipt_Date                 
      ,     SC.Receipt_Time                 
      ,     SC.Receipt_DateTime             
      ,     SC.Receipt_Shift_Date           
      ,     SC.Shift_Key                    
      ,     SC.Location_Key                 
      ,     SC.Warehouse_User_Key           
      ,     SC.Receipt_Type                 
      ,     SC.Receipt_Pallet_ID            
      ,     SC.Receipt_Tag_ID               
      ,     SC.Receipt_Tag_Box_Type         
      ,     SC.Receipt_Quantity             
      ,     SC.Product_ID                   
      ,     SC.etl_loaded_datetime          
);