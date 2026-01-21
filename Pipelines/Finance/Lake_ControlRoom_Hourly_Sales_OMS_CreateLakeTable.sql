CREATE TABLE IF NOT EXISTS `{{lake_project_id}}.Finance.Lake_ControlRoom_Hourly_Sales_OMS` 
(
    datetimecreated            DATETIME    ,
    datetimeupdated            DATETIME    ,
    Orderdate                  DATETIME    ,
    hour                       INT64       ,
    country                    STRING      ,
    currency                   STRING      ,
    freighttype                STRING      ,
    Gross_Product_Sales_LC     NUMERIC     ,
    Gross_Carriage_Sales_LC    BIGNUMERIC  ,
    Gross_Product_Sales        BIGNUMERIC  ,
    Gross_Carriage_Sales       NUMERIC     ,
    Net_Product_Sales          BIGNUMERIC  ,
    Net_Carriage_Sales         NUMERIC     ,
    Product_Cost               NUMERIC     ,
    Units                      NUMERIC     ,
    Orders                     INT64       ,
    etl_loaded_datetime    	   TIMESTAMP   
) 
PARTITION BY DATE(Orderdate)