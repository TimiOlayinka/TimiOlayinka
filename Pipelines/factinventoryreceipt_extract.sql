SELECT      [Inventory Transaction ID]                AS  Inventory_Transaction_ID
      ,     [Inventory Transaction Key]               AS  Inventory_Transaction_Key
      ,     REPLACE([FactPreAdviceLine ID], '|','')   AS  FactPreAdviceLine_ID
      ,     REPLACE([PreAdvice ID], '|','')           AS  PreAdvice_ID
      ,     REPLACE([PreAdvice Client ID], '|','')    AS  PreAdvice_Client_ID
      ,     REPLACE([PreAdvice Line ID], '|','')      AS  PreAdvice_Line_ID
      ,     [PreAdvice Key]                           AS  PreAdvice_Key
      ,     [Client Key]                              AS  Client_Key
      ,     FI.[Product Key]                          AS  Product_Key
      ,     [Receipt Date]                            AS  Receipt_Date
      ,     [Receipt Time]                            AS  Receipt_Time
      ,     CAST([Receipt Date] AS DATETIME) 
            + CAST([Receipt Time] AS DATETIME)        AS Receipt_DateTime
      ,     [Receipt Shift Date]                      AS  Receipt_Shift_Date
      ,     [Shift Key]                               AS  Shift_Key
      ,     [Location Key]                            AS  Location_Key
      ,     [Warehouse User Key]                      AS  Warehouse_User_Key
      ,     REPLACE([Receipt Type], '|','')           AS  Receipt_Type
      ,     REPLACE([Receipt Pallet ID], '|','')      AS  Receipt_Pallet_ID
      ,     REPLACE([Receipt Tag ID], '|','')         AS  Receipt_Tag_ID
      ,     REPLACE([Receipt Tag Box Type], '|','')   AS  Receipt_Tag_Box_Type
      ,     [# Receipt Quantity]                      AS  Receipt_Quantity
	,     [Product ID]                              AS  Product_ID
  FROM      [XXXX].[DW].[FactInventoryReceipt]		      FI WITH (NOLOCK)
  LEFT JOIN [XXXX].[DW].DimProduct					DP WITH (NOLOCK) ON DP.[Product Key] = FI.[Product Key]
  WHERE     1 = 1 AND 
      {% if env == "dev" %}
        [Receipt Date] >= DATEADD(DAY,-10,GETDATE()) AND
      {% endif %}
  CAST([Receipt Date] AS DATETIME) + CAST([Receipt Time] AS DATETIME) > DATEADD(HOUR,-6,CONVERT(DATETIME,SUBSTRING('{{incr_column}}',1,23),121))
