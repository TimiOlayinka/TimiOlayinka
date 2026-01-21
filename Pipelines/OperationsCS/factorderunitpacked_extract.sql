SELECT      
	f.[Order Customer ID]							AS Order_Customer_ID,
	f.[Order ID]									AS Order_ID, 
	f.[Shipped Parcel ID]							AS Shipped_Parcel_ID,
	MAX(f.[Packing Warehouse ID]) 					AS Packing_Warehouse_ID,
	MAX(f.[Order Shipped DateTime])					AS Order_Shipped_DateTime,
	MAX(replace(c.[Carrier],'|',''))				AS Carrier, 
	MAX(replace(c.[Carrier Service],'|',''))		AS Carrier_Service,
	MAX(replace(ot.[Order Type Service],'|',''))	AS Order_Type_Service,
	MAX(REPLACE(ot.[Order Type ID],'|','')) 		AS Order_Type,
	MAX(f.[Packed DateTime])						AS Packed_DateTime
FROM Report.FactOrderUnitPacked f
INNER JOIN Report.DimCarrierService c 
	ON c.[Carrier Service Key] = f.[Carrier Service Key]
INNER JOIN Report.DimOrderType ot 
	ON ot.[Order Type Key] = f.[Order Type Key]
WHERE 1 = 1 AND
	{% if env == "dev" %}
    [Packed Date] >= DATEADD(DAY,-1,GETDATE()) AND
    {% endif %}
	[Packed Date] > DATEADD(DAY,-1,CONVERT(DATETIME,SUBSTRING('{{incr_column}}',1,23),121))
-- and f.[Order ID] = 'UK165919471'
GROUP BY f.[Order Customer ID],f.[Order ID],f.[Shipped Parcel ID]