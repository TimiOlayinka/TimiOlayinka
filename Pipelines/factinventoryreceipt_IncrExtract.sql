BEGIN 
	SELECT COALESCE(FORMAT_DATETIME('%F %H:%M:%E3S', MAX(Receipt_DateTime)),'1900-01-01 00:00:00.000') AS Maximum 
	FROM `{{lake_project_id}}.OperationsCS_DWH.factinventoryreceipt`;
EXCEPTION 
	WHEN ERROR THEN SELECT '1900-01-08 00:00:00.000';
END;