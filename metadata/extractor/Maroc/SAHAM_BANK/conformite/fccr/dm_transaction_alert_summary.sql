SELECT
    CONCAT(
        '{"alertIdentifier":"', db1.Alert_Id, '",',
        '"localIdentifierClient":"', db1.Client_Party_Id, '",',
        '"localDatabaseName":"', db1.FCCR_Database_Abbreviation, '",',
        '"alertType":"', db1.Alert_Type, '",',
        '"alertStatus":"', db1.Alert_Status, '",',
        '"alertCreationDate":', COALESCE('"' || NULLIF(cast(db1.Alert_Creation_Date as varchar(10)),'') || '"', 'null'), ',',
        '"alertClosureDate":', COALESCE('"' || NULLIF(cast(db1.Alert_Closure_Date as varchar(10)),'') || '"', 'null'), ',',
        '"transactionAmount":', COALESCE(cast(db1.Transaction_Amount as varchar(20)), 'null'), ',',
        '"transactionCurrency":"', db1.Transaction_Currency, '",',
        '"riskScore":', COALESCE(cast(db1.Risk_Score as varchar(10)), 'null'), ',',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS alert_data
FROM  {$Db_Schema1}.compliance_dm_transaction_alert_summary db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
