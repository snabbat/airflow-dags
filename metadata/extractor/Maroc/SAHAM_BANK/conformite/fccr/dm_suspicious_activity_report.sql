SELECT
    CONCAT(
        '{"sarIdentifier":"', db1.SAR_Id, '",',
        '"localIdentifierClient":"', db1.Client_Party_Id, '",',
        '"localDatabaseName":"', db1.FCCR_Database_Abbreviation, '",',
        '"reportType":"', db1.Report_Type, '",',
        '"reportStatus":"', db1.Report_Status, '",',
        '"filingDate":', COALESCE('"' || NULLIF(cast(db1.Filing_Date as varchar(10)),'') || '"', 'null'), ',',
        '"suspiciousActivityType":"', db1.Suspicious_Activity_Type, '",',
        '"involvedAmount":', COALESCE(cast(db1.Involved_Amount as varchar(20)), 'null'), ',',
        '"involvedCurrency":"', db1.Involved_Currency, '",',
        '"regulatoryAuthority":"', db1.Regulatory_Authority, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS sar_data
FROM  {$Db_Schema1}.compliance_dm_suspicious_activity_report db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
