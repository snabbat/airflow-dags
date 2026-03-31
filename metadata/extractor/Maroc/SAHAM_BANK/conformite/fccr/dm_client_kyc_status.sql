SELECT
    CONCAT(
        '{"rctIdentifierClient":', COALESCE('"' || NULLIF(db1.RCT_Id,'') || '"', 'null'), ',',
        '"localIdentifierClient":"', db1.Client_Party_Id, '",',
        '"localDatabaseName":"', db1.FCCR_Database_Abbreviation, '",',
        '"kycStatus":"', db1.KYC_Status, '",',
        '"kycLastReviewDate":', COALESCE('"' || NULLIF(cast(db1.KYC_Last_Review_Date as varchar(10)),'') || '"', 'null'), ',',
        '"kycNextReviewDate":', COALESCE('"' || NULLIF(cast(db1.KYC_Next_Review_Date as varchar(10)),'') || '"', 'null'), ',',
        '"riskClassification":"', db1.Risk_Classification, '",',
        '"pepIndicator":"', db1.PEP_Indicator, '",',
        '"sanctionScreeningResult":"', db1.Sanction_Screening_Result, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS kyc_data
FROM  {$Db_Schema1}.compliance_dm_client_kyc_status db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
