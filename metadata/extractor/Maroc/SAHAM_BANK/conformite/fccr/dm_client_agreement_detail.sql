SELECT
    CONCAT(
        '{"rctIdentifierClient":', COALESCE('"' || NULLIF(db1.RCT_Id,'') || '"', 'null'), ',',
        '"customerLocalization":', COALESCE('"' || NULLIF(db1.Nationality_Code_RCT,'') || '"', 'null'), ',',
        '"localIdentifierClient":"', db1.Client_Party_Id, '",',
        '"localDatabaseName":"', db1.FCCR_Database_Abbreviation, '",',
        '"productCode":"', db1.DEVL_Code, '",',
        '"issuingContractDate":', COALESCE('"' || NULLIF(cast(db1.Contract_Date as varchar(10)),'') || '"', 'null'), ',',
        '"issuingApplicationCode":"', db1.Application_Code, '",',
        '"contractIdentifier":"', db1.Agreement_Id, '",',
        '"maturityContractDate":', COALESCE('"' || NULLIF(cast(db1.Maturity_Date as varchar(10)),'') || '"', 'null'), ',',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS contract_data
FROM  {$Db_Schema1}.compliance_dm_client_agreement_detail db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.DEVL_Family_Code <> '00181'
AND db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
