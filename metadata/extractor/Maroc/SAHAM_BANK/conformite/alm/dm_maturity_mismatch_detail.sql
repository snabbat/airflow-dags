SELECT
    CONCAT(
        '{"mismatchIdentifier":"', db1.Mismatch_Id, '",',
        '"productCategory":"', db1.Product_Category, '",',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"assetMaturityDate":', COALESCE('"' || NULLIF(cast(db1.Asset_Maturity_Date as varchar(10)),'') || '"', 'null'), ',',
        '"liabilityMaturityDate":', COALESCE('"' || NULLIF(cast(db1.Liability_Maturity_Date as varchar(10)),'') || '"', 'null'), ',',
        '"assetAmount":', COALESCE(cast(db1.Asset_Amount as varchar(20)), 'null'), ',',
        '"liabilityAmount":', COALESCE(cast(db1.Liability_Amount as varchar(20)), 'null'), ',',
        '"mismatchAmount":', COALESCE(cast(db1.Mismatch_Amount as varchar(20)), 'null'), ',',
        '"mismatchDays":', COALESCE(cast(db1.Mismatch_Days as varchar(10)), 'null'), ',',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS maturity_mismatch_data
FROM  {$Db_Schema1}.compliance_dm_maturity_mismatch_detail db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
