SELECT
    CONCAT(
        '{"fundingSourceIdentifier":"', db1.Funding_Source_Id, '",',
        '"counterpartyType":"', db1.Counterparty_Type, '",',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"fundingAmount":', COALESCE(cast(db1.Funding_Amount as varchar(20)), 'null'), ',',
        '"totalFundingBase":', COALESCE(cast(db1.Total_Funding_Base as varchar(20)), 'null'), ',',
        '"concentrationRatio":', COALESCE(cast(db1.Concentration_Ratio as varchar(10)), 'null'), ',',
        '"regulatoryThreshold":', COALESCE(cast(db1.Regulatory_Threshold as varchar(10)), 'null'), ',',
        '"breachIndicator":"', db1.Breach_Indicator, '",',
        '"fundingMaturityBucket":"', db1.Funding_Maturity_Bucket, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS funding_concentration_data
FROM  {$Db_Schema1}.compliance_dm_funding_concentration_ratio db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
