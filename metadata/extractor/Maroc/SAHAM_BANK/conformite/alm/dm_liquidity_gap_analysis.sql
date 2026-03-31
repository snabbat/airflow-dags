SELECT
    CONCAT(
        '{"gapBandIdentifier":"', db1.Gap_Band_Id, '",',
        '"maturityBucket":"', db1.Maturity_Bucket, '",',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"totalAssets":', COALESCE(cast(db1.Total_Assets as varchar(20)), 'null'), ',',
        '"totalLiabilities":', COALESCE(cast(db1.Total_Liabilities as varchar(20)), 'null'), ',',
        '"netGapAmount":', COALESCE(cast(db1.Net_Gap_Amount as varchar(20)), 'null'), ',',
        '"cumulativeGap":', COALESCE(cast(db1.Cumulative_Gap as varchar(20)), 'null'), ',',
        '"gapRatio":', COALESCE(cast(db1.Gap_Ratio as varchar(10)), 'null'), ',',
        '"regulatoryLimit":', COALESCE(cast(db1.Regulatory_Limit as varchar(10)), 'null'), ',',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS liquidity_gap_data
FROM  {$Db_Schema1}.compliance_dm_liquidity_gap_analysis db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
