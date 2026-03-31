SELECT
    CONCAT(
        '{"sensitivityIdentifier":"', db1.Sensitivity_Id, '",',
        '"instrumentType":"', db1.Instrument_Type, '",',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"notionalAmount":', COALESCE(cast(db1.Notional_Amount as varchar(20)), 'null'), ',',
        '"fixedRate":', COALESCE(cast(db1.Fixed_Rate as varchar(10)), 'null'), ',',
        '"floatingRateIndex":"', db1.Floating_Rate_Index, '",',
        '"repricingFrequency":"', db1.Repricing_Frequency, '",',
        '"durationYears":', COALESCE(cast(db1.Duration_Years as varchar(10)), 'null'), ',',
        '"bpvImpact":', COALESCE(cast(db1.BPV_Impact as varchar(20)), 'null'), ',',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS rate_sensitivity_data
FROM  {$Db_Schema1}.compliance_dm_interest_rate_sensitivity db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
