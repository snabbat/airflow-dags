SELECT
    CONCAT(
        '{"productCode":"', db1.Product_Code, '",',
        '"productName":"', db1.Product_Name, '",',
        '"productCategory":"', db1.Product_Category, '",',
        '"grossRevenue":', COALESCE(cast(db1.Gross_Revenue as varchar(20)), 'null'), ',',
        '"directCosts":', COALESCE(cast(db1.Direct_Costs as varchar(20)), 'null'), ',',
        '"netMargin":', COALESCE(cast(db1.Net_Margin as varchar(20)), 'null'), ',',
        '"marginPercentage":', COALESCE(cast(db1.Margin_Percentage as varchar(10)), 'null'), ',',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"entityCode":"', db1.Entity_Code, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS product_profitability_data
FROM  {$Db_Schema1}.finance_dm_product_profitability_detail db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
