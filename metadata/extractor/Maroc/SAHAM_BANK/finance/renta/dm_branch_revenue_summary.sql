SELECT
    CONCAT(
        '{"branchCode":"', db1.Branch_Code, '",',
        '"branchName":"', db1.Branch_Name, '",',
        '"regionCode":"', db1.Region_Code, '",',
        '"interestIncome":', COALESCE(cast(db1.Interest_Income as varchar(20)), 'null'), ',',
        '"feeIncome":', COALESCE(cast(db1.Fee_Income as varchar(20)), 'null'), ',',
        '"totalRevenue":', COALESCE(cast(db1.Total_Revenue as varchar(20)), 'null'), ',',
        '"operatingExpenses":', COALESCE(cast(db1.Operating_Expenses as varchar(20)), 'null'), ',',
        '"netIncome":', COALESCE(cast(db1.Net_Income as varchar(20)), 'null'), ',',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS branch_revenue_data
FROM  {$Db_Schema1}.finance_dm_branch_revenue_summary db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
