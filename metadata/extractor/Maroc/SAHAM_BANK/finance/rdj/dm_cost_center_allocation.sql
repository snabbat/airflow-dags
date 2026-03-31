SELECT
    CONCAT(
        '{"allocationId":"', db1.Allocation_Id, '",',
        '"costCenterCode":"', db1.Cost_Center_Code, '",',
        '"costCenterName":"', db1.Cost_Center_Name, '",',
        '"expenseCategory":"', db1.Expense_Category, '",',
        '"allocatedAmount":', COALESCE(cast(db1.Allocated_Amount as varchar(20)), 'null'), ',',
        '"allocationBasis":"', db1.Allocation_Basis, '",',
        '"allocationPercentage":', COALESCE(cast(db1.Allocation_Percentage as varchar(10)), 'null'), ',',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"periodDate":', COALESCE('"' || NULLIF(cast(db1.Period_Date as varchar(10)),'') || '"', 'null'), ',',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS cost_allocation_data
FROM  {$Db_Schema1}.finance_dm_cost_center_allocation db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
