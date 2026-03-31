SELECT
    CONCAT(
        '{"balanceSheetLineId":"', db1.BS_Line_Id, '",',
        '"accountCategory":"', db1.Account_Category, '",',
        '"glAccountCode":"', db1.GL_Account_Code, '",',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"debitBalance":', COALESCE(cast(db1.Debit_Balance as varchar(20)), 'null'), ',',
        '"creditBalance":', COALESCE(cast(db1.Credit_Balance as varchar(20)), 'null'), ',',
        '"netBalance":', COALESCE(cast(db1.Net_Balance as varchar(20)), 'null'), ',',
        '"balanceDate":', COALESCE('"' || NULLIF(cast(db1.Balance_Date as varchar(10)),'') || '"', 'null'), ',',
        '"entityCode":"', db1.Entity_Code, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS balance_sheet_data
FROM  {$Db_Schema1}.finance_dm_daily_balance_sheet db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
