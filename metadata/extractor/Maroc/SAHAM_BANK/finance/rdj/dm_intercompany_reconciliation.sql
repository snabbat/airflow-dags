SELECT
    CONCAT(
        '{"reconciliationId":"', db1.Reconciliation_Id, '",',
        '"sourceEntityCode":"', db1.Source_Entity_Code, '",',
        '"targetEntityCode":"', db1.Target_Entity_Code, '",',
        '"glAccountCode":"', db1.GL_Account_Code, '",',
        '"sourceAmount":', COALESCE(cast(db1.Source_Amount as varchar(20)), 'null'), ',',
        '"targetAmount":', COALESCE(cast(db1.Target_Amount as varchar(20)), 'null'), ',',
        '"differenceAmount":', COALESCE(cast(db1.Difference_Amount as varchar(20)), 'null'), ',',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"reconciliationStatus":"', db1.Reconciliation_Status, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS intercompany_data
FROM  {$Db_Schema1}.finance_dm_intercompany_reconciliation db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
