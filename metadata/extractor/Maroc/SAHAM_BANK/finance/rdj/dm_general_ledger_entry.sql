SELECT
    CONCAT(
        '{"journalEntryId":"', db1.Journal_Entry_Id, '",',
        '"glAccountCode":"', db1.GL_Account_Code, '",',
        '"postingDate":', COALESCE('"' || NULLIF(cast(db1.Posting_Date as varchar(10)),'') || '"', 'null'), ',',
        '"valueDate":', COALESCE('"' || NULLIF(cast(db1.Value_Date as varchar(10)),'') || '"', 'null'), ',',
        '"debitAmount":', COALESCE(cast(db1.Debit_Amount as varchar(20)), 'null'), ',',
        '"creditAmount":', COALESCE(cast(db1.Credit_Amount as varchar(20)), 'null'), ',',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"transactionReference":"', db1.Transaction_Reference, '",',
        '"entityCode":"', db1.Entity_Code, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS ledger_entry_data
FROM  {$Db_Schema1}.finance_dm_general_ledger_entry db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
