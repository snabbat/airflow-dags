SELECT
    CONCAT(
        '{"feeCommissionId":"', db1.Fee_Commission_Id, '",',
        '"productCode":"', db1.Product_Code, '",',
        '"feeType":"', db1.Fee_Type, '",',
        '"commissionCategory":"', db1.Commission_Category, '",',
        '"grossFeeAmount":', COALESCE(cast(db1.Gross_Fee_Amount as varchar(20)), 'null'), ',',
        '"discountAmount":', COALESCE(cast(db1.Discount_Amount as varchar(20)), 'null'), ',',
        '"netFeeAmount":', COALESCE(cast(db1.Net_Fee_Amount as varchar(20)), 'null'), ',',
        '"transactionCount":', COALESCE(cast(db1.Transaction_Count as varchar(10)), 'null'), ',',
        '"currencyCode":"', db1.Currency_Code, '",',
        '"collectMonth":"', DATE_FORMAT(vicm1.Month_End_Date, 'YYYY-MM'), '"}'
    ) AS fee_commission_data
FROM  {$Db_Schema1}.finance_dm_fee_commission_breakdown db1
CROSS JOIN  {$Db_Schema2}.v_ibfs_mar_bnk_month_date vicm1
WHERE db1.Date_Valid = vicm1.Month_End_Date
AND date_valid = DATE_FORMAT({$ID_DATE},'yyyy-MM-dd')
